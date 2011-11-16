(ns elephantdb.testing
  (:use clojure.test)
  (:require [hadoop-util.core :as h]
            [elephantdb.common.shard :as shard]
            [elephantdb.common.config :as conf]
            [elephantdb.client :as client]
            [elephantdb.common.log :as log]
            [elephantdb.common.util :as u]
            [elephantdb.service :as service]
            [elephantdb.thrift :as thrift])
  (:import [java.util UUID ArrayList]
           [elephantdb Utils ByteArray]
           [elephantdb.hadoop ElephantRecordWritable ElephantOutputFormat
            ElephantOutputFormat$Args LocalElephantManager]
           [elephantdb.store DomainStore]
           [org.apache.hadoop.io IntWritable]
           [org.apache.hadoop.mapred JobConf]
           [org.apache.thrift TException]))

(defn uuid []
  (str (UUID/randomUUID)))

;; TODO: Add (when vals ,,,)
(defn barr [& vals]
  (byte-array (map byte vals)))

(defn barr= [& vals]
  (apply = (map #(ByteArray. %) vals)))

(defn barrs= [& arrs]
  (and (apply = (map count arrs))
       (every? identity
               (apply map (fn [& vals]
                            (or (every? nil? vals)
                                (apply barr= vals)))
                      arrs))))

(defn domain-data [& key-val-pairs]
  (map (fn [x]
         [(barr (first x))
          (if-not (nil? (second x))
            (apply barr (second x)))])
       (partition 2 key-val-pairs)))

(defn delete-all [fs paths]
  (dorun
   (for [t paths]
     (.delete fs (h/path t) true))))

(defmacro with-fs-tmp
  [[fs-sym & tmp-syms] & body]
  (let [tmp-paths (mapcat (fn [t]
                            [t `(str "/tmp/unittests/" (uuid))])
                          tmp-syms)]
    `(let [~fs-sym (h/filesystem)
           ~@tmp-paths]
       (.mkdirs ~fs-sym (h/path "/tmp/unittests"))
       (try ~@body
            (finally
             (delete-all ~fs-sym [~@tmp-syms]))))))

(defmacro def-fs-test
  [name fs-args & body]
  `(deftest ~name
     (with-fs-tmp ~fs-args
       ~@body)))

(defn local-temp-path []
  (str (System/getProperty "java.io.tmpdir") "/" (uuid)))

(defmacro with-local-tmp [[fs-sym & tmp-syms] & [kw & more :as body]]
  (let [[log-lev body] (if (keyword? kw)
                         [kw more]
                         [:warn body])
        tmp-paths (mapcat (fn [t] [t `(local-temp-path)]) tmp-syms)]
    `(log/with-log-level ~log-lev
       (let [~fs-sym (h/local-filesystem)
             ~@tmp-paths]
         (try
           ~@body
           (finally
            (delete-all ~fs-sym ~(vec tmp-syms))))))))

(defmacro deflocalfstest [name local-args & body]
  `(deftest ~name
     (with-local-tmp ~local-args
       ~@body)))

(defn add-string [db key value]
  (.add db
        (.getBytes key)
        (.getBytes value)))

(defn get-string [db key]
  (when-let [r (.get db (.getBytes key))]
    (String. r)))

(defn get-kvpairs [db]
  (doall
   (for [kvp (seq db)]
     [(. kvp key) (. kvp value)])))

(defn get-string-kvpairs [db]
  (for [[k v] (get-kvpairs db)]
    [(String. k) (String. v)]))

(defn append-string-pairs [factory t pairs]
  (let [db (.openPersistenceForAppend factory t {})]
    (doseq [[k v] pairs]
      (add-string db k v))
    (.close db)))

(defn create-string-pairs [factory t pairs]
  (let [db (.createPersistence factory t {})]
    (doseq [[k v] pairs]
      (add-string db k v))
    (.close db)))

(defn test-key-to-shard
  "bind this to get different behavior when making sharded domains."
  {:dynamic true}
  [key numshards]
  (Utils/keyShard key numshards))

(defn mk-elephant-writer
  [shards factory output-dir tmpdir & kargs]
  (let [kargs (apply hash-map kargs)
        args (ElephantOutputFormat$Args.
              (conf/convert-clj-domain-spec
               {:num-shards shards
                :persistence-factory factory})
              output-dir)]
    (when-let [upd (:updater kargs)]
      (set! (. args updater) upd))
    (when-let [update-dir (:update-dir kargs)]
      (set! (. args updateDirHdfs) update-dir))
    (.getRecordWriter (ElephantOutputFormat.)
                      nil
                      (doto
                          (JobConf.)
                        (Utils/setObject
                         ElephantOutputFormat/ARGS_CONF
                         args )
                        (LocalElephantManager/setTmpDirs [tmpdir]))
                      nil
                      nil)))

(defn mk-sharded-domain
  [fs path domain-spec keyvals & {:keys [version]}]
  (with-local-tmp [lfs localtmp]
    (let [vs (DomainStore. fs path (conf/convert-clj-domain-spec domain-spec))
          dpath (if version
                  (let [v (long version)]
                    (do (if (.hasVersion vs v)
                          (.deleteVersion vs v))
                        (.createVersion vs v)))
                  (.createVersion vs))
          shardedkeyvals (map
                          (fn [[k v]]
                            [(test-key-to-shard k (:num-shards domain-spec))
                             k v])
                          keyvals)
          writer (mk-elephant-writer
                  (:num-shards domain-spec)
                  (:persistence-factory domain-spec)
                  dpath
                  localtmp)]
      (doseq [[s k v] shardedkeyvals]
        (when v
          (.write writer
                  (IntWritable. s)
                  (ElephantRecordWritable. k v))))
      (.close writer nil)
      (.succeedVersion vs dpath))))

(defn reverse-pre-sharded [shardmap]
  (->> shardmap
       (u/val-map #(map (fn [x] (ByteArray. (first x))) %))
       (u/reverse-multimap)
       (u/val-map first)))

(defn mk-presharded-domain [fs path factory shardmap]
  (let [keyvals (apply concat (vals shardmap))
        shards (reverse-pre-sharded shardmap)
        domain-spec {:num-shards (count shardmap) :persistence-factory factory}]
    (binding [test-key-to-shard (fn [k _] (shards (ByteArray. k)))]
      (mk-sharded-domain fs path domain-spec keyvals))))

(defn mk-local-config [local-dir]
  {:local-dir local-dir
   :max-online-download-rate-kb-s 1024
   :update-interval-s 60})

(defn mk-service-handler
  [global-config localdir domain-to-host-to-shards]
  (binding [shard/compute-host-to-shards
            (if domain-to-host-to-shards
              (fn [d _ _ _] (domain-to-host-to-shards d))
              shard/compute-host-to-shards)]
    (let [handler (service/service-handler global-config
                                           (mk-local-config localdir))]
      (while (not (.isFullyLoaded handler))
        (log/info "waiting...")
        (Thread/sleep 500))
      handler)))

(defmacro with-sharded-domain
  [[pathsym domain-spec keyvals] & body]
  `(with-fs-tmp [fs# ~pathsym]
     (mk-sharded-domain fs# ~pathsym ~domain-spec ~keyvals)
     ~@body))

(defmacro with-presharded-domain
  [[dname pathsym factory shardmap] & body]
  `(with-fs-tmp [fs# ~pathsym]
     (mk-presharded-domain
      fs#
      ~pathsym
      ~factory
      ~shardmap)
     (binding [shard/key-shard (let [prev# shard/key-shard
                                     rev# (reverse-pre-sharded ~shardmap)]
                                 (fn [d# k# a#]
                                   (if (= d# ~dname)
                                     (rev# (ByteArray. k#))
                                     (prev# d# k# a#))))]
       ~@body)))

(defmacro with-service-handler
  [[handler-sym hosts domains-conf domain-to-host-to-shards] & body]
  (let [global-conf {:replication 1 :hosts hosts :domains domains-conf}]
    `(with-local-tmp [lfs# localtmp#]
       (let [~handler-sym (mk-service-handler ~global-conf
                                              localtmp#
                                              ~domain-to-host-to-shards)
             updater# (service/launch-updater! 100 ~handler-sym)]
         (try ~@body
              (finally (.shutdown ~handler-sym)
                       (future-cancel updater#)))))))

(defn mk-mocked-remote-multiget-fn
  [domain-to-host-to-shards shards-to-pairs down-hosts]
  (fn [host port domain keys]    
    (when (= host (u/local-hostname))
      (throw (RuntimeException. "Tried to make remote call to local server")))
    (when (get (set down-hosts) host)
      (throw (TException. (str host " is down"))))
    (let [shards (get (domain-to-host-to-shards domain) host)
          pairs (apply concat (vals (select-keys shards-to-pairs shards)))]
      (for [key keys]
        (if-let [myval (first (filter #(barr= key (first %)) pairs))]
          (thrift/mk-value (second myval))
          (throw (thrift/wrong-host-ex)))))))

(defmacro with-mocked-remote
  [[domain-to-host-to-shards shards-to-pairs down-hosts] & body]
  ;; mock client/try-multi-get only for non local-hostname hosts
  `(binding [client/multi-get-remote
             (mk-mocked-remote-multiget-fn ~domain-to-host-to-shards
                                           ~shards-to-pairs
                                           ~down-hosts)]
     ~@body))

(defmacro with-single-service-handler
  [[handler-sym domains-conf] & body]
  `(with-service-handler [~handler-sym [(u/local-hostname)] ~domains-conf nil]
     ~@body))

(defn check-domain-pred
  [domain-name handler pairs pred]
  (doseq [[k v] pairs]
    (let [newv (-> handler (.get domain-name k) (.get_data))]
      (if-not v
        (is (nil? newv))
        (is (pred v newv) (apply str (map seq [k v newv])))))))

(defn- objify-kvpairs [pairs]
  (for [[k v] pairs]
    [(ByteArray. k)
     (ByteArray. v)]))

(defn kv-pairs= [& pairs-seq]
  (let [pairs-seq (map objify-kvpairs pairs-seq)]
    (apply = (map set pairs-seq))))

(defn check-domain [domain-name handler pairs]
  (check-domain-pred domain-name handler pairs barr=))

(defn check-domain-not [domain-name handler pairs]
  (check-domain-pred domain-name handler pairs (complement barr=)))
