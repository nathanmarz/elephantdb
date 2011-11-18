(ns elephantdb.keyval.testing
  (:use clojure.test
        elephantdb.common.testing)
  (:require [hadoop-util.core :as h]
            [elephantdb.keyval.service :as service]
            [elephantdb.common.thrift :as thrift]
            [elephantdb.common.shard :as shard]
            [elephantdb.common.config :as conf]
            [elephantdb.common.logging :as log]
            [elephantdb.common.util :as u])
  (:import [elephantdb Utils ByteArray]
           [elephantdb.hadoop ElephantRecordWritable ElephantOutputFormat
            ElephantOutputFormat$Args LocalElephantManager]
           [elephantdb.store DomainStore]
           [org.apache.hadoop.io IntWritable]
           [org.apache.hadoop.mapred JobConf]
           [org.apache.thrift TException]))

(defn domain-data [& key-val-pairs]
  (map (fn [x]
         [(barr (first x))
          (if-not (nil? (second x))
            (apply barr (second x)))])
       (partition 2 key-val-pairs)))

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

;; bind this to get different behavior when making sharded domains.
;; TODO: Remove first arg from key-shard.
(def ^:dynamic test-key->shard
  (partial shard/key-shard "testdomain"))

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
                            [(test-key->shard k (:num-shards domain-spec))
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
        domain-spec {:num-shards (count shardmap)
                     :persistence-factory factory}]
    (binding [test-key->shard (fn [k _] (shards (ByteArray. k)))]
      (mk-sharded-domain fs path domain-spec keyvals))))

(defn mk-local-config [local-dir]
  {:local-dir local-dir
   :max-online-download-rate-kb-s 1024
   :update-interval-s 60})

(defn mk-service-handler
  [global-config localdir host-to-shards]
  (binding [shard/compute-host-to-shards (if host-to-shards
                                           (constantly host-to-shards)
                                           shard/compute-host-to-shards)]
    (let [handler (service/service-handler
                   (merge global-config (mk-local-config localdir)))]
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
     (mk-presharded-domain fs#
                           ~pathsym
                           ~factory
                           ~shardmap)
     (binding [shard/key-shard (let [rev# (reverse-pre-sharded ~shardmap)]
                                 (fn [d# k# a#]
                                   (if (= d# ~dname)
                                     (rev# (ByteArray. k#))
                                     (shard/key-shard d# k# a#))))]
       ~@body)))

(defmacro with-service-handler
  [[handler-sym hosts domains-conf & [host-to-shards]] & body]
  (let [global-conf {:replication 1 :hosts hosts :domains domains-conf}]
    `(with-local-tmp [lfs# localtmp#]
       (let [~handler-sym (mk-service-handler ~global-conf
                                              localtmp#
                                              ~host-to-shards)
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
  ;; mock service/try-multi-get only for non local-hostname hosts
  `(binding [service/multi-get-remote
             (mk-mocked-remote-multiget-fn ~domain-to-host-to-shards
                                           ~shards-to-pairs
                                           ~down-hosts)]
     ~@body))

(defmacro with-single-service-handler
  [[handler-sym domains-conf] & body]
  `(with-service-handler [~handler-sym [(u/local-hostname)] ~domains-conf]
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
