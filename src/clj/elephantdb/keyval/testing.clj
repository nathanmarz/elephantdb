(ns elephantdb.keyval.testing
  (:use clojure.test
        elephantdb.common.testing
        [jackknife.logging :only (info)])
  (:require [jackknife.core :as u]
            [elephantdb.keyval.core :as kv]
            [elephantdb.common.thrift :as thrift]
            [elephantdb.common.shard :as shard]
            [elephantdb.common.database :as db]
            [elephantdb.common.config :as conf])
  (:import [elephantdb Utils]
           [elephantdb.hadoop ElephantOutputFormat
            ElephantOutputFormat$Args LocalElephantManager]
           [elephantdb.store DomainStore]
           [elephantdb.persistence KeyValPersistence Coordinator]
           [org.apache.hadoop.io IntWritable]
           [org.apache.hadoop.mapred JobConf]
           [org.apache.thrift TException]
           [elephantdb.document KeyValDocument]))

;; ## Key Value Testing
;;
;; I haven't fixed this (or any of the tests) up yet.

(defn domain-data
  "TODO: Destroy. This doesn't make sense anymore with the new
  handling for objects."
  [& key-val-pairs]
  (map (fn [[k v]]
         [(barr k) (when v (apply barr v))])
       (partition 2 key-val-pairs)))

(defn index [persistence key value]
  (.index persistence (KeyValDocument. key value)))

(defn edb-get [persistence key]
  (.get persistence key))

(defn get-all [persistence]
  (doall
   (for [kv-pair (seq persistence)]
     [(.key kv-pair) (.value kv-pair)])))

(defn append-pairs [coordinator t spec & kv-pairs]
  (with-open [db (.openPersistenceForAppend coordinator t spec {})]
    (doseq [[k v] kv-pairs]
      (index db k v))))

(defn create-pairs [coordinator t spec & kv-pairs]
  (with-open [db (.createPersistence coordinator t spec {})]
    (doseq [[k v] kv-pairs]
      (index db k v))))

;; bind this to get different behavior when making sharded domains.
;; TODO: Remove first arg from key->shard.
(def ^:dynamic test-key->shard
  (partial shard/key->shard "testdomain"))

(defn mk-elephant-writer
  [shards coordinator output-dir tmpdir & kargs]
  (let [kargs (apply hash-map kargs)
        args (ElephantOutputFormat$Args.
              (conf/convert-clj-domain-spec
               {:num-shards shards
                :coordinator coordinator})
              output-dir)]
    (when-let [upd (:updater kargs)]
      (set! (. args updater) upd))
    (when-let [update-dir (:update-dir kargs)]
      (set! (. args updateDirHdfs) update-dir))
    (.getRecordWriter (ElephantOutputFormat.)
                      nil
                      (doto
                          (JobConf.)
                        (Utils/setObject ElephantOutputFormat/ARGS_CONF
                                         args)
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
                  (:coordinator domain-spec)
                  dpath
                  localtmp)]
      (doseq [[s k v] shardedkeyvals]
        (when v
          (.write writer
                  (IntWritable. s)
                  (KeyValDocument. k v))))
      (.close writer nil)
      (.succeedVersion vs dpath))))

(defn reverse-pre-sharded [shardmap]
  (->> shardmap
       (u/val-map #(map first %))
       (u/reverse-multimap)
       (u/val-map first)))

(defn mk-presharded-domain [fs path coordinator shardmap]
  (let [keyvals (apply concat (vals shardmap))
        shards (reverse-pre-sharded shardmap)
        domain-spec {:num-shards (count shardmap)
                     :coordinator coordinator}]
    (binding [test-key->shard (fn [k _] (shards k))]
      (mk-sharded-domain fs path domain-spec keyvals))))

(defn mk-local-config [local-dir]
  {:local-dir local-dir
   :download-rate-limit 1024
   :update-interval-s 60})

(defn mk-service-handler
  [global-config localdir host->shards]
  (binding [shard/compute-host->shards (if host-to-shards
                                         (constantly host->shards)
                                         shard/compute-host->shards)]
    (let [handler (db/build-database
                   (merge global-config (mk-local-config localdir)))]
      (while (not (.isFullyLoaded handler))
        (info "waiting...")
        (Thread/sleep 500))
      handler)))

(defmacro with-sharded-domain
  [[pathsym domain-spec keyvals] & body]
  `(with-fs-tmp [fs# ~pathsym]
     (mk-sharded-domain fs# ~pathsym ~domain-spec ~keyvals)
     ~@body))

(defmacro with-presharded-domain
  [[dname pathsym coordinator shardmap] & body]
  `(with-fs-tmp [fs# ~pathsym]
     (mk-presharded-domain fs#
                           ~pathsym
                           ~coordinator
                           ~shardmap)
     (binding [shard/key->shard (let [rev# (reverse-pre-sharded ~shardmap)]
                                  (fn [d# k# a#]
                                    (if (= d# ~dname)
                                      (rev# k#)
                                      (shard/key->shard d# k# a#))))]
       ~@body)))

(defmacro with-service-handler
  [[handler-sym hosts domains-conf & [host-to-shards]] & body]
  (let [global-conf {:replication 1 :hosts hosts :domains domains-conf}]
    `(with-local-tmp [lfs# localtmp#]
       (let [~handler-sym (mk-service-handler ~global-conf
                                              localtmp#
                                              ~host-to-shards)
             updater# (db/launch-updater! ~handler-sym 100)]
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
          (kv/mk-value (second myval))
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

(defn kv-pairs= [& pairs-seq]
  (apply = (map set pairs-seq)))

(defn check-domain [domain-name handler pairs]
  (check-domain-pred domain-name handler pairs barr=))

(def check-domain-not
  (complement check-domain))

(defn memory-persistence []
  (let [state (atom {})]
    (reify elephantdb.persistence.Persistence
      (index [this doc]
        (swap! state assoc (.key doc) (.val doc)))
      (iterator [this]
        (map (fn [[k v]] (KeyValDocument. k v))
             @state))
      (close [_]))))

(defn memory-coordinator []
  (let [state (atom {})]
    (reify elephantdb.persistence.Coordinator
      (openPersistenceForRead [this root opts]
        (or (get @state root)
            (let [fresh (memory-persistence)]
              (swap! state assoc root fresh)
              fresh))))))

;; ## Type Versions
;;
;; TODO -- convert to KeyValPersistence

(defrecord MemoryPersistence [state]
  elephantdb.persistence.Persistence
  (index [{:keys [state]} doc]
    (swap! state assoc (.key doc) (.val doc)))
  (iterator [{:keys [state]}]
    (map (fn [[k v]] (KeyValDocument. k v))
         @state))
  (close [_]))

(defrecord MemoryCoordinator [state]
  elephantdb.persistence.Coordinator
  (openPersistenceForRead
    [{:keys [state] :as m} root opts]
    (println (:state m))
    (or (get @state root)
        (let [fresh (MemoryPersistence. (atom {}))]
          (swap! state assoc root fresh)
          fresh))))
