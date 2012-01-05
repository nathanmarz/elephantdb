(ns elephantdb.keyval.testing
  (:use midje.sweet
        elephantdb.common.testing
        [jackknife.logging :only (info)])
  (:require [jackknife.core :as u]
            [hadoop-util.test :as t]
            [elephantdb.keyval.core :as kv]
            [elephantdb.common.thrift :as thrift]
            [elephantdb.common.shard :as shard]
            [elephantdb.common.domain :as dom]
            [elephantdb.common.database :as db]
            [elephantdb.common.config :as conf])
  (:import [elephantdb Utils DomainSpec]
           [elephantdb.partition HashModScheme]
           [elephantdb.store DomainStore]
           [elephantdb.persistence Persistence KeyValPersistence Coordinator]
           [org.apache.hadoop.io IntWritable]
           [org.apache.thrift7 TException]
           [elephantdb.serialize KryoSerializer SerializationWrapper]
           [elephantdb.document KeyValDocument]))

;; ## Key Value Testing
;;
;; The following functions provide various helpers for manipulating
;;and accessing KeyValPersistence objects and the domains that wrap
;;them.

;; TODO: Index should be moved into a namespace that deals more
;; generally with domains, vs this key-value specific testing
;; namespace.

(defn index
  "Sinks the supplied document into the supplied persistence."
  [^Persistence persistence doc]
  (.index persistence doc))

(defn edb-get
  "Retrieves the supplied key from the supplied
  persistence. Persistence must be open."
  [^KeyValPersistence persistence key]
  (.get persistence key))

(defn edb-put
  "Sinks the supplied key and value into the supplied persistence."
  [^KeyValPersistence persistence key val]
  (.put persistence key val))

(defn get-all
  "Returns a sequence of all key-value pairs in the supplied
  persistence."
  [persistence]
  (doall
   (for [kv-pair (seq persistence)]
     [(.key kv-pair) (.value kv-pair)])))

(defn append-pairs
  "Accepts a sequence of kv-pairs and indexes each into an existing
  persistence at the supplied path."
  [coordinator path & kv-pairs]
  (with-open [db (.openPersistenceForAppend coordinator path {})]
    (doseq [[k v] kv-pairs]
      (index db k v))))

(defn create-pairs
  "Creates a persistence at the supplied path by indexing the supplied
  tuples."
  [coordinator path & kv-pairs]
  (.createPersistence coordinator path {})
  (apply append-pairs coordinator path kv-pairs))

(defn prep-coordinator
  "If the supplied coordinator implements SerializationWrapper, sets
  the proper serialization; else does nothing. In either case,
  `prep-coordinator` returns the supplied coordinator."
  [coordinator]
  (if (instance? SerializationWrapper coordinator)
    (doto coordinator
      (.setSerializer (KryoSerializer.)))
    coordinator))

(defn is-db-pairs?
  "Returns true if the persistence housed by the supplied coordinator
  contains the supplied pairs, false otherwise."
  [coordinator path & pairs]
  (with-open [db (.openPersistenceForRead coordinator path {})]
    (fact (get-all db) => (just pairs))))

;; bind this to get different behavior when making sharded domains.
;; TODO: Remove first arg from key->shard.
(def ^:dynamic test-key->shard
  (partial dom/key->shard "testdomain"))

(defn mk-sharded-domain
  [path domain-spec keyvals & {:keys [version]}]
  (t/with-local-tmp [lfs localtmp]
    (let [vs (DomainStore. path (conf/convert-clj-domain-spec domain-spec))
          dpath (if version
                  (do (when (.hasVersion vs version)
                        (.deleteVersion vs version))
                      (.createVersion vs version))
                  (.createVersion vs))
          shardedkeyvals (map
                          (fn [[k v]]
                            [(test-key->shard k (:num-shards domain-spec))
                             k v])
                          keyvals)]
      (with-open [writer (elephant-writer domain-spec
                                          dpath
                                          localtmp)]
        (doseq [[s k v] shardedkeyvals]
          (when v
            (-> writer
                (.write (IntWritable. s) (KeyValDocument. k v))))))
      (.succeedVersion vs dpath))))

;; ## Shard Mocking Functions

(defn wrap-keyval
  "Accepts a vector with [key, value] and returns a vector of
  sharding-key, document suitable for indexing into an ElephantDB
  Domain."
  [[k v]]
  [k (KeyValDocument. k v)])

(def shard-keyvals
  "Accepts a DomainSpec and a sequence of [key val] pairs and returns
  a map of shard->doc-seq. A custom sharding function can be supplied
  with the `:shard-fn` keyword."
  (mk-sharder wrap-keyval))

(def presharded-kv->writer!
  (mk-presharded->writer! #(KeyValDocument. %1 %2)))

(def unsharded-kv->writer!
  (mk-unsharded->writer! wrap-keyval))

(defn fill-with-writer!
  "Accepts a domain-spec, a path and a pre-sharded map of
  documents. sharded-docs is a map of shard->doc-seq."
  [spec path sharded-docs & {:keys [version]}]
  (t/with-local-tmp [lfs tmp]
    (let [store (DomainStore. path spec)
          dpath (create-version store
                                :version version
                                :force? true)]
      (with-open [writer (elephant-writer spec path tmp)]
        (doseq [[idx doc-seq] sharded-docs
                doc           doc-seq]
          (.write writer (IntWritable. idx) doc)))
      (.succeedVersion store dpath))))

(defn mk-sharded-domain
  [path spec kv-seq & {:keys [version]}]
  (mk-populated-store! path
                       spec
                       (map (fn [[k v]]
                              [k (KeyValDocument. k v)])
                            kv-seq)
                       :version version))

(defn reverse-pre-sharded [shardmap]
  (->> shardmap
       (u/val-map #(map first %))
       (u/reverse-multimap)
       (u/val-map first)))

(defn mk-presharded-domain [path coordinator shardmap]
  (let [keyvals (apply concat (vals shardmap))
        shards (reverse-pre-sharded shardmap)
        domain-spec {:num-shards (count shardmap)
                     :coordinator coordinator}]
    (binding [test-key->shard (fn [k _] (shards k))]
      (mk-sharded-domain path domain-spec keyvals))))

(defn mk-local-config [local-dir]
  {:local-dir local-dir
   :download-rate-limit 1024
   :update-interval-s 60})

(defn mk-service-handler
  [global-config localdir host->shards]
  (binding [shard/compute-host->shards (if host->shards
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
  `(t/with-fs-tmp [_ ~pathsym]
     (mk-sharded-domain ~pathsym ~domain-spec ~keyvals)
     ~@body))

(defmacro with-presharded-domain
  [[dname pathsym coordinator shardmap] & body]
  `(t/with-fs-tmp [_ ~pathsym]
     (mk-presharded-domain ~pathsym
                           ~coordinator
                           ~shardmap)
     (binding [dom/key->shard (let [rev# (reverse-pre-sharded ~shardmap)]
                                (fn [d# k# a#]
                                  (if (= d# ~dname)
                                    (rev# k#)
                                    (dom/key->shard d# k# a#))))]
       ~@body)))

(defmacro with-service-handler
  [[handler-sym hosts domains-conf & [host-to-shards]] & body]
  (let [global-conf {:replication 1 :hosts hosts :domains domains-conf}]
    `(t/with-local-tmp [lfs# localtmp#]
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
        (fact newv => nil?)
        (fact (apply str (map seq [k v newv])) => (pred v newv))))))

(defn kv-pairs= [& pair-seq]
  (apply = (map set pair-seq)))

(defn check-domain [domain-name handler pairs]
  (check-domain-pred domain-name handler pairs barr=))

(def check-domain-not
  (complement check-domain))

;; ## Key-Value Memory Persistence

(deftype MemoryPersistence [state]
  KeyValPersistence
  (get [this k]
    (get @(.state this) k))

  (put [this k v]
    (-> (.state this)
        (swap! assoc k v)))

  (index [this doc]
    (-> (.state this)
        (swap! assoc (.key doc) (.value doc))))

  (iterator [this]
    (map (fn [[k v]] (KeyValDocument. k v))
         @(.state this)))

  (close [_]))

(defrecord MemoryCoordinator [state]
  Coordinator
  (createPersistence [this root opts]
    (.openPersistenceForAppend this root opts))
  
  (openPersistenceForRead [this root opts]
    (.openPersistenceForAppend this root opts))

  (openPersistenceForAppend
    [{:keys [state] :as m} root opts]
    (or (get @state root)
        (let [fresh (MemoryPersistence. (atom {}))]
          (swap! state assoc root fresh)
          fresh))))

(defn memory-spec
  "Returns a DomainSpec initialized with an in-memory coordinator, a
  HashMod scheme and the supplied shard-count."
  [shard-count]
  (DomainSpec. (MemoryCoordinator. (atom {}))
               (HashModScheme.)
               shard-count))
