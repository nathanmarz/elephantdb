(ns elephantdb.test.keyval
  (:use elephantdb.test.common
        [elephantdb.common.domain :only (build-domain)]
        [jackknife.logging :only (info with-log-level)])
  (:require [jackknife.core :as u]
            [hadoop-util.test :as t]
            [elephantdb.keyval.core :as kv]
            [elephantdb.common.thrift :as thrift]
            [elephantdb.common.database :as db])
  (:import [elephantdb  DomainSpec]
           [elephantdb.partition HashModScheme]
           [elephantdb.persistence Persistence KeyValPersistence Coordinator]
           [org.apache.hadoop.io IntWritable]
           [org.apache.thrift TException]
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
;;
;; ### Persistence & Coordinator Level Tests

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
  "Returns a map containing all key-value pairs in the supplied
  KeyValuePersistence."
  [persistence]
  (into {} (for [kv-pair (seq persistence)]
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

;; ## DomainStore Level Testing

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

(defn mk-kv-domain
  "Accepts a domain-spec, path, and a sequence of kv-pairs and
   populated the domain at the supplied path.

  Optional keyword arguments are :shard-fn and :version."
  [spec path kv-seq & {:keys [version shard-fn]}]
  (let [creator (mk-unsharded-domain-creator wrap-keyval)]
    (creator spec path kv-seq
             :version  version
             :shard-fn shard-fn)))

(defmacro with-domain
  "Used as:

   (with-domain [my-domain domain-spec
                 {1 2, 3 4}
                 :version 5
                 :shard-fn (constantly 10)]
          (seq my-domain))

  A domain with the supplied domain-spec is bound to `sym` inside the
  body of `with-domain`."
  [[sym spec kv-pairs & {:keys [version shard-fn]}] & body]
  `(with-log-level :off
     (t/with-fs-tmp [fs# path#]
       (mk-kv-domain ~spec path# ~kv-pairs
                     :version ~version
                     :shard-fn ~shard-fn)
       (let [~sym (build-domain path#)]
         ~@body))))

(defn mk-presharded-kv-domain
  "Accepts a domain-spec, path, and a map of shard->kv-pair-seq and
   populated the domain at the supplied path.

  Optional keyword arguments are :shard-fn and :version."
  [spec path shard-map & {:keys [version]}]
  (let [creator (mk-domain-creator (fn [[k v]]
                                     (KeyValDocument. k v)))]
    (creator spec path shard-map
             :version  version)))

(defmacro with-presharded-domain
  "Unlike with-domain, with-presharded-domain accepts a map of
   shard->key-val sequence.

   (with-domain [my-domain domain-spec
                {0 [[1 2] [3 4]]
                 3 [[4 5]]}
                 :version 100]
          (seq my-domain))"
  [[sym spec shard-map & {:keys [version]}] & body]
  `(with-log-level :off
     (t/with-fs-tmp [fs# path#]
       (mk-presharded-kv-domain ~spec path# ~shard-map
                                :version ~version)
       (let [~sym (build-domain path#)]
         ~@body))))

;; ## Thrift Service Testing Helpers

(defn mk-local-config [local-dir]
  {:local-root local-dir
   :download-rate-limit 1024
   :update-interval-s 60})

;; TODO: Add in the ability to re-bind the host->shard decision.

(defn mk-service-handler [database]
  (db/prepare database)
  (let [handler  (doto (kv/kv-service database)
                   (.updateAll))]
    (loop [times 20]
      (if (pos? times)
        (if (.isFullyLoaded handler)
          handler
          (do (info "waiting...")
              (Thread/sleep 50)
              (recur (dec times))))
        (throw (RuntimeException. "Couldn't load handler!"))))))

;; TODO: Fake out host->shard function, start an updater, and cancel
;; with future-cancel.
(defn with-service-handler*
  [handler-fn name->docs & {:keys [host-shard-fn conf-map]}]
  (t/with-fs-tmp [_ remote-tmp]
    (t/with-local-tmp [_ local-tmp]
      (let [db (build-unsharded-test-db local-tmp remote-tmp
                                        name->docs conf-map)
            updater (db/launch-updater! db (:update-interval-s conf-map))
            handler (mk-service-handler db)]
        (try (handler-fn handler)
             (finally (.shutdown db)
                      (future-cancel updater)))))))

(defmacro with-service-handler
  [[sym & opts] & body]
  `(with-service-handler* (fn [~sym] ~@body) ~@opts))

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
        (if-let [myval (first (filter #(= key (first %)) pairs))]
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

(comment
  "OVERHAUL these. Remove fact references."
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
    (complement check-domain)))

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
