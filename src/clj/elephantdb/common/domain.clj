(ns elephantdb.common.domain
  (:require [hadoop-util.core :as h]
            [jackknife.logging :as log]
            [jackknife.core :as u]
            [elephantdb.common.hadoop :as hadoop]
            [elephantdb.common.shard :as shard]
            [elephantdb.common.status :as status]
            [elephantdb.common.thrift :as thrift])
  (:import [elephantdb.store DomainStore]
           [elephantdb.common.status IStateful]
           [elephantdb.persistence ShardSet Shutdownable]))

;; ## Testing functions

(defn specs-match?
  "Returns true of the specs of all supplied DomainStores match, false
  otherwise."
  [& stores]
  (apply = (map (fn [^DomainStore x] (.getSpec x))
                stores)))

;; `existing-shard-seq` is good for testing, but we don't really need
;; it in a working production system (since the domain knows what
;; shards should be holding.)

(defn existing-shard-seq
  "Returns a sequence of all shards present on the fileystem for the
  supplied store and version. Useful for testing."
  [store version]
  (let [num-shards (.. store getSpec getNumShards)
        filesystem (.getFileSystem store)]
    (filter (fn [idx]
              (->> (.shardPath store idx version)
                   (.exists filesystem)))
            (range num-shards))))

;; Store manipulation

(defmulti version-seq type)

(defmethod version-seq DomainStore
  [store]
  (into [] (.getAllVersions store)))

(def newest-version
  "Returns the newest version for the supplied object, nil if it
   doesn't exist."
  (comp first version-seq))

(def has-data?
  "Returns true if the supplied object has some available version to
  load, false otherwise."
  (comp boolean newest-version))

(defn needs-update?
  "Returns true if the remote VersionedStore contains newer data than
  its local copy, false otherwise."
  [local-vs remote-vs]
  (or (not (has-data? local-vs))
      (let [local-version  (.mostRecentVersion local-vs)
            remote-version (.mostRecentVersion remote-vs)]
        (when (and local-version remote-version)
          (< local-version remote-version)))))

(defn try-domain-store
  "Attempts to return a DomainStore object from the current path and
  filesystem; if this doesn't exist, returns nil."
  [fs domain-path]
  (try (DomainStore. fs domain-path)
       (catch IllegalArgumentException _)))

(defn mk-local-store
  [local-path remote-vs]
  (DomainStore. local-path (.getSpec remote-vs)))

;; ## Domain Manipulation Functions

(defn cleanup-domain!
  "Destroys all but the most recent version in the local versioned
   store located at `domain-path`."
  [domain & {:keys [to-keep]
             :or {to-keep 1}}]
  (doto (:local-store domain)
    (.cleanup to-keep)))

(defn cleanup-domains!
  "Destroys every old version for each of the supplied domains."
  [domain-seq]
  (u/do-pmap cleanup-domain! domain-seq))

;; ## Shard Manipulation
;;
;; TODO: In the future, `close-shard` should make use of
;; [slingshot](https://github.com/scgilardi/slingshot) to actually
;; throw a data structure describing what's happened up the way. If we
;; know that there was a problem closing a given persistence we can
;; assist the user by actually reporting what happened. This will be
;; especially important when displaying this information in the
;; ElephantDB UI.

(defn close-shard!
  "Returns nil on success. throws IOException when some sort of
   failure occurs."
  [lp & {:keys [error-msg]}]
  (try (.close lp)
       (catch Throwable t
         (log/error t error-msg)
         (throw t))))

;; TODO: Think about some sort of data structure logging here, or a
;; better way of reporting what's happened.
(defn close-shards!
  [shard-map]
  (doseq [[idx shard] shard-map]
    (log/info "Closing shard #: " idx)
    (close-shard! shard)
    (log/info "Closed shard #: " idx)))

(defn open-shard!
  "Opens and returns a Persistence object standing in for the shard
  with the supplied index."
  [domain-store shard-idx version]
  (log/info "Opening shard #: " shard-idx)
  (u/with-ret (.openShardForRead domain-store shard-idx)
    (log/info "Opened shard #: " shard-idx)))

;; TODO: This should look inside the host->shard map.
(defn retrieve-shards!
  "Accepts a domain object and returns a sequence of opened
  Persistence objects on success."
  [{:keys [local-store]} version]
  {:pre [(some #{version} (version-seq local-store))]}
  (let [shards (existing-shard-seq local-store version)]
    (u/with-ret (->> (u/do-pmap (fn [idx]
                                  (open-shard! local-store idx version))
                                shards)
                     (zipmap shards))
      (log/info "Finished opening domain at " (.getRoot local-store)))))

(defn swap-status!
  "Accepts a domain and a transition function (from the
  elephantdb.common.status.IStateful interface) and returns the new
  status."
  [{:keys [status] :as domain} transition-fn & args]
  (apply swap! status transition-fn args))

(defn domain-data
  "Returns a data-map w/ :version & :shards."
  [domain]
  @(:domain-data domain))

(defn current-version
  "Returns the unix timestamp (in millis) of the current version being
  served by the supplied domain."
  [domain]
  (-> (domain-data domain)
      (get :version)))

(defn shard-set
  "Returns the set of shards the current domain is responsible for."
  [{:keys [shard-index hostname]}]
  (shard/shard-set shard-index hostname))

;; This is wonderful! Hot swapping is taken care of completely.
(defn load-version!
  "Takes a domain, a version number (a long!), and a read-write lock
  and performs data swappage."
  [domain new-version rw-lock]
  (let [{:keys [shards version] :as data} (domain-data domain)]
    (if (= version new-version)
      (log/info new-version " is already loaded.")
      (let [new-shards (retrieve-shards! domain new-version)]
        (u/with-write-lock rw-lock
          (reset! (:domain-data domain)
                  {:shards new-shards
                   :version new-version}))
        (status/to-ready domain)
        (close-shards! shards)))))

(defn boot-domain!
  "if a version of data exists, go ahead and start serving
  it. Otherwise do nothing."
  [domain rw-lock]
  (when-let [latest (newest-version domain)]
    (load-version! domain latest rw-lock)))

;; ## Record Definition

(defrecord Domain
    [local-store remote-store serializer
     hostname status domain-data shard-index]  
  Shutdownable
  (shutdown [this]
    (status/to-shutdown this)
    (close-shards! (-> (domain-data this)
                       (get :shards))))
 
  IStateful
  ;; Allows for more elegant state transitions.
  (status [this]
    @(:status this))
  (to-ready [this]
    (swap-status! this status/to-ready))
  (to-loading [this]
    (swap-status! this status/to-loading))
  (to-failed [this msg]
    (swap-status! this status/to-failed msg))
  (to-shutdown [this]
    (swap-status! this status/to-shutdown)))

(defmethod version-seq Domain
  [{store :local-store}]
  (version-seq store))

;; ## The Big Guns

(defn build-domain
  "Constructs a domain record."
  [local-root hdfs-conf remote-path hosts replication]
  (let [rfs (h/filesystem hdfs-conf)
        remote-store  (DomainStore. rfs remote-path)
        local-store   (mk-local-store local-root remote-store)
        index (shard/generate-index hosts (-> local-store .getSpec .getNumShards))]
    (Domain. local-store
             remote-store
             (-> local-store .getSpec .getCoordinator .getKryoBuffer)
             (u/local-hostname)
             (atom (thrift/loading-status))
             (atom {})
             index)))

;; ## Updater Logic

(def updating?
  "TODO: Check that this is correct."
  (every-pred status/loading? status/ready?))

(defn has-version? [domain version]
  (some #{version} (version-seq domain)))

(defn transfer-shard!
  "TODO: Docs!"
  [{:keys [local-store remote-store]} version idx]
  (let [remote-fs   (.getFileSystem remote-store)
        remote-path (.shardPath remote-store idx version)
        local-path  (.shardPath local-store idx version)]
    (if (.exists remote-fs remote-shard-path)
      (do (log/info
           (format "Copied %s to %s." remote-path local-path))
          (hadoop/rcopy remote-fs remote-path local-path :throttle throttle)
          (log/info
           (format "Copied %s to %s." remote-path local-path)))
      (do (log/info
           "Shard %s did not exist. Creating Empty LP." remote-path)
          (.close (.createShard local-store idx version))))))

(defn transfer-version!
  "TODO: Docs!"
  [domain version & {:keys [throttle]}]
  (let [{:keys [local-store remote-store]} domain]
    (.createVersion local-store version)
    (u/do-pmap (partial transfer-shard! domain version)
               (shard-set domain))
    (.succeedVersion local-store version)))

;; TODO: Do we have the right semantics here for loading?
(defn update-domain!
  [{:keys [remote-store] :as domain} rw-lock
   & {:keys [throttle version]
      :or {version (.mostRecentVersion remote-store)}}]
  (when (and (not (has-version? domain version))
             (has-version? (:remote-store domain) version))
    (status/to-loading domain)
    (transfer-version! domain version :throttle throttle)
    (load-version! version rw-lock)
    (status/to-ready domain)))

(defn attempt-update!
  [domain rw-lock & {:keys [throttle]}]
  (if (updating? domain)
    (log/info "UPDATER - Not updating as update's still in progress.")
    (future (update-domain! rw-lock :throttle throttle))))

;; ### Sharding Logic
;;
;; These functions provide hints to a given domain about where to look
;;for some key.

(defn key->shard
  "Accepts a local store and a key (any object will do); returns the
  approprate shard number for the given key."
  [{:keys [local-store]} key]
  (let [^ShardSet shard-set (.getShardSet local-store)]
    (.shardIndex shard-set key)))

(defn retrieve-shard
  "If the supplied domain contains the given key, returns the
  Persistence object. Returns nil if the key wasn't sharded to this
  particular domain."
  [domain key]
  (let [shard-idx (key->shard domain key)]
    (get-in (domain-data domain)
            [:shards shard-idx])))

(defn prioritize-hosts
  "Accepts a domain and a sharding-key and returns a sequence of hosts
  to try when attempting to process some value."
  [{:keys [shard-index hostname] :as domain} key]
  (shard/prioritize-hosts shard-index
                          (key->shard domain key)
                          #{hostname}))
