(ns elephantdb.common.domain
  (:use [jackknife.def :only (defalias)])
  (:require [hadoop-util.core :as h]
            [jackknife.logging :as log]
            [jackknife.core :as u]
            [elephantdb.common.hadoop :as hadoop]
            [elephantdb.common.shard :as shard]
            [elephantdb.common.status :as status]
            [elephantdb.keyval.thrift :as thrift])
  (:import [elephantdb.store DomainStore]
           [elephantdb.common.status IStateful]
           [elephantdb.persistence ShardSet Shutdownable]))

;; ## Domain Getters

(defn domain-data
  "Returns a data-map w/ :version & :shards."
  [domain]
  @(:domain-data domain))

(defn shard-set
  "Returns the set of shards the current domain is responsible for."
  [{:keys [shard-index hostname]}]
  (shard/shard-set shard-index hostname))

(defn current-version
  "Returns the unix timestamp (in millis) of the current version being
  served by the supplied domain."
  [domain]
  (-> (domain-data domain)
      (get :version)))

(def updating?
  "Returns true if the domain is currently serving data and updating
  from the remote store, false otherwise."
  (every-pred status/loading? status/ready?))

;; Store manipulation

(defn try-domain-store
  "Attempts to return a DomainStore object from the current path and
  filesystem; if this doesn't exist, returns nil."
  [fs domain-path]
  (try (DomainStore. fs domain-path)
       (catch IllegalArgumentException _)))

(defn mk-local-store
  [local-path remote-vs]
  (DomainStore. local-path (.getSpec remote-vs)))

(defmulti version-seq type)

(defmethod version-seq DomainStore
  [store]
  (into [] (.getAllVersions store)))

(def newest-version
  "Returns the newest version for the supplied object, nil if it
   doesn't exist."
  (comp first version-seq))

(defn has-version?
  "Returns true if the supplied Domain or DomainStore has the supplied
  version available, false otherwise."
  [store version]
  (some #{version} (version-seq store)))

(def has-data?
  "Returns true if the supplied object has some available version to
  load, false otherwise."
  (comp boolean newest-version))

(defn needs-update?
  "Returns true if the remote VersionedStore contains newer data than
  the local store,, false otherwise."
  [local-store remote-store]
  (or (not (has-data? local-store))
      (let [local-version  (.mostRecentVersion local-store)
            remote-version (.mostRecentVersion remote-store)]
        (when (and local-version remote-version)
          (< local-version remote-version)))))

;; ## Domain Tidying

(defn cleanup-domain!
  "Destroys all but the most recent version in the supplied domain's
  local store. Optionally, takes a `:to-keep` keyword option that
  keeps multiple versions."
  [domain & {:keys [to-keep]
             :or {to-keep 1}}]
  (doto (:local-store domain)
    (.cleanup to-keep)))

(defn cleanup-domains!
  "Destroys every old version (in parallel) for each of the supplied domains."
  [domain-seq & {:keys [to-keep]}]
  (u/do-pmap #(cleanup-domain! % :to-keep to-keep)
             domain-seq))

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
  "Closes the supplied Persistence. Returns nil on success. throws
   IOException when some sort of failure occurs."
  [lp & {:keys [error-msg]}]
  (try (.close lp)
       (catch java.io.IOException t
         (log/error t error-msg)
         (throw t))))

(defn close-shards!
  "Closes all shards in the supplied shard-map. (A shard map is a map
  of index->Persistence instance.)"
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

(defn retrieve-shards!
  "Accepts a domain object. On success, returns a sequence of opened
  Persistence objects for the supplied version. (Version must exist in
  local domain store!)"
  [{:keys [local-store] :as domain} version]
  {:pre [(has-version? local-store version)]}
  (let [shards (shard-set domain)]
    (u/with-ret (->> (u/do-pmap (fn [idx]
                                  (open-shard! local-store idx version))
                                shards)
                     (zipmap shards))
      (log/info "Finished opening domain at " (.getRoot local-store)))))

(defn load-version!
  "Takes a domain, a version number (a long!), and a read-write lock,
  and hot-swaps in the new version for the old, closing all old shards
  on completion."
  [domain new-version rw-lock]
  {:pre [(-> (:local-store domain)
             (has-version? new-version))]}
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
  "if a version of data exists in the local store, go ahead and start
  serving it. Otherwise do nothing."
  [domain rw-lock]
  (when-let [latest (newest-version domain)]
    (load-version! domain latest rw-lock)))

;; ## Domain Record Definition

(defn swap-status!
  "Accepts a domain and a transition function (from the
  elephantdb.common.status.IStateful interface) and returns the new
  status."
  [{:keys [status] :as domain} transition-fn & args]
  (apply swap! status transition-fn args))

(defrecord Domain
    [local-store remote-store serializer
     hostname status domain-data shard-index]  
  Shutdownable
  (shutdown [this]
    "Shutting down a domain requires closing all of its shards."
    (status/to-shutdown this)
    (close-shards! (-> (domain-data this)
                       (get :shards))))
 
  IStateful
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
  [domain]
  (version-seq (:local-store domain)))

;; ## Domain Builder
;;
;; This is the main function used by a database to build its domain objects.

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

;; ## Domain Updater Logic

(defalias throttle hadoop/throttle
  "Returns a throttling agent for use in throttling domain updates.")

(defn transfer-shard!
  "Transfers the supplied shard (specified by `idx`) from the supplied
  remote version's remote store to the appropriate path on the local
  store."
  [{:keys [local-store remote-store]} version idx]
  (let [remote-fs   (.getFileSystem remote-store)
        remote-path (.shardPath remote-store idx version)
        local-path  (.shardPath local-store idx version)]
    (if (.exists remote-fs remote-path)
      (do (log/info
           (format "Copied %s to %s." remote-path local-path))
          (hadoop/rcopy remote-fs remote-path local-path
                        :throttle throttle)
          (log/info
           (format "Copied %s to %s." remote-path local-path)))
      (do (log/info
           "Shard %s did not exist. Creating Empty LP." remote-path)
          (.close (.createShard local-store idx version))))))

(defn transfer-version!
  "Transfers the supplied version from the domain's remote store to
  its local store. To throttle the download, provide a throttling
  agent with the optional `:throttle` keyword argument."
  [domain version & {:keys [throttle]}]
  (let [{:keys [local-store remote-store]} domain]
    (.createVersion local-store version)
    (u/do-pmap (partial transfer-shard! domain version)
               (shard-set domain))
    (.succeedVersion local-store version)))

(defn transfer-possible?
  "Returns true if the remote store has the supplied version (and the
  local store doesn't), false otherwise."
  [domain version]
  (and (not (has-version? domain version))
       (-> (:remote-store domain)
           (has-version? version))))

(defn update-domain!
  "When a new version is available on the remote store,
  `update-domain!` transfers the version to the local store, hotswaps
  it in and closes the old version's shards. The domain's status is
  set appropriately at each stage.

  `update-domain!` accepts the following optional keyword arguments:

   :throttle - provide an optional throttling agent.
   :version - try to update to some version other than the most recent
  on the remote store."
  [{:keys [remote-store] :as domain} rw-lock
   & {:keys [throttle version]
      :or {version (.mostRecentVersion remote-store)}}]
  (when (transfer-possible? domain version)
    (status/to-loading domain)
    (transfer-version! domain version :throttle throttle)
    (load-version! version rw-lock)
    (status/to-ready domain)))

(defn attempt-update!
  "If the supplied domain isn't currently updating, returns a future
  containing a triggered update computation."
  [domain rw-lock & {:keys [throttle version]}]
  (if (updating? domain)
    (log/info "UPDATER - Not updating as update's still in progress.")
    (future (update-domain! domain rw-lock
                            :throttle throttle
                            :version version))))

;; ### Sharding Logic
;;
;; These functions provide hints to a given domain about where to look
;;for a given sharding key.

(defn key->shard
  "Accepts a local store and a key (any object will do); returns the
  approprate shard number for the given key."
  [{:keys [local-store]} key]
  (let [^ShardSet shard-set (.getShardSet local-store)]
    (.shardIndex shard-set key)))

(defn retrieve-shard
  "If the supplied domain contains the given sharding key, returns the
   Persistence object to which the key has been sharded, else returns
   nil."
  [domain key]
  (let [shard-idx (key->shard domain key)]
    (get-in (domain-data domain)
            [:shards shard-idx])))

(defn prioritize-hosts
  "Accepts a domain and a sharding-key and returns a sequence of hosts
  to try when attempting to find the Document paired with the sharding
  key."
  [{:keys [shard-index hostname] :as domain} key]
  (shard/prioritize-hosts shard-index
                          (key->shard domain key)
                          #{hostname}))
