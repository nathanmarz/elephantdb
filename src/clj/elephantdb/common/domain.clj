(ns elephantdb.common.domain
  (:refer-clojure :exclude (conj!))
  (:use [jackknife.def :only (defalias)])
  (:require [hadoop-util.core :as h]
            [hadoop-util.transfer :as transfer]
            [jackknife.core :as u]
            [jackknife.logging :as log]
            [elephantdb.common.shard :as shard]
            [elephantdb.common.config :as conf]
            [elephantdb.common.status :as status])
  (:import [elephantdb Utils DomainSpec]
           [elephantdb.store DomainStore]
           [elephantdb.common.status IStateful IStatus KeywordStatus]
           [elephantdb.persistence ShardSet Shutdownable]
           [java.util.concurrent ExecutionException]))

;; Store manipulation

(defn try-domain-store
  "Attempts to return a DomainStore object from the current path and
  filesystem; if this doesn't exist, returns nil."
  [domain-path & {:keys [fs]}]
  (try (if fs
         (DomainStore. fs domain-path)
         (DomainStore. domain-path))
       (catch IllegalArgumentException _)))

(defmulti mk-local-store
  "Returns a new domain store based on the supplied remote store. The
   remote store can be a path to a remote store or the DomainStore
   itself."
  (fn [_ remote]
    (class remote)))

(defmethod mk-local-store nil
  [local-path null]
  (DomainStore. local-path))

(defmethod mk-local-store String
  [local-path remote-path]
  (try (mk-local-store local-path (DomainStore. remote-path))
       (catch IllegalArgumentException _
         (u/throw-runtime "No remote DomainStore at " remote-path))))

(defmethod mk-local-store DomainStore
  [local-path remote-vs]
  (DomainStore. local-path (.getSpec remote-vs)))

(defmethod mk-local-store DomainSpec
  [local-path spec]
  (DomainStore. local-path spec))

(defmethod mk-local-store clojure.lang.IPersistentMap
  [local-path spec]
  (let [spec (conf/convert-clj-domain-spec spec)]
    (DomainStore. local-path spec)))

;; ## Domain Getters

(defn domain-data
  "Returns a data-map w/ :version & :shards."
  [domain]
  @(.domainData domain))

(defn shard-set
  "Returns the set of shards for which the current domain is
  responsible."
  [domain]
  (shard/shard-set (.shardIndex domain)
                   (.hostname domain)))

(defn current-version
  "Returns the unix timestamp (in millis) of the current version being
  served by the supplied domain."
  [domain]
  (-> (domain-data domain)
      (get :version)))

(defmulti version-seq type)

(defmethod version-seq nil [_] nil)

(defmethod version-seq DomainStore
  [store]
  (seq (.getAllVersions store)))

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

(def loaded?
  "Returns true if the domain is loaded, false otherwise."
  (comp boolean domain-data))

(defn needs-update?
  "Returns true if the remote VersionedStore contains newer data than
  the local store,, false otherwise."
  [local-store remote-store]
  (or (not (has-data? local-store))
      (let [local-version  (.mostRecentVersion local-store)
            remote-version (.mostRecentVersion remote-store)]
        (when (and local-version remote-version)
          (< local-version remote-version)))))

;; ## Sinking and Sourcing
;;
;; These methods allow the user to sink and source objects from the
;; supplied domain.

;; ## Domain Tidying

(defn cleanup-domain!
  "Destroys all but the most recent version in the supplied domain's
  local store. Optionally, takes a `:to-keep` keyword option that
  keeps multiple versions."
  [domain & {:keys [to-keep]
             :or {to-keep 1}}]
  (doto (.localStore domain)
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
  [domain-store shard-idx version & {:keys [allow-writes]}]
  (let [fs (.getFileSystem domain-store)]
    (log/info "Opening shard #: " shard-idx)
    (when-not (.exists fs (h/path (.shardPath domain-store shard-idx)))
      (log/info "Shard doesn't exist. Creating shard # " shard-idx)
      (.createShard domain-store shard-idx))
    (u/with-ret (if allow-writes
                  (.openShardForAppend domain-store shard-idx)
                  (.openShardForRead domain-store shard-idx))
      (log/info "Opened shard #: " shard-idx))))

(defn retrieve-shards!
  "Accepts a domain object. On success, returns a sequence of opened
  Persistence objects for the supplied version. (Version must exist in
  local domain store!)"
  [domain version]
  (let [local-store (.localStore domain)
        shards      (shard-set domain)
        open!       (fn [idx]
                      (open-shard! local-store idx version
                                   :allow-writes (.allowWrites domain)))]
    (assert (has-version? local-store version)
            (str version "  doesn't exist."))
    (u/with-ret (->> (doall (map open! shards))
                     (zipmap shards))
      (log/info "Finished opening domain at " (.getRoot local-store)))))

(defn load-version!
  "Takes a domain, a version number (a long!), and a read-write lock,
  and hot-swaps in the new version for the old, closing all old shards
  on completion."
  [domain new-version]
  {:pre [(-> (.localStore domain)
             (has-version? new-version))]}
  (let [{:keys [shards version] :as data} (domain-data domain)]
    (if (= version new-version)
      (log/info new-version " is already loaded.")
      (let [new-shards (retrieve-shards! domain new-version)]
        (u/with-write-lock (.rwLock domain)
          (reset! (.domainData domain)
                  {:shards new-shards
                   :version new-version}))
        (status/to-ready domain)
        (close-shards! shards)))))

(defn boot-domain!
  "if a version of data exists in the local store, go ahead and start
  serving it. Otherwise do nothing."
  [domain]
  (when-let [latest (newest-version domain)]
    (load-version! domain latest)))

;; ### Sharding Logic
;;
;; These functions provide hints to a given domain about where to look
;;for a given sharding key.

;; TODO: Make this work with a sequence of keys!
(defn key->shard
  "Accepts a local store and a key (any object will do); returns the
  approprate shard number for the given key."
  [domain key]
  (when-let [version (current-version domain)]
    (let [^ShardSet shard-set (-> (.localStore domain)
                                  (.getShardSet version))]
      (.shardIndex shard-set key))))

(defn retrieve-shard
  "If the supplied domain contains the given sharding key, returns the
   Persistence object to which the key has been sharded, else returns
   nil."
  [domain key]
  (when-let [shard-idx (key->shard domain key)]
    (get-in (domain-data domain)
            [:shards shard-idx])))

(defn prioritize-hosts
  "Accepts a domain and a sharding-key and returns a sequence of hosts
  to try when attempting to find the Document paired with the sharding
  key."
  [domain key]
  (shard/prioritize-hosts (.shardIndex domain)
                          (key->shard domain key)
                          #{(.hostname domain)}))

(defn index!
  "Accepts a domain and any number of pairs of shard-key and indexable
  document, and indexes the supplied documents into the supplied
  persistence."
  [domain & pairs]
  {:pre [(.allowWrites domain)]}
  (u/with-write-lock (.rwLock domain)
    (when-let [shard-map (:shards (domain-data domain))]
      (doseq [[idx doc-seq] (group-by #(key->shard domain (first %))
                                      pairs)]
        (let [shard (shard-map idx)]
          (doseq [doc (map second doc-seq)]
            (.index shard doc)))))))

;; ## Domain Type Definition

(deftype Domain
    [localStore remoteStore serializer throttle rwLock
     hostname status domainData shardIndex allowWrites]
  clojure.lang.Seqable
  (seq [this]
    (when-let [data (domain-data this)]
      (mapcat #(lazy-seq %)
              (vals (:shards data)))))
  
  Shutdownable
  (shutdown [this]
    "Shutting down a domain requires closing all of its shards."
    (status/to-shutdown this)
    (u/with-write-lock rwLock
      (close-shards! (-> (domain-data this)
                         (get :shards)))))
 
  IStateful
  (get-status [_] @status)
  (to-ready [this]
    (status/swap-status! status status/to-ready))
  (to-loading [_]
    (status/swap-status! status status/to-loading))
  (to-failed [_ msg]
    (status/swap-status! status status/to-failed msg))
  (to-shutdown [_]
    (status/swap-status! status status/to-shutdown))

  IStatus
  (ready? [this] (status/ready? (status/get-status this)))
  (failed? [this] (status/failed? (status/get-status this)))
  (shutdown? [this] (status/shutdown? (status/get-status this)))
  (loading? [this] (status/loading? (status/get-status this))))

(defn domain? [x]
  (instance? Domain x))

(defmethod version-seq Domain
  [domain]
  (version-seq (.localStore domain)))

;; ## Domain Builder
;;
;; This is the main function used by a database to build its domain objects.

(defn build-domain
  "Constructs a domain record."
  [local-root
   & {:keys [throttle hdfs-conf remote-path hosts
             replication spec allow-writes]
      :or {hdfs-conf   {}
           hosts       [(u/local-hostname)]
           replication 1}}]
  (let [remote-fs     (h/filesystem hdfs-conf)
        remote-store  (when remote-path
                        (DomainStore. remote-fs remote-path))
        local-store   (mk-local-store local-root (or remote-store spec))
        local-spec    (.getSpec local-store)
        index (shard/generate-index hosts
                                    (.getNumShards local-spec)
                                    replication)]
    (doto (Domain. local-store
                   remote-store
                   (Utils/makeSerializer local-spec)
                   throttle
                   (u/mk-rw-lock)
                   (u/local-hostname)
                   (atom (KeywordStatus. :idle))
                   (atom nil)
                   index
                   allow-writes)
      (boot-domain!))))

;; ## Domain Updater Logic

(defalias throttle transfer/throttle
  "Returns a throttling agent for use in throttling domain updates.")

(defn transfer-shard!
  "Transfers the supplied shard (specified by `idx`) from the supplied
  remote version's remote store to the appropriate path on the local
  store."
  [domain version idx]
  (let [throttle     (.throttle domain)
        local-store  (.localStore domain)
        remote-store (.remoteStore domain)
        remote-fs    (.getFileSystem remote-store)
        remote-path  (.shardPath remote-store idx version)
        local-path   (.shardPath local-store idx version)]
    (if (.exists remote-fs (h/path remote-path))
      (do (log/info (format "Copying %s to %s." remote-path local-path))
          (transfer/rcopy remote-fs remote-path local-path
                          :throttle throttle)
          (log/info (format "Copied %s to %s." remote-path local-path)))
      (do (log/info "Shard doesn't exist. Creating shard # " idx)
          (.createShard local-store idx version)))))

(defn transfer-version!
  "Transfers the supplied version from the domain's remote store to
  its local store. To throttle the download, provide a throttling
  agent with the optional `:throttle` keyword argument."
  [domain version]
  (let [local-store  (.localStore domain)
        version-path (.createVersion local-store version)]
    (u/do-pmap (partial transfer-shard! domain version)
               (shard-set domain))
    (.succeedVersion local-store version-path)))

(defn transfer-possible?
  "Returns true if the remote store has the supplied version (and the
  local store doesn't), false otherwise."
  [domain version]
  (boolean
   (and (not (has-version? domain version))
        (-> (.remoteStore domain)
            (has-version? version)))))

(defn update-domain!
  "When a new version is available on the remote store,
  `update-domain!` transfers the version to the local store, hotswaps
  it in and closes the old version's shards. The domain's status is
  set appropriately at each stage.

  `update-domain!` accepts the following optional keyword arguments:

   :version - try to update to some version other than the most recent
  on the remote store."
  [domain & {:keys [version]}]
  (let [version (or version (.mostRecentVersion (.remoteStore domain)))]
    (when (transfer-possible? domain version)
      (doto domain
        (status/to-loading)
        (cleanup-domain!)
        (transfer-version! version)
        (load-version! version)
        (cleanup-domain!)))))

(defn attempt-update!
  "If the supplied domain isn't currently updating, returns a future
  containing a triggered update computation."
  [domain & {:keys [version]}]
  (when-not (status/loading? domain)
    (future (update-domain! domain :version version))))
