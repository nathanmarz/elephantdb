(ns elephantdb.common.fresh
  (:use [elephantdb.keyval.thrift :only (with-elephant-connection)]
        [elephantdb.common.iface :only (shutdown)])
  (:require [hadoop-util.core :as h]
            [elephantdb.common.util :as u]
            [elephantdb.common.status :as stat]
            [elephantdb.common.shard :as shard]
            [elephantdb.common.logging :as log]
            [elephantdb.common.thrift :as thrift])
  (:import [elephantdb.store DomainStore]
           [elephantdb.persistence ShardSet]
           [elephantdb.common.status KeywordStatus IStateful]
           [elephantdb.common.iface IShutdownable IVersioned]
           [org.apache.thrift TException]
           [elephantdb.generated ElephantDB ElephantDB$Iface ElephantDB$Processor
            WrongHostException DomainNotFoundException DomainNotLoadedException]))

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

(defn needs-update?
  "Returns true if the remote VersionedStore contains newer data than
  its local copy, false otherwise."
  [local-vs remote-vs]
  (or (not (has-data? local-vs))
      (let [local-version  (.mostRecentVersion local-vs)
            remote-version (.mostRecentVersion remote-vs)]
        (when (and local-version remote-version)
          (< local-version remote-version)))))

(defmulti version-seq type)

(defmethod version-seq DomainStore
  [store]
  (into [] (.getAllVersions store)))

(defmethod version-seq Domain
  [{store :local-store}]
  (version-seq store))

(def newest-version
  "Returns the newest version for the supplied object, nil if it
   doesn't exist."
  (comp first versions-seq))

(def has-data?
  "Returns true if the supplied object has some available version to
  load, false otherwise."
  (comp boolean newest-version))

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
        (stat/to-ready domain)
        (close-shards! shards)))))

(defn boot-domain!
  "if a version of data exists, go ahead and start serving
  it. Otherwise do nothing."
  [domain rw-lock]
  (when-let [latest (newest-version domain)]
    (activate! domain latest rw-lock)))

;; ## Record Definition

(defrecord Domain
    [local-store remote-store serializer
     hostname status domain-data shard-index]  
  IShutdownable
  (shutdown [this]
    ;; status needs a better interface.
    (stat/to-shutdown this)
    (close-shards! (-> (domain-data this)
                       (get :shards))))
 
  IStateful
  ;; Allows for more elegant state transitions.
  (status [this]
    @(:status this))
  (to-ready [this]
    (swap-status! this stat/to-ready))
  (to-loading [this]
    (swap-status! this stat/to-loading))
  (to-failed [this msg]
    (swap-status! this stat/to-failed msg))
  (to-shutdown [this]
    (swap-status! this stat/to-shutdown)))

;; ## The big daddy!

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

;; BREAK!
;; 
;; ## Examples
;;
;; The configuration was traditionally split up into global and
;;local. We need to have a global config to allow all machines to
;;access the information therein. That's a deploy issue, I'm
;;convinced; the actual access to either configuration should occur in
;;the same fashion.
;;
;; Here's an example configuration:

(def example-config
  {:replication 1
   :port 3578
   :download-cap 1024
   :local-root "/Users/sritchie/Desktop/domainroot"
   :hosts ["localhost"]
   :domains {"graph" "/mybucket/elephantdb/graph"
             "docs"  "/data/docdb"}
   :hdfs-conf {"fs.default.name"
               "hdfs://hadoop-devel-nn.local.twitter.com:8020"}
   :blob-conf {"fs.default.name"
               "hdfs://hadoop-devel-nn.local.twitter.com:8020"}})

;; ## Database Creation
;;
;; Database level functions here.

(defn purge-unused-domains!
  "Walks through the supplied database's local directory, recursively
  deleting all directories with names that aren't present in the
  supplied `domain-seq`."
  [{:keys [local-dir domains]}]
  (let [domain-set (into #{} (keys domains))]
    (u/dofor [domain-path (-> local-dir h/mk-local-path .listFiles)
              :when (and (.isDirectory domain-path)
                         (not (contains? domain-set
                                         (.getName domain-path))))]
             (log/info "Deleting local path of deleted domain: " domain-path)
             (h/delete (h/local-filesystem)
                       (.getPath domain-path) true))))

(defn domain-path
  "Returns the root path that should be used for a domain with the
  supplied name located within the supplied elephantdb root
  directory."
  [local-root domain-name]
  (str local-root "/" domain-name))

;; A database ends up being the initial configuration map with much
;; more detail about the individual domains. The build-database
;; function merges in the proper domain-maps for each listed domain.

(defn build-database
  [{:keys [domains hosts replication local-root hdfs-conf] :as conf-map}]
  (assoc conf-map
    :domains (u/update-vals
              domains
              (fn [domain-name remote-path]
                (let [local-path (domain-path local-root domain-name)]
                  (build-domain
                   local-root hdfs-conf remote-path hosts replication))))))

;; A full database ends up looking something like the commented out
;; block below. Previously, a large number of functions would try and
;; update all databases at once. With Clojure's concurrency mechanisms
;; we can treat each domain as its own thing and dispatch futures to
;; take care of each in turn.

;; ## Database Manipulation Functions

(defn domain-get [database domain]
  (or (-> database :domains (get domain))
      (thrift/domain-not-found-ex domain)))

(comment
  {:replication 1
   :port 3578
   :download-cap 1024
   :local-root "/Users/sritchie/Desktop/domainroot"
   :hosts ["localhost"]
   
   :hdfs-conf {"fs.default.name"
               "hdfs://hadoop-devel-nn.local.twitter.com:8020"}
   :blob-conf {"fs.default.name"
               "hdfs://hadoop-devel-nn.local.twitter.com:8020"}
   :domains {"graph" {:remote-store <remote-domain-store>
                      :local-store <local-domain-store>
                      :serializer <serializer>
                      :status     <status-atom>
                      :domain-data (atom {:version 123534534
                                          :shards {1 <persistence>
                                                   3 <persistence>}})
                      :shard-index 'shard-index}

             "docs" {:remote-store <remote-domain-store>
                     :local-store <local-domain-store>
                     :serializer <serializer>
                     :status     <status-atom>
                     :domain-data (atom {:version 123534534
                                         :shards {1 <persistence>
                                                  3 <persistence>}})
                     :shard-index 'shard-index}}})

;; ## Example Service Handler
;;
;; This logic is not really even particular to key value -- the only
;;thing we need to know is how to access a given persistence, which we
;;can pass around in Clojure with a function!

(defn trim-hosts
    "Used within a multi-get's loop. Accepts a sequence of hosts + a
    sequence of hosts known to be bad, filters the bad hosts and drops
    the first one."
    [host-seq bad-hosts]
    (remove (set bad-hosts)
            (rest host-seq)))

(defn index-keys
  "returns {:index}[hosts-to-try global-index key all-hosts] seq"
  [domain keys]
  (for [[idx key] (map-indexed vector keys)
        :let [hosts (fresh/prioritize-hosts domain key)]]
    {:index idx, :key key, :hosts hosts, :all-hosts hosts}))

(defn try-multi-get
  [service domain-name error-suffix key-seq]
    (let []
      (try (.directMultiGet service domain-name key-seq)
           (catch TException e
             (log/error e "Thrift exception on " suffix)) ;; try next host
           (catch WrongHostException e
             (log/error e "Fatal exception on " suffix)
             (throw (TException. "Fatal exception when performing get" e)))
           (catch DomainNotFoundException e
             (log/error e "Could not find domain when executing read on " suffix)
             (throw e))
           (catch DomainNotLoadedException e
             (log/error e "Domain not loaded when executing read on " suffix)
             (throw e)))))

;; Into this function comes a sequence of indexed-keys. Each
;; of these is a map with :index, :key and :host keys. On
;; success, it returns the indexed-keys input with :value keys
;; associated onto every map. On failure it throws an
;; exception, or returns nil.

(defn multi-get*
  [service domain-name local-hostname hostname port indexed-keys]
  (let [key-seq  (map :keys indexed-keys)
        suffix   (format "%s:%s/%s" hostname domain-name key-seq)
        multiget #(try-multi-get % domain-name suffix key-seq)]
    (when-let [vals (if (= local-hostname hostname)
                      (multiget service)
                      (with-elephant-connection hostname port remote-service
                        (multiget remote-service)))]
      (map (fn [m v] (assoc m :value v))
           indexed-keys
           vals))))

(defn service-handler
  "Entry point to edb. `service-handler` returns a proxied
  implementation of EDB's interface."
  [edb-config]
  (let [^ReentrantReadWriteLock rw-lock (u/mk-rw-lock)
        download-supervisor (atom nil)
        localhost (u/local-hostname)
        {domain-map :domains :as database} (build-database edb-config)]      
    (reify
      IPreparable
      (prepare [this]
        (with-ret true
          (future
            (purge-unused-domains! database)
            (doseq [domain (vals domains)]
              (boot-domain! domain rw-lock)))))
        
      IShutdownable
      (shutdown [_]
        (log/info "ElephantDB received shutdown notice...")
        (u/with-write-lock rw-lock
          (doseq [domain (vals domain-map)]
            (shutdown domain))))

      ElephantDB$Iface
      ;; IN PROGRESS
      (update [this domain]
        "Trigger an update on a single domain -- this means that the
          domain should look to its remote store and sync the latest
          version to itself, then update when complete.

         TODO: check what this currently returns.")

      ;; IN PROGRESS
      (updateAll [this]
        "Trigger updates on all domains.")

      (get [this domain key]
        (first (.multiGet this domain [key])))

      (getInt [this domain key]
        (.get this domain key))

      (getLong [this domain key]
        (.get this domain key))

      (getString [this domain key]
        (.get this domain key))

      ;; TODO: Wrap the return value in some fashion!
      (directMultiGet [_ domain-name keys]
        (u/with-read-lock rw-lock
          (let [domain (domain-get database domain-name)]
            (u/dofor [key keys, :let [shard (retrieve-shard domain key)]]
                     (log/debug
                      (format "Direct get: key %s at shard %s" key shard))
                     (if shard
                       (thrift/mk-value (.get shard key))
                       (throw (thrift/wrong-host-ex)))))))

      ;; Start out by indexing each key; this requires indexing each
      ;; key into a map (see `index-keys` above). The loop first
      ;; checks that every key has at least one host associated with
      ;; it. If any key is lacking hosts, multiGet throws an
      ;; exception for the entire multiGet.
      ;;
      ;; Assuming that doesn't happen, the system groups keys by the
      ;; first host in the list (localhost, if any keys are located
      ;; on the machine executing the call) and performs a
      ;; directMultiGet.
      ;;
      ;; If any host had unsuccessful results (didn't return
      ;; anything, basically), it's removed from the host lists of
      ;; every key for the subsequent loops.
      ;;
      ;; Once the multi-get loop completes without any failures the
      ;; entire sequence of keys is returned.
      (multiGet [this domain-name key-seq]
        (loop [indexed-keys (-> (domain-get database domain-name)
                                (index-keys key-seq))
               results []]
          (if-let [bad-key (some (comp empty? :hosts) indexed-keys)]
            (throw (thrift/hosts-down-ex (:all-hosts bad-key)))
            (let [host-map   (group-by (comp first :hosts) indexed-keys)
                  get-fn     (fn [host indexed-keys]
                               (multi-get* this domain-name
                                           localhost host
                                           port indexed-keys))
                  rets       (u/do-pmap (fn [[host indexed-keys]]
                                          [host (get-fn host indexed-keys)])
                                        host-map)
                  successful (into {} (filter second rets))
                  results    (->> (vals succeeded)
                                  (apply concat results))
                  fail-map   (apply dissoc host-map (keys succeeded))]
              (if (empty? failed-host-map)
                (map :value (sort-by :index results))
                (recur (map (fn [m]
                              (update-in m [:hosts] trim-hosts (keys fail-map)))
                            (apply concat (vals failed-host-map)))
                       results))))))
        
      (multiGetInt [this domain keys]
        (.multiGet this domain keys))

      (multiGetLong [this domain keys]
        (.multiGet this domain keys))

      (multiGetString [this domain keys]
        (.multiGet this domain keys))
        
      (getDomainStatus [_ domain-name]
        (stat/status (domain-get database domain-name)))

      (getDomains [_] (keys domain-map))

      (getStatus [_]
        (thrift/elephant-status
         (u/val-map stat/status domain-map)))

      (isFullyLoaded [this]
        "Are all domains loaded properly?"
        (every? (some-fn stat/ready? stat/failed?)
                (vals domain-map)))

      (isUpdating [this]
        "Is some domain currently updating?"
        (let [domains (vals domain-map)]
          (some stat/loading?
                (map stat/status domains)))))))
