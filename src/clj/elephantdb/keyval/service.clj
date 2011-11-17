(ns elephantdb.keyval.service
  (:use elephantdb.common.hadoop
        elephantdb.common.util
        hadoop-util.core
        elephantdb.keyval.config
        elephantdb.keyval.loader)
  (:require [clojure.string :as s]
            [elephantdb.keyval.domain :as domain]
            [elephantdb.keyval.thrift :as thrift]
            [elephantdb.common.shard :as shard]
            [elephantdb.common.log :as log])
  (:import [java.io File]
           [org.apache.thrift.server THsHaServer THsHaServer$Options]
           [org.apache.thrift.protocol TBinaryProtocol$Factory]
           [org.apache.thrift.transport TNonblockingServerSocket]
           [java.util.concurrent.locks ReentrantReadWriteLock]
           [elephantdb.generated ElephantDB ElephantDB$Iface ElephantDB$Processor]
           [elephantdb Shutdownable client]
           [elephantdb.persistence LocalPersistence]
           [elephantdb.store DomainStore]))

(def ^{:doc "Example, meant to be ignored."}
  example-global-conf
  {:replication 1
   :port 3578
   :hosts ["localhost"]
   :domains {"graph" "/mybucket/elephantdb/graph"
             "docs"  "/data/docdb"}})

(defn- init-domain-info-map
  " Generates a map with kv pairs of the following form:
    {\"domain-name\" {:shard-index {:hosts-to-shards <map>
                                    :shards-to-hosts <map>}
                      :domain-status <status-atom>
                      :domain-data <data-atom>}}"
  [fs {:keys [domains hosts replication]}]
  (let [domain-shards (shard/shard-domains fs domains hosts replication)]
    (update-vals (fn [k _]
                   (-> k domain-shards domain/init-domain-info))
                 domains)))

(defn domain-has-data? [local-vs]
  (-> local-vs .mostRecentVersion boolean))

(defn domain-needs-update?
  "Returns true if the remote VersionedStore contains newer data than
  its local copy, false otherwise."
  [local-vs remote-vs]
  (or (not (domain-has-data? local-vs))
      (let [local-version  (.mostRecentVersion local-vs)
            remote-version (.mostRecentVersion remote-vs)]
        (when (and local-version remote-version)
          (< local-version remote-version)))))

(defn mk-local-vs
  [remote-vs local-path]
  (DomainStore. (local-filesystem)
                local-path
                (.getSpec remote-vs)))

(defn try-domain-store
  "Attempts to return a DomainStore object from the current path and
  filesystem; if this doesn't exist, returns nil."  [fs domain-path]
  (try (DomainStore. fs domain-path)
       (catch IllegalArgumentException _)))

(defn cleanup-domain!
  [domain-path]
  "Destroys all but the most recent version in the versioned store
  located at `domain-path`."
  (when-let [store (try-domain-store (local-filesystem)
                                     domain-path)]
    (doto store (.cleanup 1))))

(defn cleanup-domains!
  "Destroys every old version for each domain in `domains` located
  inside of `local-dir`. (Each domain directory contains a versioned
  store, capable of being 'cleaned up' in this fashion.)

  If any cleanup operation throws an error, `cleanup-domains!` will
  try to operate on the rest of the domains, and throw a single error
  on completion."
  [domains local-dir]
  (let [error (atom nil)]
    (dofor [domain domains :let [domain-path (str (path local-dir domain))]]
           (try (log/info "Purging old versions of domain: " domain)
                (cleanup-domain! domain-path)
                (catch Throwable t
                  (log/error t "Error when purging old versions of domain: " domain)
                  (reset! error t))))
    (when-let [e @error]
      (throw e))))

(defn try-thrift
  "Applies each kv pair to the supplied `func` in parallel, farming
  the work out to futures; each domain is set to `initial-status` at
  the beginning of the func, and `thrift/ready-status` on successful
  completion. (Failures are marked as appropriate.)

  After completing all functions, we remove all versions of each
  domain but the last."
  [domains-info local-dir initial-status func]
  (with-ret (p-dofor [[domain info] domains-info]
                     (try (domain/set-domain-status! info initial-status)
                          (func domain info)
                          (domain/set-domain-status! info (thrift/ready-status))
                          (catch Throwable t
                            (log/error t "Error when loading domain " domain)
                            (domain/set-domain-status! info
                                                       (thrift/failed-status t)))))
    (try (log/info "Removing all old versions of updated domains!")
         (cleanup-domains! (keys domains-info) local-dir)
         (catch Throwable t
           (log/error t "Error when cleaning old versions.")))))

;; TODO: Test this deal with the finished loaders, etc.
(defn update-and-sync-status!
  "Triggers throttled update routine for all domains keyed in
  `domains-info`. Once these complete, atomically swaps them into for
  the current data and registers success."
  [edb-config rw-lock domains-info loader-state]
  (let [{:keys [hdfs-conf local-dir local-db-conf]} edb-config
        remote-path-map (:domains edb-config)
        remote-fs (filesystem hdfs-conf)
        
        ;; Do we throttle? TODO: Use this arg.
        throttle? (->> (vals domains-info)
                       (map domain/domain-status)
                       (some thrift/status-ready?))]
    (try-thrift domains-info
                local-dir
                (thrift/ready-status :loading? true)
                (fn [domain info]
                  (let [remote-path (get remote-path-map domain)
                        remote-vs (try-domain-store remote-fs remote-path)]
                    (when remote-vs
                      (let [local-domain-root (str (path local-dir domain))
                            local-vs (mk-local-vs remote-vs local-domain-root)]
                        (if (domain-needs-update? local-vs remote-vs)
                          (->> (load-domain domain
                                            remote-fs
                                            local-db-conf
                                            local-domain-root
                                            remote-path
                                            loader-state)
                               (domain/set-domain-data! rw-lock domain info)
                               (log/info "Finished loading all updated domains from remote."))
                          (let [{:keys [finished-loaders shard-states]} loader-state]
                            (swap! finished-loaders + (count (shard-states domain))))))))))))

(defn load-cached-domains!
  [edb-config rw-lock domains-info]
  (let [{:keys [hdfs-conf local-dir local-db-conf]} edb-config
        remote-path-map (:domains edb-config)
        remote-fs (filesystem hdfs-conf)]
    (try-thrift domains-info
                local-dir
                (thrift/loading-status)
                (fn [domain info]
                  (let [remote-path  (get remote-path-map domain)
                        remote-vs (DomainStore. remote-fs remote-path)
                        local-path (str (path local-dir domain))
                        local-vs (mk-local-vs remote-vs local-path)]  
                    (when-let [new-data (and (domain-has-data? local-vs)
                                             (open-domain local-db-conf
                                                          local-path
                                                          (domain/host-shards info)))]
                      (domain/set-domain-data! rw-lock domain info new-data)))))))

(defn purge-unused-domains!
  "Walks through the supplied local directory, recursively deleting
  all directories with names that aren't present in the supplied
  `domains`."
  [domain-seq local-dir]
  (let [lfs (local-filesystem)
        domain-set (set domain-seq)]
    (dofor [domain-path (-> local-dir mk-local-path .listFiles)
            :when (and (.isDirectory domain-path)
                       (not (contains? domain-set
                                       (.getName domain-path))))]
           (log/info "Deleting local path of deleted domain: " domain-path)
           (delete lfs (.getPath domain-path) true))))

(defn prepare-local-domains!
  "Wipe domains not being used, make ready all cached domains, and get
  the downloading process started for all others."
  [domains-info edb-config rw-lock]
  (let [{:keys [local-dir]} edb-config
        domain-seq (keys domains-info)]
    (with-ret domains-info
      (future        
        (purge-unused-domains! domain-seq local-dir)
        (load-cached-domains! edb-config rw-lock domains-info)
        (update-and-sync-status! edb-config
                                 rw-lock
                                 domains-info
                                 (->> domains-info
                                      (val-map domain/host-shards)
                                      (mk-loader-state)))))))

(defn- close-lps
  [domains-info]
  (doseq [[domain info] domains-info
          shard (domain/host-shards info)]
    (let [lp (domain/domain-data info shard)]
      (log/info "Closing LP for " domain "/" shard)
      (if lp
        (do (.close lp)
            (log/info "Closed LP for " domain "/" shard))
        (log/info "LP not loaded for " domain "/" shard)))))
 
(defn- get-readable-domain-info [domains-info domain]
  (let [info (domains-info domain)]
    (when-not info
      (throw (thrift/domain-not-found-ex domain)))
    (when-not (thrift/status-ready? (domain/domain-status info))
      (throw (thrift/domain-not-loaded-ex domain)))
    info))

(defn service-updating?
  [service-handler download-supervisor]
  (boolean
   (or (some thrift/status-loading?
             (-> service-handler .getStatus .get_domain_statuses vals))
       (and @download-supervisor
            (not (.isDone @download-supervisor))))))

(defn- update-domains
  [download-supervisor domains-info edb-config rw-lock]
  (let [{max-kbs :max-online-download-rate-kb-s
         local-dir :local-dir} edb-config
         download-state (-> domains-info
                            (domain/all-shards)
                            (mk-loader-state))
         shard-amount (flattened-count (vals (:shard-states download-state)))]
    (log/info "UPDATER - Updating domains: " (s/join ", " (keys domains-info)))
    (reset! download-supervisor (start-download-supervisor shard-amount max-kbs download-state))
    (future (update-and-sync-status! edb-config
                                     rw-lock
                                     domains-info
                                     download-state))))

(defn- trigger-update
  [service-handler download-supervisor domains-info edb-config rw-lock]
  (with-ret true
    (if (service-updating? service-handler download-supervisor)
      (log/info "UPDATER - Not updating as update process still in progress.")
      (update-domains download-supervisor domains-info edb-config rw-lock))))

;; 5. TODO: (What does this mean?) Create Hadoop FS on demand... need
;; retry logic if loaders fail?

;; When we first start up the service, we trigger an unthrottled
;; download. It'd be good if we had access to the download supervisor
;; inside of prepare-local-domain, and could decide whether or not to
;; throttle the thing.

(defn edb-proxy
  "See `src/elephantdb.thrift` for more information on the methods we
  implement."
  [client {:keys [hdfs-conf local-dir] :as edb-config}]
  (let [^ReentrantReadWriteLock rw-lock (mk-rw-lock)
        domains-info (-> (filesystem hdfs-conf)
                         (init-domain-info-map edb-config))
        download-supervisor (atom nil)]
    (prepare-local-domains! domains-info edb-config rw-lock)
    (proxy [ElephantDB$Iface Shutdownable] []
      (shutdown []
        (log/info "ElephantDB received shutdown notice...")
        (with-write-lock rw-lock
          (dofor [[_ info] domains-info]
                 (domain/set-domain-status! info (thrift/shutdown-status))))
        (close-lps domains-info))
      
      (get [^String domain key]
        (.get @client domain key))

      (multiGet [^String domain keys]
        (.multiGet @client domain keys))

      (directMultiGet [^String domain keys]
        (with-read-lock rw-lock
          (let [info (get-readable-domain-info domains-info domain)]
            (dofor [key keys
                    :let [shard (domain/key-shard domain info key)
                          ^LocalPersistence lp (domain/domain-data info shard)]]
                   (log/debug "Direct get key " (seq key) "at shard " shard)
                   (if lp
                     (thrift/mk-value (.get lp key))
                     (throw (thrift/wrong-host-ex)))))))
      
      (getDomainStatus [^String domain]
        (let [info (domains-info domain)]
          (when-not info
            (throw (thrift/domain-not-found-ex domain)))
          (domain/domain-status info)))

      (getDomains []
        (keys domains-info))
      
      (getStatus []
        (thrift/elephant-status
         (val-map domain/domain-status domains-info)))

      (isFullyLoaded []
        (let [stat (.get_domain_statuses (.getStatus this))]
          (every? #(or (thrift/status-ready? %)
                       (thrift/status-failed? %))
                  (vals stat))))

      (isUpdating []
        (service-updating? this download-supervisor))
      
      (updateAll []
        (trigger-update this
                        download-supervisor
                        domains-info
                        edb-config
                        rw-lock))
      
      (update [^String domain]
        (trigger-update this
                        download-supervisor
                        (select-keys domains-info [domain])
                        edb-config
                        rw-lock)))))

(defn service-handler
  "Entry point to edb. `service-handler` returns a proxied
  implementation of EDB's interface."
  [global-config local-config]
  (let [client     (atom nil)
        edb-config (merge global-config local-config)]
    (with-ret-bound [ret (edb-proxy client edb-config)]
      (reset! client (client. (:hdfs-conf local-config)
                              global-config
                              ret)))))

(defn thrift-server
  [service-handler port]
  (let [options (THsHaServer$Options.)
        _ (set! (.maxWorkerThreads options) 64)]
    (THsHaServer. (ElephantDB$Processor. service-handler)
                  (TNonblockingServerSocket. port)
                  (TBinaryProtocol$Factory.)
                  options)))

(defn launch-updater!
  "Returns a future."
  [interval-secs ^ElephantDB$Iface service-handler]
  (let [interval-ms (* 1000 interval-secs)]
    (future
      (log/info (format "Starting updater process with an interval of: %s seconds..."
                           interval-secs))
      (while true
        (Thread/sleep interval-ms)
        (log/info "Updater process: Checking if update is possible...")
        (.updateAll service-handler)))))
