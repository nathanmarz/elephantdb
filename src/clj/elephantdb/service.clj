(ns elephantdb.service
  (:use [elephantdb config log util hadoop loader]
        [clojure.contrib.def :only (defnk)])
  (:require [clojure.string :as s]
            [elephantdb.domain :as domain]
            [elephantdb.thrift :as thrift]
            [elephantdb.shard :as shard])
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
  [fs global-config]
  (let [domain-shards (shard/shard-domains fs global-config)]
    (update-vals (:domains global-config)
                 (fn [k _]
                   (-> k domain-shards domain/init-domain-info)))))

(defn load-and-sync-domain-status
  [domain domain-info loader-fn rw-lock update?]
  (future
    (try (domain/set-domain-status! domain-info
                                    (if update?
                                      (thrift/ready-status true)
                                      (thrift/loading-status)))
         (let [new-data (loader-fn domain)]  
           (when (and new-data (not= new-data :no-update))
             (domain/set-domain-data! rw-lock domain domain-info new-data))
           (domain/set-domain-status! domain-info
                                      (thrift/ready-status false)))
         (catch Throwable t
           (log-error t "Error when loading domain " domain)
           (domain/set-domain-status! domain-info
                                      (thrift/failed-status t))))))

(defnk load-and-sync-status
  [domains-info rw-lock loader-fn :update? false]
  (let [loaders (dofor [[domain info] domains-info]
                       (load-and-sync-domain-status domain
                                                    info
                                                    loader-fn
                                                    rw-lock
                                                    update?))]
    (with-ret (future-values loaders)
      (log-message "Successfully loaded domains: "
                   (s/join ", " (keys domains-info))))))

(defn domain-needs-update? [local-vs remote-vs]
  (or (nil? (.mostRecentVersion local-vs))
      (< (.mostRecentVersion local-vs)
         (.mostRecentVersion remote-vs))))

;; TODO: examine this... can we remove this use-cache deal?
(defn use-cache-or-update
  ([domain domains-info global-config local-config]
     (->> {domain (domain/host-shards (domains-info domain))}
          (mk-loader-state)
          (use-cache-or-update domain domains-info global-config local-config false)))
  ([domain domains-info global-config local-config update? ^DownloadState state]
     (let [fs (filesystem (:hdfs-conf local-config))
           lfs (local-filesystem)
           local-dir (:local-dir local-config)
           local-domain-root (str (path local-dir domain))
           remote-path (-> global-config :domains (get domain))
           remote-vs (DomainStore. fs remote-path)
           local-vs  (DomainStore. lfs local-domain-root (.getSpec remote-vs))]
       (if (domain-needs-update? local-vs remote-vs)
         (load-domain domain
                      fs
                      local-config
                      local-domain-root
                      remote-path
                      (domain/host-shards (domains-info domain))
                      state)
         ;; use cached domain from local-dir (no update needed)
         (do
           ;; signal all shards of domain are done loading
           (swap! (:finished-loaders state) + (count (get (:shard-states state) domain)))
           (if update?
             :no-update
             (open-domain local-config
                          (str (path local-dir domain))
                          (domain/host-shards (domains-info domain)))))))))

(defn purge-unused-domains!
  "Walks through the supplied local directory, recursively deleting
  all directories with names that aren't present in the supplied
  `domain-set`."
  [domain-set local-dir]
  (let [lfs (local-filesystem)]
    (dofor [domain-path (-> local-dir mk-local-path .listFiles)
            :when (and (.isDirectory domain-path)
                       (not (contains? domain-set (.getName domain-path))))]
           (log-message "Deleting local path of deleted domain: " domain-path)
           (delete lfs (.getPath domain-path) true))))

;; TODO: take this out of the loop.
(defn sync-remote-data
  [domains-info global-config local-config rw-lock]
  (load-and-sync-status domains-info
                        rw-lock
                        (fn [domain]
                          (use-cache-or-update domain
                                               domains-info
                                               global-config
                                               local-config))))

(defn cleanup-domain!
  [domain-path]
  "Destroys all but the most recent version in the versioned store
  located at `domain-path`."
  (doto (DomainStore. (local-filesystem) domain-path)
    (.cleanup 1)))

(defn cleanup-domains!
  "Destroys every old version for each domain in `domains` located
  inside of `local-dir`. (Each domain directory contains a versioned
  store, capable of being 'cleaned up' in this fashion.)

  If any cleanup operation throws an error, `cleanup-domains!` will
  try to operate on the rest of the domains, and throw a single error
  on completion."
  [domains local-dir]
  (let [error (atom nil)]
    (dofor [domain domains :let [domain-path (str local-dir domain)]]
           (try (log-message "Purging old versions of domain: " domain)
                (cleanup-domain! domain-path)
                (catch Throwable t
                  (log-error t "Error when purging old versions of domain: " domain)
                  (reset! error t))))
    (when-let [e @error]
      (throw e))))

(defn sync-updated!
  [global-config local-config rw-lock]
  (let [{:keys [hdfs-conf local-dir]} local-config
        domains-info (-> (filesystem hdfs-conf)
                         (init-domain-info-map global-config))
        domains (keys domains-info)]
    (log-message "Domains info: " domains-info)
    (future
      (try (purge-unused-domains! (set domains) local-dir)
           (sync-remote-data domains-info global-config local-config rw-lock)
           (cleanup-domains! domains local-dir)
           (log-message "Finished loading all updated domains from remote")
           (catch Throwable t
             (log-error t "Error when syncing data.")
             (throw t))))
    domains-info))

(defn- close-lps
  [domains-info]
  (doseq [[domain info] domains-info]
    (doseq [shard (domain/host-shards info)]
      (let [lp (domain/domain-data info shard)]
        (log-message "Closing LP for " domain "/" shard)
        (if lp
          (.close lp)
          (log-message "LP not loaded"))
        (log-message "Closed LP for " domain "/" shard)))))

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
   (or (some (fn [[domain status]]
               (thrift/status-loading? status))
             (-> service-handler .getStatus .get_domain_statuses))
       (and @download-supervisor
            (not (.isDone @download-supervisor))))))

(defn- update-domains
  [service-handler download-supervisor all-domains domains-info global-config local-config rw-lock]
  (if (service-updating? service-handler download-supervisor)
    (log-message "UPDATER - Not updating as update process still in progress.")
    (let [max-kbs (:max-online-download-rate-kb-s local-config)
          all-shards (domain/all-shards domains-info)
          ^DownloadState state (->> all-domains
                                    (map (juxt identity all-shards))
                                    (into {})
                                    (mk-loader-state))
          shard-amount (flattened-count (vals (:shard-states state)))]
      (log-message "UPDATER - Updating domains: " (s/join ", " all-domains))
      (reset! download-supervisor
              (start-download-supervisor shard-amount max-kbs state))
      (future
        (try (load-and-sync-status (select-keys domains-info all-domains)
                                   rw-lock
                                   (fn [domain]
                                     (use-cache-or-update domain
                                                          domains-info
                                                          global-config
                                                          local-config
                                                          true
                                                          state))
                                   :update? true)
             (log-message "Finished updating all domains from remote")

             ;; Cleaning up old versions seems to happen after each
             ;; load. TODO: Move this shit together.
             (try (log-message "Removing all old versions of domains (CLEANUP)")
                  (cleanup-domains! (keys domains-info)
                                    (:local-dir local-config))
                  (catch Throwable t (log-error t "Error when cleaning old versions")))
             (catch Throwable t
               (log-error t "Error when syncing data")
               (throw t)))))))

;; TODO: Update this to remove  token references.
;;
;; TODO: Take a purge flag, that'll trigger a data wipe.
;;
;; 5. (What does this mean?) Create Hadoop FS on demand... need retry
;; logic if loaders fail?

;;  Example of domains-info local:
;;
;; {test {:elephantdb.domain/shard-index {:elephantdb.shard/hosts-to-shards {192.168.1.3 #{0 1 2 3}}
;;                                        :elephantdb.shard/shards-to-hosts {3 #{192.168.1.3}
;;                                                                           2 #{192.168.1.3}
;;                                                                           1 #{192.168.1.3}
;;                                                                           0 #{192.168.1.3}}}
;;        :elephantdb.domain/domain-status #<Atom@3643b5bb: #<DomainStatus <DomainStatus loading:LoadingStatus()>>>
;;        :elephantdb.domain/domain-data #<Atom@4feaefc5: nil>}}

(defn edb-proxy
  "See `src/elephantdb.thrift` for more information on the methods we
  implement."
  [client global-config local-config]
  (let [^ReentrantReadWriteLock rw-lock (mk-rw-lock)
        domains-info (sync-updated! global-config local-config rw-lock)
        download-supervisor (atom nil)]
    (proxy [ElephantDB$Iface Shutdownable] []
      (shutdown []
        (log-message "ElephantDB received shutdown notice...")
        (with-write-lock rw-lock
          (dofor [[_ info] domains-info]
                 (domain/set-domain-status! info (thrift/shutdown-status))))
        (close-lps domains-info))

      (get [^String domain ^bytes key]
        (.get @client domain key))

      (getString [^String domain ^String key]
        (.getString @client domain key))

      (getInt [^String domain key]
        (.getInt @client domain key))

      (getLong [^String domain key]
        (.getLong @client domain key))

      (multiGet [^String domain keys]
        (.multiGet @client domain keys))

      (multiGetString [^String domain ^String keys]
        (.multiGetString @client domain keys))

      (multiGetInt [^String domain keys]
        (.multiGetInt @client domain keys))
      
      (multiGetLong [^String domain keys]
        (.multiGetLong @client domain keys))

      (directMultiGet [^String domain keys]
        (with-read-lock rw-lock
          (dofor [key keys]
                 (let [info (get-readable-domain-info domains-info domain)
                       shard (domain/key-shard domain info key)
                       ^LocalPersistence lp (domain/domain-data info shard)]
                   (log-debug "Direct get key " (seq key) "at shard " shard)
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
         (map-mapvals domains-info domain/domain-status)))

      (isFullyLoaded []
        (let [stat (.get_domain_statuses (.getStatus this))]
          (every? #(or (thrift/status-ready? %)
                       (thrift/status-failed? %))
                  (vals stat))))

      (isUpdating []
        (service-updating? this download-supervisor))

      (updateAll []
        (with-ret true
          (update-domains this
                          download-supervisor
                          (keys domains-info)
                          domains-info
                          global-config
                          local-config
                          rw-lock)))
      
      (update [^String domain]
        (with-ret true
          (update-domains this
                          download-supervisor
                          [domain]
                          domains-info
                          global-config
                          local-config
                          rw-lock))))))

(defn service-handler
  "Entry point to edb. `service-handler` returns a proxied
  implementation of EDB's interface."
  [global-config local-config]
  (let [client (atom nil)]
    (with-ret-bound [ret (edb-proxy client global-config local-config)]
      (reset! client (client. ret
                              (:hdfs-conf local-config)
                              global-config)))))

(defn thrift-server
  [service-handler port]
  (let [options (THsHaServer$Options.)
        _ (set! (.maxWorkerThreads options) 64)]
    (THsHaServer. (ElephantDB$Processor. service-handler)
                  (TNonblockingServerSocket. port)
                  (TBinaryProtocol$Factory.)
                  options)))
