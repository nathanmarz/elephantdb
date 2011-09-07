(ns elephantdb.service
  (:use [elephantdb config log util hadoop loader])
  (:require [elephantdb.domain :as domain]
            [elephantdb.thrift :as thrift]
            [elephantdb.shard :as shard]
            [clojure.contrib.str-utils :as str-utils])
  (:import [java.util.concurrent.locks ReentrantReadWriteLock]
           [elephantdb.generated ElephantDB ElephantDB$Iface]
           [elephantdb Shutdownable client]
           [elephantdb.persistence LocalPersistence]
           [elephantdb.store DomainStore]))

;; { :replication 2
;;   :hosts ["elephant1.server" "elephant2.server" "elephant3.server"]
;;   :port 3578
;;   :domains {"graph" "s3n://mybucket/elephantdb/graph"
;;             "docs"  "/data/docdb"
;;             }
;; }

(defn- init-domain-info-map [fs global-config]
  (log-message "Global config: " global-config)
  (let [domain-shards (shard/shard-domains fs global-config)]
    (->> (dofor [domain (keys (:domains global-config))]
                [domain (domain/init-domain-info (domain-shards domain))])
         (into {}))))

(defn load-and-sync-status-of-domain
  [domain domains-info update? loader-fn]
  (future
    (try (domain/set-domain-status! (domains-info domain)
                                    (if update?
                                      (thrift/ready-status true)
                                      (thrift/loading-status)))
         ;; BUG: loader-fn is
         ;; (fn [domain]
         ;; (close-if-updated domain
         ;;                   domains-info
         ;;                   local-config
         ;;                   (use-cache-or-update domain domains-info global-config local-config true state)))
         ;;
         ;; Actually, we should be checking the same condition as
         ;; close-if-updated, and swapping first.
         
         (let [domain-data (loader-fn domain)]  
           ;; Here, we finally do the swap.
           (when domain-data
             (domain/set-domain-data! (domains-info domain)
                                      domain-data))
           (domain/set-domain-status!
            (domains-info domain)
            (thrift/ready-status false)))
         (catch Throwable t
           (log-error t "Error when loading domain " domain)
           (domain/set-domain-status!
            (domains-info domain)
            (thrift/failed-status t))))))

(defn load-and-sync-status
  ([domains-info update? loader-fn]
     (load-and-sync-status (keys domains-info)
                           domains-info
                           update?
                           loader-fn))
  ([domains domains-info update? loader-fn]
     (let [loaders (dofor [domain domains]
                          (load-and-sync-status-of-domain domain
                                                          domains-info
                                                          update?
                                                          loader-fn))]
       (with-ret (future-values loaders)
         (log-message "Successfully loaded domains: "
                      (str-utils/str-join ", " domains))))))

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
           (swap! (:finished-loaders state) + (count ((:shard-states state) domain)))
           (if update?
             :no-update
             (open-domain local-config
                          (str (path local-dir domain))
                          (domain/host-shards (domains-info domain)))))))))

(defn delete-deleted-domains
  "Deletes all domains from local filesystem that have been deleted
  from the global config."
  [domains-info global-config local-config]
  (let [lfs (local-filesystem)
        local-dir (:local-dir local-config)
        local-path (.pathToFile lfs (path local-dir))]
    (doseq [domain-path (.listFiles local-path)]
      (when (and (.isDirectory domain-path)
                 (not (domains-info (last (.split (.getPath domain-path) "/")))))
        (log-message "Deleting local path of deleted domain: " domain-path)
        (delete lfs (.getPath domain-path) true)))))

(defn sync-data-updated
  [domains-info global-config local-config]
  (load-and-sync-status domains-info
                        false
                        (fn [domain]
                          (use-cache-or-update domain domains-info global-config local-config))))

(defn cleanup-domain
  [domain domains-info local-config]
  (let [lfs (local-filesystem)
        local-dir (:local-dir local-config)]
    (let [local-domain-root (str (path local-dir domain))
          local-vs (DomainStore. lfs local-domain-root)]
      (log-message "Cleaning up local domain versions (only keeping latest version) for domain: "
                   domain)
      (.cleanup local-vs 1))))

(defn cleanup-domains
  [domains-info local-config]
  (let [error (atom nil)]
    (doseq [domain (keys domains-info)]
      (try (cleanup-domain domain domains-info local-config)
           (catch Throwable t
             (log-error t "Error when cleaning old versions of domain: " domain)
             (reset! error t))))
    (when-let [e @error]
      (throw e))))

(defn sync-updated
  "Only fetch domains from remote if a newer version is available.
Keep the cached versions of any domains that haven't been updated"
  [global-config local-config]
  (log-message "Loading remote data if necessary, otherwise loading cached data")
  (let [fs (filesystem (:hdfs-conf local-config))
        domains-info (init-domain-info-map fs global-config)]
    (log-message "Domains info: " domains-info)
    (future
      (try (delete-deleted-domains domains-info global-config local-config)
           (sync-data-updated domains-info global-config local-config)
           (cleanup-domains domains-info local-config)
           (log-message "Caching global config " global-config)
           (cache-global-config! local-config global-config)
           (log-message "Cached config " (read-cached-global-config local-config))
           (log-message "Finished loading all updated domains from remote")
           (catch Throwable t (log-error t "Error when syncing data") (throw t))))
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

(defn- close-if-updated [domain domains-info local-config new-data]
  (when (not= new-data :no-update)
    (when new-data
      (close-domain domain domains-info))
    new-data))

(defn service-updating? [service-handler download-supervisor]
  (boolean
   (or (some (fn [[domain status]]
               (thrift/status-loading? status))
             (.get_domain_statuses (.getStatus service-handler)))
       (and @download-supervisor
            (not (.isDone @download-supervisor))))))

(defn- update-domains
  [service-handler download-supervisor all-domains domains-info global-config local-config]
  (if (service-updating? service-handler
                         download-supervisor)
    (log-message "UPDATER - Not updating as update process still in progress.")
    (let [max-kbs (:max-online-download-rate-kb-s local-config)
          all-shards (domain/all-shards domains-info)
          domain-shards (->> all-domains
                             (map (fn [domain]
                                    [domain (all-shards domain)]))
                             (into {}))
          shard-amount (->> (vals all-shards)
                            (map #(count %))
                            (reduce + 0))]
      (log-message "UPDATER - Updating domains: " (str-utils/str-join ", " all-domains))
      (let [^DownloadState state (mk-loader-state domain-shards)
            shard-amount (reduce + 0 (map #(count %) (vals (:shard-states state))))]
        (reset! download-supervisor (start-download-supervisor shard-amount max-kbs state))
        (future
          (try (load-and-sync-status
                all-domains domains-info
                true
                
                ;; Update this to prevent that whole close-if-updated business.
                (fn [domain]
                  (->> (use-cache-or-update domain
                                            domains-info
                                            global-config
                                            local-config
                                            true
                                            state)
                       (close-if-updated domain
                                         domains-info
                                         local-config))))
               (log-message "Finished updating all domains from remote")
               (try (log-message "Removing all old versions of domains (CLEANUP)")
                    (cleanup-domains domains-info local-config)
                    (catch Throwable t (log-error t "Error when cleaning old versions")))
               (catch Throwable t
                 (log-error t "Error when syncing data")
                 (throw t))))))))

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
        domains-info (sync-updated global-config local-config)
        download-supervisor (atom nil)]
    (proxy [ElephantDB$Iface Shutdownable] []
      (shutdown []
        (log-message "ElephantDB received shutdown notice...")
        (write-locked
         rw-lock
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
        (read-locked rw-lock
                     (dofor [key keys]
                            (let [info                   (get-readable-domain-info domains-info domain)
                                  shard                  (domain/key-shard domain info key)
                                  ^LocalPersistence lp  (domain/domain-data info shard)]
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
         (into {} (for [[d i] domains-info]
                    [d (domain/domain-status i)]))))

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
                          local-config)))

      (update [^String domain]
        (with-ret true
          (update-domains this
                          download-supervisor
                          [domain]
                          domains-info
                          global-config
                          local-config))))))

;; `service-handler` is the entry point to elephantdb, initialized in
;; `main` and passed into the thrift server.
(defn service-handler
  [global-config local-config]
  (let [client (atom nil)]
    (with-ret-bound [ret (edb-proxy client global-config local-config)]
      (reset! client (client. ret
                              (:hdfs-conf local-config)
                              global-config)))))
