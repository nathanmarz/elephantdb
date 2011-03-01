(ns elephantdb.service
  (:import [java.util.concurrent.locks ReentrantReadWriteLock])
  (:import [elephantdb.generated ElephantDB ElephantDB$Iface])
  (:import [elephantdb Shutdownable client])
  (:import [elephantdb.persistence LocalPersistence])
  (:import [elephantdb.store DomainStore])
  (:require [elephantdb [domain :as domain] [thrift :as thrift] [shard :as shard]])
  (:use [elephantdb config log util hadoop loader]))

; { :replication 2
;   :hosts ["elephant1.server" "elephant2.server" "elephant3.server"]
;   :port 3578
;   :domains {"graph" "s3n://mybucket/elephantdb/graph"
;             "docs"  "/data/docdb"
;             }
; }

(defn- init-domain-info-map [fs global-config]
  (let [domain-shards (shard/shard-domains fs global-config)]
    (into {}
      (dofor [domain (keys (:domains global-config))]
          [domain (domain/init-domain-info (domain-shards domain))]
            ))))

(defn load-and-sync-status [domains-info loader-fn]
  (let [loaders (dofor [domain (keys domains-info)]
                       (future
                         (try
                           (let [domain-data (loader-fn domain)]
                             (if domain-data
                               (domain/set-domain-data! (domains-info domain)
                                                        domain-data))
                             (domain/set-domain-status!
                              (domains-info domain)
                              (thrift/ready-status false)))
                           (catch Throwable t
                             (log-error t "Error when loading domain " domain)
                             (domain/set-domain-status!
                              (domains-info domain)
                              (thrift/failed-status t))
                             ))))]
    (with-ret (future-values loaders)
      (log-message "Successfully loaded all domains"))
    ))

(defn domain-needs-update? [local-vs remote-vs]
  (or (nil? (.mostRecentVersion local-vs))
      (< (.mostRecentVersion local-vs)
         (.mostRecentVersion remote-vs))))

(defn use-cache-or-update
  ([domain domains-info global-config local-config open-if-no-update]
     (let [fs (filesystem (:hdfs-conf local-config))
           lfs (local-filesystem)
           local-dir (:local-dir local-config)
           local-domain-root (str (path local-dir domain))
           remote-path (-> global-config :domains (get domain))
           remote-vs (DomainStore. fs remote-path)
           local-vs (DomainStore. lfs local-domain-root (.getSpec remote-vs))]
       (if (domain-needs-update? local-vs remote-vs)
         (load-domain fs
                      local-config
                      local-domain-root
                      remote-path
                      (domain/host-shards (domains-info domain)))
         ;; use cached domain from local-dir (no update needed)
         (do
           (swap! finished-loaders + 1)
           (if open-if-no-update
             (open-domain local-config
                          (str (path local-dir domain))
                          (domain/host-shards (domains-info domain))))))))
  ([domain domains-info global-config local-config]
     (use-cache-or-update domain domains-info global-config local-config true)))

(defn delete-deleted-domains
  "Deletes all domains from local filesystem that have been deleted from the global config."
  [domains-info global-config local-config]
  (let [lfs (local-filesystem)
        local-dir (:local-dir local-config)
        local-path (.pathToFile lfs (path local-dir))]
    (doseq [domain-path (.listFiles local-path)]
      (when (and (.isDirectory domain-path)
                 (not (domains-info (last (.split (.getPath domain-path) "/")))))
        (log-message "Deleting local path of deleted domain: " domain-path)
        (delete lfs (.getPath domain-path) true)))))

(defn sync-data-updated [domains-info global-config local-config]
  (load-and-sync-status domains-info
                        (fn [domain]
                          (use-cache-or-update domain domains-info global-config local-config))))

(defn cleanup-domains [fs domains-info global-config local-config]
  (let [lfs (local-filesystem)
        local-dir (:local-dir local-config)]
    (doseq [domain (keys domains-info)]
      (let [local-domain-root (str (path local-dir domain))
            local-vs (DomainStore. lfs local-domain-root)]
        (log-message "Cleaning up local domain versions (only keeping latest version) for domain: " domain)
        (.cleanup local-vs 1)))))

(defn sync-updated
  "Only fetch domains from remote if a newer version is available.
Keep the cached versions of any domains that haven't been updated"
  [global-config local-config token]
  (log-message "Loading remote data if necessary, otherwise loading cached data")
  (let [fs (filesystem (:hdfs-conf local-config))
        domains-info (init-domain-info-map fs global-config)
        cache-config (assoc global-config :token token)]
    (log-message "Domains info: " domains-info)
    (future
      (try
        (delete-deleted-domains domains-info global-config local-config)
        (sync-data-updated domains-info global-config local-config)
        (cleanup-domains fs domains-info global-config local-config)
        (log-message "Caching global config " cache-config)
        (cache-global-config! local-config cache-config)
        (log-message "Cached config " (read-cached-global-config local-config))
        (log-message "Finished loading all updated domains from remote")
        (catch Throwable t (log-error t "Error when syncing data") (throw t))))
    domains-info))

(defn sync-local [global-config local-config]
  (log-message "Loading cached data")
  (let [lfs (local-filesystem)
        local-dir (:local-dir local-config)
        domains-map (into {}
                          (map (fn [[k v]]
                                 [k (str-path local-dir k)])
                               (:domains global-config)))
        domains-info (init-domain-info-map
                      lfs
                      (assoc global-config :domains domains-map))]
    (future
     (load-and-sync-status domains-info
                           (fn [domain]
                             (open-domain
                              local-config
                              (str (path local-dir domain))
                              (domain/host-shards (domains-info domain))
                              ))))
    domains-info
    ))

;; returns map of domain to domain info and launches futures that will fill in the domain info
(defn- sync-data [global-config local-config token]
  (if (cache? global-config token)
    (sync-local global-config local-config)
    (sync-updated global-config local-config token)
    ))

(defn- close-lps [domains-info]
  (doseq [[domain info] domains-info]
    (doseq [shard (domain/host-shards info)]
      (let [lp (domain/domain-data info shard)]
        (log-message "Closing LP for " domain "/" shard)
        (if lp
          (.close lp)
          (log-message "LP not loaded"))
        (log-message "Closed LP for " domain "/" shard))
      )))

(defn- get-readable-domain-info [domains-info domain]
  (let [info (domains-info domain)]
    (when-not info
      (throw (thrift/domain-not-found-ex domain)))
    (when-not (thrift/status-ready? (domain/domain-status info))
      (throw (thrift/domain-not-loaded-ex domain)))
    info ))

(defn- update-all-domains [domains-info global-config local-config]
  (let [domains (keys domains-info)
        max-kbs 1024]
    (log-message "Updating ALL domains: " domains)
    (start-download-supervisor (count domains) max-kbs)
    (future
      (try
        (load-and-sync-status domains-info
                              (fn [domain]
                                (use-cache-or-update domain domains-info global-config local-config false)))
        (log-message "Finished updating all domains from remote")
        (catch Throwable t (log-error t "Error when syncing data") (throw t))))))

(defn- update-domains [domains]
  (log-message "Updating domains: " domains)
  true) ;; just return true for now

(defn- update-domain [domain]
  (update-domains [domain]))

;; 1. after *first* load finishes, right the global config with the token within
;; 2. when receive an update, open up the version and mkdir immediately,
;;    start loading
;; 3. when starting up, if token is written start up immediately,
;;    start up loaders for anything with incomplete version, clearing dir first
;; 4. when loaders finish, complete the version and delete the old
;;    version (on startup should delete old versions) - does deletion
;;    need to be throttled?
;; 5. Create Hadoop FS on demand... need retry logic if loaders fail?


  ;; list<Value> multiGet(1: string domain, 2: list<binary> key)
  ;;   throws (1: DomainNotFoundException dnfe, 2: HostsDownException hde, 3: DomainNotLoadedException dnle);
  ;; list<Value> multiGetString(1: string domain, 2: list<string> key)
  ;;   throws (1: DomainNotFoundException dnfe, 2: HostsDownException hde, 3: DomainNotLoadedException dnle);
  ;; list<Value> multiGetInt(1: string domain, 2: list<i32> key)
  ;;   throws (1: DomainNotFoundException dnfe, 2: HostsDownException hde, 3: DomainNotLoadedException dnle);
  ;; list<Value> multiGetLong(1: string domain, 2: list<i64> key)
  ;;   throws (1: DomainNotFoundException dnfe, 2: HostsDownException hde, 3: DomainNotLoadedException dnle);

  ;; list<Value> directMultiGet(1: string domain, 2: list<binary> key)
  ;;   throws (1: DomainNotFoundException dnfe, 2: DomainNotLoadedException dnle, 3: WrongHostException whe);

;;  Example of domains-info local:
;;  {test {:elephantdb.domain/shard-index {:elephantdb.shard/hosts-to-shards {192.168.1.3 #{0 1 2 3}}, :elephantdb.shard/shards-to-hosts {3 #{192.168.1.3}, 2 #{192.168.1.3}, 1 #{192.168.1.3}, 0 #{192.168.1.3}}}, :elephantdb.domain/domain-status #<Atom@3643b5bb: #<DomainStatus <DomainStatus loading:LoadingStatus()>>>, :elephantdb.domain/domain-data #<Atom@4feaefc5: nil>}}
(defn service-handler [global-config local-config token]
  (let [domains-info (sync-data global-config local-config token)
        client (atom nil)
        #^ReentrantReadWriteLock rw-lock (mk-rw-lock)
        ret (proxy [ElephantDB$Iface Shutdownable] []
              (shutdown
               []
               (log-message "ElephantDB received shutdown notice...")
               (write-locked
                rw-lock
                (dofor [[_ info] domains-info]
                       (domain/set-domain-status! info (thrift/shutdown-status))))
               (close-lps domains-info))

              (get
               [#^String domain #^bytes key]
               (.get @client domain key))

              (getString
               [#^String domain #^String key]
               (.getString @client domain key))

              (getInt
               [#^String domain key]
               (.getInt @client domain key))

              (getLong
               [#^String domain key]
               (.getLong @client domain key))

              (multiGet
               [#^String domain keys]
               (.multiGet @client domain keys))

              (multiGetString
               [#^String domain #^String keys]
               (.multiGetString @client domain keys))

              (multiGetInt
               [#^String domain keys]
               (.multiGetInt @client domain keys))

              (multiGetLong
               [#^String domain keys]
               (.multiGetLong @client domain keys))

              (directMultiGet
               [#^String domain keys]
               (read-locked
                rw-lock
                (dofor [key keys]
                       (let [info                   (get-readable-domain-info domains-info domain)
                             shard                  (domain/key-shard domain info key)
                             #^LocalPersistence lp  (domain/domain-data info shard)]
                         (log-debug "Direct get key " (seq key) "at shard " shard)
                         (when-not lp
                           (throw (thrift/wrong-host-ex)))
                         (thrift/mk-value (.get lp key))
                         ))))

              (getDomainStatus
               [#^String domain]
               (let [info (domains-info domain)]
                 (when-not info
                   (throw (thrift/domain-not-found-ex domain)))
                 (domain/domain-status info)))

              (getDomains
               []
               (keys domains-info))

              (getStatus
               []
               (thrift/elephant-status
                (into {} (for [[d i] domains-info] [d (domain/domain-status i)]))))

              (isFullyLoaded
               []
               (let [stat (.get_domain_statuses (.getStatus this))]
                 (every? #(or (thrift/status-ready? %) (thrift/status-failed? %)) (vals stat))))

              (updateAll
               []
                (update-all-domains domains-info global-config local-config)
               )

              (update
               [#^String domain]
                ;; TODO
                (update-domain domain)
               ))]
      (reset! client (client. ret (:hdfs-conf local-config) global-config))
      ret ))
