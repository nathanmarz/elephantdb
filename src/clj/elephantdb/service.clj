(ns elephantdb.service
  (:import [java.util.concurrent.locks ReentrantReadWriteLock])
  (:import [elephantdb.generated ElephantDB ElephantDB$Iface])
  (:import [elephantdb Shutdownable client])
  (:import [elephantdb.persistence LocalPersistence])
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
                          (domain/set-domain-data! (domains-info domain)
                                                   (loader-fn domain))
                          (domain/set-domain-status!
                           (domains-info domain)
                           (thrift/ready-status false))
                          (catch Throwable t
                            (log-error t "Error when loading domain " domain)
                            (domain/set-domain-status!
                             (domains-info domain)
                             (thrift/failed-status t))
                            ))))]
    (with-ret (future-values loaders)
      (log-message "Successfully loaded all domains"))
    ))

(defn sync-data-scratch [domains-info global-config local-config]
  (let [fs (filesystem (:hdfs-conf local-config))
        local-dir (:local-dir local-config)]
    (load-and-sync-status domains-info
                          (fn [domain]
                            (load-domain
                             fs
                             local-config
                             (str (path local-dir domain))
                             (-> global-config (:domains) (get domain))
                             (domain/host-shards (domains-info domain)))))
    ))

(defn- sync-global [global-config local-config token]
  (log-message "Loading remote data")
  (let [fs (filesystem (:hdfs-conf local-config))
        domains-info (init-domain-info-map fs global-config)
        local-dir (:local-dir local-config)
        lfs (doto (local-filesystem) (clear-dir local-dir))
        cache-config (assoc global-config :token token)]
    (log-message "Domains info:" domains-info)
    (future
     (sync-data-scratch domains-info global-config local-config)
     (log-message "Caching global config " cache-config)
     (cache-global-config! local-config cache-config))
    domains-info ))



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

;; should delete any domains that don't exist in config as well 
;; returns map of domain to domain info and launches futures that will fill in the domain info
(defn- sync-data [global-config local-config token]
  (if (cache? global-config token)
    (sync-local global-config local-config)
    (sync-global global-config local-config token)
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

;; 1. after *first* load finishes, right the global config with the token within
;; 2. when receive an update, open up the version and mkdir immediately,
;;    start loading
;; 3. when starting up, if token is written start up immediately,
;;    start up loaders for anything with incomplete version, clearing dir first
;; 4. when loaders finish, complete the version and delete the old
;;    version (on startup should delete old versions) - does deletion
;;    need to be throttled?
;; 5. Create Hadoop FS on demand... need retry logic if loaders fail?

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

              (directGet
               [#^String domain key]
               (read-locked
                rw-lock
                (let [info                   (get-readable-domain-info domains-info domain)
                      shard                  (domain/key-shard domain info key)
                      #^LocalPersistence lp  (domain/domain-data info shard)]
                  (log-debug "Direct get key" (seq key) "at shard" shard)
                  (when-not lp
                    (throw (thrift/wrong-host-ex)))
                  (thrift/mk-value (.get lp key))
                  )))
              
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
                 (every? thrift/status-ready? (vals stat))))

              (updateAll
               []
               ;; TODO
               )

              (update
               [#^String domain]
               ;; TODO
               ))]
      (reset! client (client. ret (:hdfs-conf local-config) global-config))
      ret ))
