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

(defn- init-domain-info-map-global [fs global-config]
  (let [domain-shards (shard/shard-domains fs global-config)]
    (into {}
      (dofor [domain (keys (:domains global-config))]
          [domain (domain/init-domain-info (domain-shards domain))]
            ))))

(defn- init-domain-info-map-local [local-config]
  (throw (RuntimeException. "not implemented"))
  )


;; for version cache:
;;  1. before loading, write the global configs locally with token
;;  2. you know a domain is downloaded when the domain spec is written in the domain dir
;;  3. when starting up, only refresh those domains where a newer version exists that is less than or equal to the token (ignore global-config if token is the same as what's written locally)

;; (defn write-clj-config! [conf fs str-path]

;; if token is the same as what is local, initialize using the local data. token is the maximum version to load from hdfs
;; otherwise, read what's globally available

;; should delete any domains that don't exist in config as well 
;; returns map of domain to domain info and launches futures that will fill in the domain info
(defn- sync-data [global-config local-config token]
  (let [fs (filesystem (:hdfs-conf local-config))
        domains-info (init-domain-info-map-global fs global-config)
        local-dir (:local-dir local-config)
        lfs (doto (local-filesystem) (clear-dir local-dir))]
    (write-clj-config! (assoc global-config :token token) lfs (local-global-config-cache local-dir))
    (doseq [[domain remote-location] (:domains global-config)]
      (future
        (try
          (domain/set-domain-data! (domains-info domain)
            (load-domain
              fs
              local-config
              (str (path local-dir domain))
              remote-location
              token
              (domain/host-shards (domains-info domain))))
           (domain/set-domain-status!
             (domains-info domain)
             (thrift/ready-status false))
        (catch Throwable t
          (log-error t "Error when loading domain " domain)
          (domain/set-domain-status!
             (domains-info domain)
             (thrift/failed-status t))
             ))))
      domains-info ))

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
