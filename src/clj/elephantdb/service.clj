(ns elephantdb.service
  (:import [elephantdb.generated ElephantDB ElephantDB$Iface])
  (:import [elephantdb Shutdownable])
  (:require [elephantdb [domain :as d] [thrift :as thrift]])
  (:use [elephantdb config log util hadoop loader]))



  ; Value get(1: string domain, 2: binary key)
  ;   throws (1: DomainNotFoundException dnfe, 2: HostsDownException hde, 3: DomainNotLoadedException dnle);
  ; Value getString(1: string domain, 2: string key)
  ;   throws (1: DomainNotFoundException dnfe, 2: HostsDownException hde, 3: DomainNotLoadedException dnle);
  ; Value getInt(1: string domain, 2: i32 key)
  ;   throws (1: DomainNotFoundException dnfe, 2: HostsDownException hde, 3: DomainNotLoadedException dnle);
  ; Value getLong(1: string domain, 2: i64 key)
  ;   throws (1: DomainNotFoundException dnfe, 2: HostsDownException hde, 3: DomainNotLoadedException dnle);
  ; 
  ; Value directGet(1: string domain, 2: binary key)
  ;   throws (1: DomainNotFoundException dnfe, 2: HostsDownException hde, 3: DomainNotLoadedException dnle, 4: WrongHostException whe);
  ; Value directGetString(1: string domain, 2: string key)
  ;   throws (1: DomainNotFoundException dnfe, 2: HostsDownException hde, 3: DomainNotLoadedException dnle, 4: WrongHostException whe);
  ; Value directGetInt(1: string domain, 2: i32 key)
  ;   throws (1: DomainNotFoundException dnfe, 2: HostsDownException hde, 3: DomainNotLoadedException dnle, 4: WrongHostException whe);
  ; Value directGetLong(1: string domain, 2: i64 key)
  ;   throws (1: DomainNotFoundException dnfe, 2: HostsDownException hde, 3: DomainNotLoadedException dnle, 4: WrongHostException whe);
  ; 
  ; DomainStatus getDomainStatus(1: string domain);
  ; list<string> getDomains();
  ; Status getStatus();
  ; bool isFullyLoaded();
  ; 
  ; /*
  ;   This method will re-download the global configuration file and add any new domains
  ; */
  ; void updateAll() throws (1: InvalidConfigurationException ice);
  ; //returns whether it's updating or not
  ; bool update(1: string domain);
; { :replication 2
;   :hosts ["elephant1.server" "elephant2.server" "elephant3.server"]
;   :port 3578
;   :domains {"graph" "s3n://mybucket/elephantdb/graph"
;             "docs"  "/data/docdb"
;             }
; }


(defn- init-domain-info-map-global [fs global-config]
  ;; if token is the same as what is local, initialize using the local data. token is the maximum version to load from hdfs
  ;; otherwise, read what's globally available
  )

(defn- init-domain-info-map-local [local-config]
  (throw (RuntimeException. "not implemented"))
  )

;; should delete any domains that don't exist in config as well 
;; returns map of domain to domain info and launches futures that will fill in the domain info
(defn- sync-data [global-config local-config token]
  (let [fs (filesystem (:hdfs local-config))
        domains-info (init-domain-info-map-global global-config)
        local-dir (:local-dir local-config)
        lfs (doto (local-filesystem) (clear-dir local-dir))]
    (dofor [[domain remote-location] (:domains global-config)]
         (future
           (set-domain-data! (domains-info domain)
             (load-domain
               (path local-dir domain)
               (global-domain-root global-config domain)
               (d/host-shards (domains-info domain))))
            (set-domain-status!
              (domains-info domain)
              (thrift/ready-status false))))
      domains-info ))

(defn service-handler [global-config local-config token]
  (let []
  (proxy [ElephantDB$Iface Shutdownable] []
    (shutdown []
      (log-message "ElephantDB received shutdown notice... killing supervisor")
      ;; TODO: shutdown logic (read-write lock to close dbs)
      )
    (get [#^String domain #^bytes key]
      )
    (getString [#^String domain #^String key]
      )
    (getInt [#^String domain key]
      )

    (directGet [#^String domain key]
      )
    (directGetString [#^String domain #^String key]
      )
    (directGetInt [#^String domain key]
      )
    (directGetLong [#^String domain key]
      )

    (getDomainStatus [#^String domain]
      )
    (getDomains []
      )
    (getStatus []
      )
    (isFullyLoaded []
      )

    (updateAll []
      )
    (update [#^String domain]
      )
    )))