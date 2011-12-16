(ns elephantdb.keyval.service
  (:use [elephantdb.keyval.thrift :only (with-elephant-connection)])
  (:require [clojure.string :as s]
            [hadoop-util.core :as h]
            [elephantdb.common.util :as u]
            [elephantdb.common.loader :as loader]
            [elephantdb.common.hadoop :as hadoop]
            [elephantdb.common.domain :as domain]
            [elephantdb.common.thrift :as thrift]
            [elephantdb.common.shard :as shard]
            [elephantdb.common.logging :as log])
  (:import [java.io File]
           [elephantdb.common.iface IShutdownable]
           [org.apache.thrift.server THsHaServer THsHaServer$Options]
           [org.apache.thrift.protocol TBinaryProtocol$Factory]
           [org.apache.thrift.transport TNonblockingServerSocket]
           [java.util.concurrent.locks ReentrantReadWriteLock]
           [elephantdb.generated ElephantDB ElephantDB$Iface ElephantDB$Processor
            WrongHostException DomainNotFoundException DomainNotLoadedException]
           [elephantdb.persistence Persistence]
           [elephantdb.store DomainStore]
           [org.apache.thrift TException]))

(defn- init-domain-info-map
  " Generates a map with kv pairs of the following form:
    {\"domain-name\" {:shard-index {:hosts-to-shards <map>
                                    :shards-to-hosts <map>}
                      :domain-status <status-atom>
                      :domain-data <data-atom>}}"
  [fs {:keys [domains hosts replication]}]
  (let [domain-shards (shard/shard-domains fs domains hosts replication)]
    (u/update-vals (fn [k _]
                     (-> k domain-shards domain/init-domain-info))
                   domains)))

(defn try-thrift
  "Applies each kv pair to the supplied `func` in parallel, farming
  the work out to futures; each domain is set to `initial-status` at
  the beginning of the func, and `thrift/ready-status` on successful
  completion. (Failures are marked as appropriate.)

  After completing all functions, we remove all versions of each
  domain but the last."
  [domains-info local-dir initial-status func]
  (u/with-ret (u/p-dofor [[domain info] domains-info]
                         (try (domain/set-domain-status! info initial-status)
                              (func domain info)
                              (domain/set-domain-status! info (thrift/ready-status))
                              (catch Throwable t
                                (log/error t "Error when loading domain " domain)
                                (domain/set-domain-status! info
                                                           (thrift/failed-status t)))))
    (try (log/info "Removing all old versions of updated domains!")
         (domain/cleanup-domains! (keys domains-info) local-dir)
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
        remote-fs (h/filesystem hdfs-conf)
        
        ;; Do we throttle? TODO: Use this arg.
        throttle? (->> (vals domains-info)
                       (map domain/domain-status)
                       (some thrift/status-ready?))]
    (try-thrift domains-info
                local-dir
                (thrift/ready-status :loading? true)
                (fn [domain info]
                  (let [remote-path (clojure.core/get remote-path-map domain)
                        remote-vs (domain/try-domain-store remote-fs remote-path)]
                    (when remote-vs
                      (let [local-domain-root (str (h/path local-dir domain))
                            local-vs (domain/mk-local-vs remote-vs local-domain-root)]
                        (if (domain/domain-needs-update? local-vs remote-vs)
                          (->> (loader/load-domain domain
                                                   remote-fs
                                                   local-db-conf
                                                   local-domain-root
                                                   remote-path
                                                   loader-state)
                               (domain/set-domain-data! rw-lock domain info)
                               (log/info
                                "Finished loading all updated domains from remote."))
                          (let [{:keys [finished-loaders shard-states]} loader-state]
                            (swap! finished-loaders +
                                   (count (shard-states domain))))))))))))

(defn load-cached-domains!
  [edb-config rw-lock domains-info]
  (let [{:keys [hdfs-conf local-dir local-db-conf]} edb-config
        remote-path-map (:domains edb-config)
        remote-fs (h/filesystem hdfs-conf)]
    (try-thrift domains-info
                local-dir
                (thrift/loading-status)
                (fn [domain info]
                  (let [remote-path  (clojure.core/get remote-path-map domain)
                        remote-vs (DomainStore. remote-fs remote-path)
                        local-path (str (h/path local-dir domain))
                        local-vs (domain/mk-local-vs remote-vs local-path)]  
                    (when-let [new-data (and (domain/domain-has-data? local-vs)
                                             (loader/open-domain
                                              local-db-conf
                                              local-path
                                              (domain/host-shards info)))]
                      (domain/set-domain-data! rw-lock domain info new-data)))))))

(defn purge-unused-domains!
  "Walks through the supplied local directory, recursively deleting
  all directories with names that aren't present in the supplied
  `domains`."
  [domain-seq local-dir]
  (let [lfs (h/local-filesystem)
        domain-set (set domain-seq)]
    (u/dofor [domain-path (-> local-dir h/mk-local-path .listFiles)
              :when (and (.isDirectory domain-path)
                         (not (contains? domain-set
                                         (.getName domain-path))))]
             (log/info "Deleting local path of deleted domain: " domain-path)
             (h/delete lfs (.getPath domain-path) true))))

(defn prepare-local-domains!
  "Wipe domains not being used, make ready all cached domains, and get
  the downloading process started for all others."
  [domains-info edb-config rw-lock]
  (let [{:keys [local-dir]} edb-config
        domain-seq (keys domains-info)]
    (u/with-ret domains-info
      (future        
        (purge-unused-domains! domain-seq local-dir)
        (load-cached-domains! edb-config rw-lock domains-info)
        (update-and-sync-status! edb-config
                                 rw-lock
                                 domains-info
                                 (->> domains-info
                                      (u/val-map domain/host-shards)
                                      (hadoop/mk-loader-state)))))))

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
                            (hadoop/mk-loader-state))
         shard-amount (u/flattened-count (vals (:shard-states download-state)))]
    (log/info "UPDATER - Updating domains: " (s/join ", " (keys domains-info)))
    (reset! download-supervisor (loader/start-download-supervisor
                                 shard-amount max-kbs download-state))
    (future (update-and-sync-status! edb-config
                                     rw-lock
                                     domains-info
                                     download-state))))

(defn- trigger-update
  [service-handler download-supervisor domains-info edb-config rw-lock]
  (u/with-ret true
    (if (service-updating? service-handler download-supervisor)
      (log/info "UPDATER - Not updating as update process still in progress.")
      (update-domains download-supervisor domains-info edb-config rw-lock))))

;; 5. TODO: (What does this mean?) Create Hadoop FS on demand... need
;; retry logic if loaders fail?

;; When we first start up the service, we trigger an unthrottled
;; download. It'd be good if we had access to the download supervisor
;; inside of prepare-local-domain, and could decide whether or not to
;; throttle the thing.

(defn- get-index
  "Returns map of domain->host-sets and host->domain-sets."
  [domain-shard-indexes domain]
  (or (clojure.core/get domain-shard-indexes domain)
      (throw (thrift/domain-not-found-ex domain))))

(defn- prioritize-hosts
  [localhost domain-shard-indexes domain key]
  (let [index (get-index domain-shard-indexes domain)
        hosts (shuffle (shard/key-hosts domain index key))]
    (if (some #{localhost} hosts)
      (cons localhost (u/remove-val localhost hosts))
      hosts)))

(defn multi-get-remote
  {:dynamic true}
  [host port domain keys]
  (with-elephant-connection host port service
    (.directMultiGet service domain keys)))

;; If the client has a "local-elephant", or an enclosed edb service,
;; and the "totry" private IP address fits the local-hostname, go
;; ahead and do a direct multi-get internally. Else, create a
;; connection to the other server and do that direct multiget.

(defn try-multi-get
  [service local-hostname port domain keys totry]
  (let [suffix (format "%s:%s/%s" totry domain keys)]
    (try (if (= totry local-hostname)
           (.directMultiGet service domain keys)
           (multi-get-remote totry port domain keys))
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

(defn- host-indexed-keys
  "returns [hosts-to-try global-index key all-hosts] seq"
  [localhost domain-shard-indexes domain keys]
  (for [[gi key] (map-indexed vector keys)
        :let [priority-hosts
              (prioritize-hosts localhost domain-shard-indexes domain key)]]
    [priority-hosts gi key priority-hosts]))

(defn- multi-get*
  "executes multi-get, returns seq of [global-index val]"
  [service localhost port domain host host-indexed-keys]
  (when-let [vals (try-multi-get service
                                 localhost
                                 port
                                 domain
                                 (map u/third host-indexed-keys)
                                 host)]
    (map (fn [v [hosts gi key all-hosts]] [gi v])
         vals
         host-indexed-keys)))

(defn service-handler
  "Entry point to edb. `service-handler` returns a proxied
  implementation of EDB's interface."
  [{:keys [hdfs-conf local-dir port domains hosts replication] :as edb-config}]
  (let [^ReentrantReadWriteLock rw-lock (u/mk-rw-lock)
        download-supervisor (atom nil)
        localhost   (u/local-hostname)
        filesystem  (h/filesystem hdfs-conf)
        domains-info (init-domain-info-map filesystem edb-config)
        domain-shard-indexes (shard/shard-domains filesystem
                                                  domains
                                                  hosts
                                                  replication)]
    (prepare-local-domains! domains-info edb-config rw-lock)
    (reify
      IShutdownable
      (shutdown [_]
        (log/info "ElephantDB received shutdown notice...")
        (u/with-write-lock rw-lock
          (u/dofor [[_ info] domains-info]
                   (domain/set-domain-status! info (thrift/shutdown-status))))
        (close-lps domains-info))

      ElephantDB$Iface
      (get [this domain key]
        (first (.multiGet this domain [key])))

      (getInt [this domain key]
        (.get this domain key))

      (getLong [this domain key]
        (.get this domain key))

      (getString [this domain key]
        (.get this domain key))

      (directMultiGet [_ domain keys]
        (u/with-read-lock rw-lock
          (let [info (get-readable-domain-info domains-info domain)]
            (u/dofor [key keys
                      :let [shard (domain/key-shard domain info key)
                            ^Persistence lp (domain/domain-data info shard)]]
                     (log/debug "Direct get keys " (seq key) "at shard " shard)
                     (if lp
                       (thrift/mk-value (.get lp key))
                       (throw (thrift/wrong-host-ex)))))))

      (multiGet [this domain keys]
        (let [host-indexed-keys (host-indexed-keys localhost
                                                   domain-shard-indexes
                                                   domain
                                                   keys)]
          (loop [keys-to-get host-indexed-keys
                 results []]
            (let [host-map (group-by ffirst keys-to-get)
                  rets (u/parallel-exec
                        (for [[host host-indexed-keys] host-map]
                          #(vector host (multi-get* this
                                                    localhost
                                                    port
                                                    domain
                                                    host
                                                    host-indexed-keys))))
                  succeeded       (filter second rets)
                  succeeded-hosts (map first succeeded)
                  results (->> (map second succeeded)
                               (apply concat results))
                  failed-host-map (apply dissoc host-map succeeded-hosts)]
              (if (empty? failed-host-map)
                (map second (sort-by first results))
                (recur (for [[[_ & hosts] gi key all-hosts]
                             (apply concat (vals failed-host-map))]
                         (if (empty? hosts)
                           (throw (thrift/hosts-down-ex all-hosts))
                           [hosts gi key all-hosts]))
                       results))))))

      (multiGetInt [this domain keys]
        (.multiGet this domain keys))

      (multiGetLong [this domain keys]
        (.multiGet this domain keys))

      (multiGetString [this domain keys]
        (.multiGet this domain keys))
      
      (getDomainStatus [_ domain]
        (let [info (domains-info domain)]
          (when-not info
            (throw (thrift/domain-not-found-ex domain)))
          (domain/domain-status info)))
      
      (getDomains [_]
        (keys domains-info))
      
      (getStatus [_]
        (thrift/elephant-status
         (u/val-map domain/domain-status domains-info)))

      (isFullyLoaded [this]
        (let [stat (.get_domain_statuses (.getStatus this))]
          (every? #(or (thrift/status-ready? %)
                       (thrift/status-failed? %))
                  (vals stat))))

      (isUpdating [this]
        (service-updating? this download-supervisor))
      
      (updateAll [this]
        (trigger-update this
                        download-supervisor
                        domains-info
                        edb-config
                        rw-lock))
      
      (update [this domain]
        (trigger-update this
                        download-supervisor
                        (select-keys domains-info [domain])
                        edb-config
                        rw-lock)))))

(defn thrift-server
  [service-handler port]
  (let [options (THsHaServer$Options.)]
    (set! (.maxWorkerThreads options) 64)
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
