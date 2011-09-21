(ns elephantdb.loader
  (:use [elephantdb util hadoop config log])
  (:import [elephantdb.store DomainStore]))

(defn- shard-path
  [domain-version shard]
  (str-path domain-version shard))

(defn open-domain-shard
  [persistence-factory db-conf local-shard-path]
  (log-message "Opening LP " local-shard-path)
  (with-ret (.openPersistenceForRead
             persistence-factory
             local-shard-path
             (persistence-options db-conf persistence-factory))
    (log-message "Opened LP " local-shard-path)))

(defn open-domain
  [db-conf local-domain-root shards]
  (let [lfs (local-filesystem)
        local-store (DomainStore. lfs local-domain-root)
        domain-spec (read-domain-spec lfs local-domain-root)
        local-version-path (.mostRecentVersionPath local-store)
        future-lps (dofor [s shards]
                          [s (future (open-domain-shard
                                      (:persistence-factory domain-spec)
                                      db-conf
                                      (shard-path local-version-path s)))])]
    (with-ret (map-mapvals future-lps (memfn get))
      (log-message "Finished opening domain at " local-domain-root))))

(defn close-shard
  [[shard lp] domain]
  (try (.close lp)
       (catch Throwable t
         (log-error t "Error when closing local persistence for domain: "
                    domain
                    " and shard: "
                    shard)
         (throw t))))

(defn close-domain
  [domain domain-data]
  (log-message (format "Closing domain: %s with data: %s" domain domain-data))
  (doseq [shard-data domain-data]
    (close-shard shard-data domain))
  (log-message "Finished closing domain: " domain))

;; TODO: respect the max copy rate
;;
;; TODO: do a streaming recursive copy that can be rate limited (rate
;; limited with the other shards...)

(defn load-domain-shard!
  [fs persistence-factory persistence-opts local-shard-path remote-shard-path state]
  (if (.exists fs (path remote-shard-path))
    (do (log-message "Copying " remote-shard-path " to " local-shard-path)
        (copy-local fs remote-shard-path local-shard-path state)
        (log-message "Copied " remote-shard-path " to " local-shard-path))
    (do (log-message "Shard " remote-shard-path " did not exist. Creating empty LP")
        (.close (.createPersistence persistence-factory
                                    local-shard-path
                                    persistence-opts)))))

(defn load-domain
  "returns a map from shard to LP."
  [domain fs local-db-conf local-domain-root remote-path shards state]
  (log-message "Loading domain at " remote-path " to " local-domain-root)
  (let [lfs           (local-filesystem)
        remote-vs     (DomainStore. fs remote-path)
        factory       (-> (.getSpec remote-vs)
                          (convert-java-domain-spec)
                          (:persistence-factory))
        local-vs       (DomainStore. lfs local-domain-root (.getSpec remote-vs))
        remote-version (.mostRecentVersion remote-vs)
        local-v-path   (.createVersion local-vs remote-version)
        remote-v-path  (.versionPath remote-vs remote-version)
        _              (mkdirs lfs local-v-path)
        domain-state   (get (:shard-states state) domain)
        shard-loaders  (dofor [s shards]
                              (with-ret-bound [f (future
                                                   (load-domain-shard!
                                                    fs
                                                    factory
                                                    (persistence-options local-db-conf
                                                                         factory)
                                                    (shard-path local-v-path s)
                                                    (shard-path remote-v-path s)
                                                    (domain-state s)))]
                                (swap! (:shard-loaders state) conj f)))]
    (future-values shard-loaders)
    (.succeedVersion local-vs local-v-path)
    (log-message (format "Successfully loaded domain at %s to %s with version %s."
                         remote-path
                         local-domain-root
                         remote-version))
    (open-domain local-db-conf local-domain-root shards)))

(defn supervise-shard
  [max-kbs total-kb ^ShardState shard-state]
  (let [downloaded-kb  (:downloaded-kb shard-state)
        sleep-interval (:sleep-interval shard-state)]
    (let [dl-kb @downloaded-kb
          sleep-ms (rand 1000)] ;; sleep random amount of time up to 1s
      (swap! total-kb + dl-kb)
      (reset! sleep-interval
              (if (>= @total-kb max-kbs)
                sleep-ms
                0))
      (reset! downloaded-kb 0))))

(defn supervise-downloads
  [amount-shards max-kbs interval-ms ^DownloadState state]
  (let [domain-to-shard-states (:shard-states state)
        finished-loaders       (:finished-loaders state)
        shard-loaders          (:shard-loaders state)
        total-kb (atom 0)]
    (loop []
      (reset! total-kb 0)
      (Thread/sleep interval-ms)
      (let [finished-shard-loaders (count (filter (memfn isDone)
                                                  @shard-loaders))]
        (when (< (+ @finished-loaders finished-shard-loaders)
                 amount-shards)
          (doseq [[domain shard-states] domain-to-shard-states
                  [s-id ^ShardState s-state] shard-states]
            (supervise-shard max-kbs total-kb s-state))
          (recur))))))

(defn start-download-supervisor
  [amount-shards max-kbs ^DownloadState state]
  (let [interval-factor-secs 0.1 ;; check every 0.01s
        interval-ms (int (* interval-factor-secs 1000))
        max-kbs-val (int (* max-kbs interval-factor-secs))]
    (future
      (log-message "Starting download supervisor for " amount-shards " shard loaders")
      (reset! (:finished-loaders state) 0)
      (when (and (> amount-shards 0) ;; only monitor if there's an actual download throttle
                 (> max-kbs 0))      ;; and shards to be downloaded
        (supervise-downloads amount-shards max-kbs-val interval-ms state))
      (log-message "Download supervisor finished"))))
