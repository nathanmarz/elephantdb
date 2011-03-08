(ns elephantdb.loader
  (:import [elephantdb.store DomainStore])
  (:use [elephantdb util hadoop config log])
  (:require [elephantdb [domain :as domain]]))



(defn- shard-path [domain-version shard]
  (str-path domain-version shard))

(defn open-domain-shard [persistence-factory local-config local-shard-path]
  (log-message "Opening LP " local-shard-path)
  (with-ret (.openPersistenceForRead
             persistence-factory
             local-shard-path
             (persistence-options local-config persistence-factory))
    (log-message "Opened LP " local-shard-path)
    ))

(defn open-domain [local-config local-domain-root shards]
  (let [lfs (local-filesystem)
        local-store (DomainStore. lfs local-domain-root)
        domain-spec (read-domain-spec (local-filesystem) local-domain-root)
        local-version-path (.mostRecentVersionPath local-store)
        future-lps (dofor [s shards]
                          [s
                           (future
                            (open-domain-shard
                             (:persistence-factory domain-spec)
                             local-config
                             (shard-path local-version-path s)
                             ))])
        ]
    (with-ret (into {} (dofor [[s f] future-lps] [s (.get f)]))
      (log-message "Finished opening domain at " local-domain-root))
    ))

(defn close-domain [domain domains-info]
  (let [domain-info (domains-info domain)]
    (log-message "Closing domain: " domain " domain-info: " domain-info)
    (doseq [[shard lp] (domain/domain-data domain-info)]
      (try
        (.close lp)
        (catch Throwable t (log-error t "Error when closing local persistence for domain: " domain " and shard: " shard) (throw t))))
    (log-message "Finished closing domain: " domain)))

(defn load-domain-shard [fs persistence-factory local-config local-shard-path remote-shard-path ^ShardState state]
  ;; TODO: respect the max copy rate
  ;; TODO: do a streaming recursive copy that can be rate limited (rate limited with the other shards...)
  (if (.exists fs (path remote-shard-path))
    (do
      (log-message "Copying " remote-shard-path " to " local-shard-path)
      (copy-local fs remote-shard-path local-shard-path state)
      (log-message "Copied " remote-shard-path " to " local-shard-path)
      )
    (do
      (log-message "Shard " remote-shard-path " did not exist. Creating empty LP")
      (.close (.createPersistence persistence-factory local-shard-path (persistence-options local-config persistence-factory)))))
    )

;; returns a map from shard to LP
(defn load-domain [domain fs local-config local-domain-root remote-path shards ^DownloadState state]
  (log-message "Loading domain at " remote-path " to " local-domain-root)
  (let [lfs                (local-filesystem)
        remote-vs          (DomainStore. fs remote-path)
        domain-spec        (convert-java-domain-spec (.getSpec remote-vs))
        local-vs           (DomainStore. lfs local-domain-root (.getSpec remote-vs))
        remote-version     (.mostRecentVersion remote-vs)
        local-version-path (.createVersion local-vs remote-version)
        _                  (mkdirs lfs local-version-path)
        domain-state       ((:shard-states state) domain)
        shard-loaders      (dofor [s shards]
                                  (future
                                   (load-domain-shard
                                    fs
                                    (:persistence-factory domain-spec)
                                    local-config
                                    (shard-path local-version-path s)
                                    (shard-path
                                     (.versionPath remote-vs remote-version) s)
                                    (domain-state s))
                                   (swap! (:finished-loaders state) + 1))
                                  )]
    (future-values shard-loaders)
    (.succeedVersion local-vs local-version-path)
    (log-message "Successfully loaded domain at " remote-path " to " local-domain-root " with version " remote-version)
    (open-domain local-config local-domain-root shards)
    ))

(defn supervise-downloads [amount-shards max-kbs interval-ms ^DownloadState state]
  (let [shard-states (:shard-states state)
        finished-loaders (:finished-loaders state)
        max-kbs-per-shard (/ max-kbs amount-shards)]
    (loop []
      (when (< @finished-loaders amount-shards)
        (log-message "Download supervisor - "
                     "Finished: " @finished-loaders
                     ", waiting for: " (- amount-shards @finished-loaders))
        (doseq [s shard-states]
          (let [downloaded-kb (:downloaded-kb s)
                do-download (:do-download s)
                sleep-interval (:sleep-interval s)]
            (Thread/sleep interval-ms)
            (let [dl-kb @downloaded-kb
                  sleep-ms (rand 1000)] ;; sleep random amount of time up to 1s
              (if (>= dl-kb max-kbs-per-shard)
                (do
                  (log-message "Download throttle exceeded: " dl-kb " (" max-kbs-per-shard " allowed) - Waiting for: " sleep-ms "ms")
                  (reset! do-download false)
                  (reset! sleep-interval sleep-ms)
                  (reset! downloaded-kb 0))
                (do
                  (reset! do-download true)
                  (reset! downloaded-kb 0))))))
        (recur)))))

(defn start-download-supervisor [amount-shards max-kbs ^DownloadState state]
  (let [interval-factor-secs 0.01 ;; check every 0.01s
        interval-ms (int (* interval-factor-secs 1000))
        max-kbs-val (int (* max-kbs interval-factor-secs))]
    (future
      (log-message "Starting download supervisor")
      (reset! (:finished-loaders state) 0)
      (if (and (> amount-shards 0) ;; only monitor if there's an actual download throttle
               (> max-kbs 0))      ;; and shards to be downloaded
        (supervise-downloads amount-shards max-kbs-val interval-ms state))
      (log-message "Download supervisor finished"))))