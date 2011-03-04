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

(defn load-domain-shard [fs persistence-factory local-config local-shard-path remote-shard-path ^LoaderState state]
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
(defn load-domain [fs local-config local-domain-root remote-path shards ^LoaderState state]
  (log-message "Loading domain at " remote-path " to " local-domain-root)
  (let [lfs                (local-filesystem)
        remote-vs          (DomainStore. fs remote-path)
        domain-spec        (convert-java-domain-spec (.getSpec remote-vs))
        local-vs           (DomainStore. lfs local-domain-root (.getSpec remote-vs))
        remote-version     (.mostRecentVersion remote-vs)
        local-version-path (.createVersion local-vs remote-version)
        _                  (mkdirs lfs local-version-path)
        shard-loaders      (dofor [s shards]
                                  (future
                                   (load-domain-shard
                                    fs
                                    (:persistence-factory domain-spec)
                                    local-config
                                    (shard-path local-version-path s)
                                    (shard-path
                                     (.versionPath remote-vs remote-version) s)
                                    state))
                                  )]
    (future-values shard-loaders)
    (.succeedVersion local-vs local-version-path)
    (log-message "Successfully loaded domain at " remote-path " to " local-domain-root " with version " remote-version)
    (swap! (:finished-loaders state) + 1)
    (open-domain local-config local-domain-root shards)
    ))

(defn supervise-downloads [supervisor-id amount-domains max-kbs interval-ms ^LoaderState state]
  (let [downloaded-kb (:downloaded-kb state)
        do-download (:do-download state)
        finished-loaders (:finished-loaders state)]
    (loop []
      (when (< @finished-loaders amount-domains)
        (log-message "Download supervisor #" supervisor-id " - "
                     "Finished: " @finished-loaders
                     ", waiting for: " (- amount-domains @finished-loaders))
        (Thread/sleep interval-ms)
        (let [dl-kb @downloaded-kb]
          (if (>= dl-kb max-kbs)
            (do
              (log-message "Download throttle exceeded: " dl-kb " (" max-kbs " allowed) - Waiting for: " interval-ms "ms")
              (reset! do-download false)
              (reset! downloaded-kb 0)
              (recur))
            (do
              (reset! do-download true)
              (reset! downloaded-kb 0)
              (recur))))))))

(defn start-download-supervisor [supervisor-id amount-domains max-kbs ^LoaderState state]
  (let [interval-factor-secs 0.01 ;; check every 0.01s
        interval-ms (int (* interval-factor-secs 1000))
        max-kbs-val (int (* max-kbs interval-factor-secs))]
    (future
      (log-message "Starting download supervisor #" supervisor-id)
      (reset! (:finished-loaders state) 0)
      (reset! sleep-interval interval-ms)
      (if (> max-kbs 0) ;; only monitor if there's an actual download throttle
        (supervise-downloads supervisor-id amount-domains max-kbs-val interval-ms state))
      (log-message "Download supervisor #" supervisor-id " finished"))))