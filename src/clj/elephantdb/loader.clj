(ns elephantdb.loader
  (:import [elephantdb.store DomainStore])
  (:use [elephantdb util hadoop config log]))



(defn- shard-path [domain-version shard]
  (str-path domain-version shard))

(defn open-domain-shard [persistence-factory local-config local-shard-path]
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
    (into {} (dofor [[s f] future-lps] [s (.get f)]))
    ))

;; returns LP
(defn load-domain-shard [fs persistence-factory local-config local-shard-path remote-shard-path]
  ;; TODO: respect the max copy rate
  ;; TODO: do a streaming recursive copy that can be rate limited (rate limited with the other shards...)
  (if (.exists fs (path remote-shard-path))
    (do
      (log-message "Copying " remote-shard-path " to " local-shard-path)
      (.copyToLocalFile fs (path remote-shard-path) (path local-shard-path))
      (log-message "Copied " remote-shard-path " to " local-shard-path)
      (log-message "Opening LP " local-shard-path))
    (do
      (log-message "Shard " remote-shard-path " did not exist. Creating empty LP")
      (.close (.createPersistence persistence-factory local-shard-path (persistence-options local-config persistence-factory)))))
    )

;; returns a map from shard to LP
(defn load-domain [fs local-config local-domain-root remote-path shards]
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
                                     (.versionPath remote-vs remote-version) s)))
                                  )]
    (future-values shard-loaders)
    (.succeedVersion local-vs local-version-path)
    (log-message "Successfully loaded domain at " remote-path " to " local-domain-root " with version " remote-version)
    (open-domain local-config local-domain-root shards)
    ))
