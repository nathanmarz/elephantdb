(ns elephantdb.loader
  (:import [elephantdb.store VersionedStore])
  (:use [elephantdb util hadoop config log]))



(defn- shard-path [domain-version shard]
  (str-path domain-version shard))

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
    (let [ret (.openPersistence persistence-factory local-shard-path (persistence-options local-config persistence-factory))]
      (log-message "Opened LP " local-shard-path)
      ret ))


;; returns a map from shard to LP
(defn load-domain [fs local-config local-domain-root remote-path token shards]
  (log-message "Loading domain at " remote-path " to " local-domain-root)
  (let [lfs                (local-filesystem)
        domain-spec        (read-domain-spec fs remote-path)
        local-vs           (VersionedStore. lfs local-domain-root)
        remote-vs          (VersionedStore. fs remote-path)
        remote-version     (.mostRecentVersion remote-vs token)
        local-version-path (.createVersion local-vs remote-version)
        _                  (.mkdirs lfs local-domain-root)
        shard-loaders      (dofor [s shards]
                             [s (future
                                  (load-domain-shard
                                    fs
                                    (:persistence-factory domain-spec)
                                    local-config
                                    (shard-path local-version-path s)
                                    (shard-path (.versionPath remote-vs remote-version) s)))])
        ret                (into {} (dofor [[s f] shard-loaders] [s (.get f)]))]
    (.succeedVersion local-vs local-version-path)
    (write-domain-spec! domain-spec lfs local-version-path)
    (log-message "Successfully loaded domain at " remote-path " to " local-domain-root " with version " remote-version)
    ret ))
