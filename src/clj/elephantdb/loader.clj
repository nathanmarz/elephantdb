(ns elephantdb.loader
  (:import [elephantdb.store VersionedStore])
  (:use [elephantdb util hadoop config]))



(defn- shard-path [domain-version shard]
  (str-path domain-version shard))

;; returns LP
(defn load-domain-shard [fs persistence-factory local-config local-shard-path remote-shard-path]
  ;; TODO: respect the max copy rate
  ;; TODO: do a streaming recursive copy that can be rate limited (rate limited with the other shards...)
  (.copyToLocalFile fs (path remote-shard-path) (path local-shard-path))
  (.openPersistence persistence-factory local-shard-path (persistence-options local-config persistence-factory)))

;; returns a map from shard to LP
(defn load-domain [fs local-config local-domain-root remote-path token shards]
  (let [lfs                (local-filesystem)
        domain-spec        (read-domain-spec fs remote-path)
        local-vs           (VersionedStore. lfs local-domain-root)
        remote-vs          (VersionedStore. fs remote-path)
        remote-version     (.mostRecentVersion remote-vs token)
        local-version-path (.createVersion local-vs remote-version)
        _                  (.mkdirs lfs local-domain-root)
        shard-loaders      (dofor [s shards]
                              [s (future (load-domain-shard fs
                                                            (:persistence-factory domain-spec)
                                                            local-config
                                                            (shard-path local-version-path s)
                                                            (shard-path (.versionPath remote-vs remote-version) s)))])
        ret                (into {} (dofor [[s f] shard-loaders] [s (.get f)]))]
    (.succeedVersion local-vs local-version-path)
    (write-domain-spec! domain-spec lfs local-version-path)
    ret ))
