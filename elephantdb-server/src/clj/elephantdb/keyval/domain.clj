(ns elephantdb.keyval.domain
  (:require [jackknife.core :as u]
            [jackknife.logging :as log]
            [elephantdb.common.domain :as dom]
            [elephantdb.common.status :as s])
  (:import [elephantdb.persistence KeyValPersistence]
           [elephantdb.document KeyValDocument]
           [elephantdb.common.status IStateful]
           [elephantdb.persistence Shutdownable]
           [elephantdb.common.domain Domain]))

(defn kv-count
  "Returns the count of kv pairs in the supplied domain."
  [^Domain domain]
  (count (seq domain)))

(defn trim-hosts
    "Used within a multi-get's loop. Accepts a sequence of hosts + a
    sequence of hosts known to be bad, filters the bad hosts and drops
    the first one."
    [host-seq bad-hosts]
    (remove (set bad-hosts)
            (rest host-seq)))

(defn kv-get
  "key-value server specific get function."
  [^Domain domain ^bytes key]
  (when-let [^KeyValPersistence shard (dom/retrieve-shard domain key)]
    (log/debug (format "Direct get: key %s at shard %s" key shard))
    (u/with-read-lock (.rwLock domain)
      (try
        (.get shard key)
        (catch Exception e
          (log/error "kv-get failed: " e)
          (throw e))))))

(defn to-map
  "Returns a persistent map containing all kv pairs in the supplied
  domain."
  [^Domain domain]
  (into {} (for [^KeyValDocument doc (seq domain)]
             [(.key doc) (.value doc)])))

(defn index-keys
  "For the supplied domain and sequence of keys, returns a sequence of
  maps with the following keys:

  :key   - the key.
  :hosts - A sequence of hosts at which the key can be found.
  :all-hosts - the same list as hosts, at first. As gets are attempted
  on each key, the recursion will drop names from `hosts` and keep
  them around in `:all-hosts` for error reporting."
  [^Domain domain key-seq]
  (for [key key-seq
        :let [hosts (dom/prioritize-hosts domain key)]]
    {:key key, :hosts hosts, :all-hosts hosts}))

