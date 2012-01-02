(ns elephantdb.keyval.domain
  (:require [jackknife.core :as u]
            [jackknife.logging :as log]))

(defn kv-get
  "key-value server specific get function."
  [domain key]
  (when-let [^KeyValPersistence shard (dom/retrieve-shard domain key)]
    (log/debug (format "Direct get: key %s at shard %s" key shard))
    (u/with-read-lock (:rw-lock domain)
      (.get shard key))))
