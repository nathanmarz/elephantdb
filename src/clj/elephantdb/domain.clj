(ns elephantdb.domain
  (:use elephantdb.common.util
        [elephantdb thrift config]
        [elephantdb.loader :only (close-domain)]
        [hadoop-util.core :only (local-filesystem)])
  (:require [elephantdb.common.shard :as s])
  (:import [elephantdb Utils]))

(defn init-domain-info
  [domain-shard-index]
  {::shard-index    domain-shard-index
   ::domain-status (atom (loading-status))
   ::domain-data   (atom nil)})

(defn domain-data
  ([domain-info] @(::domain-data domain-info))
  ([domain-info shard]
     (when-let [domain-data @(::domain-data domain-info)]
       (domain-data shard))))

(defn set-domain-data!
  [rw-lock domain domain-info new-data]
  (let [old-data (domain-data domain-info)]
    (with-write-lock rw-lock
      (reset! (::domain-data domain-info) new-data))
    (when old-data
      (close-domain domain old-data))))

(defn domain-status [domain-info]
  @(::domain-status domain-info))

(defn set-domain-status! [domain-info status]
  (reset! (::domain-status domain-info) status))

(defn shard-index [domain-info]
  (::shard-index domain-info))

(defn host-shards
  ([domain-info]
     (host-shards domain-info (local-hostname)))
  ([domain-info host]
     (s/host-shards (::shard-index domain-info) host)))

(defn all-shards
  "Returns Map of domain-name to Set of shards for that domain"
  [domains-info]
  (val-map host-shards domains-info))

(defn key-hosts [domain domain-info #^bytes key]
  (s/key-hosts domain (::shard-index domain-info) key))

(defn key-shard [domain domain-info key]
  (let [index (shard-index domain-info)]
    (s/key-shard domain key (s/num-shards index))))
