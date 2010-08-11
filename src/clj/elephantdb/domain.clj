(ns elephantdb.domain
  (:require [elephantdb [shard :as s]])
  (:use [elephantdb util thrift config])
  (:import [elephantdb Utils]))

;; domain-status is an atom around a DomainStatus thrift object
;; domain-data is an atom map from shard to local persistence (or nil if it's not loaded yet)
(defstruct domain-info-struct ::shard-index ::domain-status ::domain-data)

(defn init-domain-info [domain-shard-index]
  (struct domain-info-struct domain-shard-index
                             (atom (loading-status))
                             (atom nil)))

(defn domain-data [domain-info shard]
  (@(::domain-data domain-info) shard))

(defn set-domain-data! [domain-info domain-data]
  (reset! (::domain-data domain-info) domain-data))

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

(defn key-hosts [domain domain-info #^bytes key]
  (s/key-hosts domain (::shard-index domain-info) key))

(defn key-shard [domain domain-info key]
  (let [index (shard-index domain-info)]
    (s/key-shard domain key (s/num-shards index))
    ))
