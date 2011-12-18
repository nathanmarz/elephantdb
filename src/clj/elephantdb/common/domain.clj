(ns elephantdb.common.domain
  (:use [hadoop-util.core :only (local-filesystem path)])
  (:require [elephantdb.common.util :as u]
            [elephantdb.common.shard :as shard]
            [elephantdb.common.loader :as loader]
            [elephantdb.common.thrift :as thrift]
            [elephantdb.common.logging :as log])
  (:import [elephantdb Utils]
           [elephantdb.store DomainStore]))

(defn init-domain-info
  [domain-shard-index]
  {:shard-index    domain-shard-index
   :domain-status (atom (thrift/loading-status))
   :domain-data   (atom nil)})

(defn init-domain-info-map
  " Generates a map with kv pairs of the following form:
    {\"domain-name\" {:shard-index {:hosts-to-shards <map>
                                    :shards-to-hosts <map>}
                      :domain-status <status-atom>
                      :domain-data <data-atom>}}"
  [fs {:keys [domains hosts replication]}]
  (let [domain-shards (shard/shard-domains fs domains hosts replication)]
    (u/update-vals (fn [k _]
                     (-> k domain-shards init-domain-info))
                   domains)))

(defn domain-data
  ([domain-info] @(:domain-data domain-info))
  ([domain-info shard]
     (when-let [domain-data @(:domain-data domain-info)]
       (domain-data shard))))

(defn set-domain-data!
  [rw-lock domain domain-info new-data]
  (let [old-data (domain-data domain-info)]
    (u/with-write-lock rw-lock
      (reset! (:domain-data domain-info) new-data))
    (when old-data
      (loader/close-domain domain old-data))))

(defn domain-status [domain-info]
  @(:domain-status domain-info))

(defn set-domain-status! [domain-info status]
  (reset! (:domain-status domain-info) status))

(defn shard-index [domain-info]
  (:shard-index domain-info))

(defn host-shards
  ([domain-info]
     (host-shards domain-info (u/local-hostname)))
  ([domain-info host]
     (shard/host-shards (:shard-index domain-info) host)))

(defn all-shards
  "Returns Map of domain-name to Set of shards for that domain"
  [domains-info]
  (u/val-map host-shards domains-info))

(defn key-hosts [domain domain-info ^bytes key]
  (shard/key-hosts domain (:shard-index domain-info) key))

(defn key-shard
  "TODO: Remove domain."
  [domain domain-info key]
  (let [index (shard-index domain-info)]
    (shard/key-shard domain key (shard/num-shards index))))

(defn has-data? [store]
  (-> store .mostRecentVersion boolean))

(defn needs-update?
  "Returns true if the remote VersionedStore contains newer data than
  its local copy, false otherwise."
  [local-vs remote-vs]
  (or (not (has-data? local-vs))
      (let [local-version  (.mostRecentVersion local-vs)
            remote-version (.mostRecentVersion remote-vs)]
        (when (and local-version remote-version)
          (< local-version remote-version)))))

(defn local-store
  [local-path remote-vs]
  (DomainStore. (local-filesystem)
                local-path
                (.getSpec remote-vs)))

(defn try-domain-store
  "Attempts to return a DomainStore object from the current path and
  filesystem; if this doesn't exist, returns nil."  [fs domain-path]
  (try (DomainStore. fs domain-path)
       (catch IllegalArgumentException _)))

(defn cleanup-domain!
  [domain-path]
  "Destroys all but the most recent version in the versioned store
  located at `domain-path`."
  (when-let [store (try-domain-store (local-filesystem)
                                     domain-path)]
    (doto store (.cleanup 1))))

(defn cleanup-domains!
  "Destroys every old version for each domain in `domains` located
  inside of `local-dir`. (Each domain directory contains a versioned
  store, capable of being 'cleaned up' in this fashion.)

  If any cleanup operation throws an error, `cleanup-domains!` will
  try to operate on the rest of the domains, and throw a single error
  on completion."
  [domains local-dir]
  (let [error (atom nil)]
    (u/dofor [domain domains :let [domain-path (str (path local-dir domain))]]
             (try (log/info "Purging old versions of domain: " domain)
                  (cleanup-domain! domain-path)
                  (catch Throwable t
                    (log/error t "Error when purging old versions of domain: " domain)
                    (reset! error t))))
    (when-let [e @error]
      (throw e))))
