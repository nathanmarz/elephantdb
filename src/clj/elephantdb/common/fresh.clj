(ns elephantdb.common.fresh
  (:require [hadoop-util.core :as h]
            [elephantdb.common.util :as u]
            [elephantdb.common.status :as stat]
            [elephantdb.common.shard :as shard])
  (:import [elephantdb.store DomainStore]))

;; A fresh start.

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

(defn specs-match?
  "Returns true of the specs of all supplied DomainStores match, false
  otherwise."
  [& stores]
  (apply = (map (fn [^DomainStore x] (.getSpec x))
                stores)))

(defn local-store
  [local-path remote-vs]
  (DomainStore. local-path (.getSpec remote-vs)))

(defn version-seq [store]
  (into [] (.getAllVersions store)))

(defn build-domain
  [local-root hdfs-conf remote-path hosts replication]
  (let [rfs (h/filesystem hdfs-conf)
        remote-store  (DomainStore. rfs remote-path)
        local-store   (local-store local-root remote-store)
        index (generate-index hosts (-> local-store .getSpec .getNumShards))]
    {:remote-store remote-store
     :local-store  local-store
     :serializer   (-> local-store .getSpec .getCoordinator .getKryoBuffer)
     :status       (atom (KeywordStatus. :loading))
     :domain-data (atom nil)
     :shard-index (atom index)

     ;; These two shouldn't be inside atoms; they should instead be
     ;; specified with a protocol and implemented through functions.
     :current-version (atom (.mostRecentVersion local-store))
     :version-map (atom (zipmap (version-seq local-store)
                                (repeat {:status :closed})))}))

;; ## Examples
;;
;; The configuration was traditionally split up into global and
;;local. We need to have a global config to allow all machines to
;;access the information therein. That's a deploy issue, I'm
;;convinced; the actual access to either configuration should occur in
;;the same fashion.
;;
;; Here's an example configuration:

(def example-config
  {:replication 1
   :port 3578
   :download-cap 1024
   :local-root "/Users/sritchie/Desktop/domainroot"
   :hosts ["localhost"]
   :domains {"graph" "/mybucket/elephantdb/graph"
             "docs"  "/data/docdb"}
   :hdfs-conf {"fs.default.name"
               "hdfs://hadoop-devel-nn.local.twitter.com:8020"}
   :blob-conf {"fs.default.name"
               "hdfs://hadoop-devel-nn.local.twitter.com:8020"}})



(defn domain-path
  "Returns the root path that should be used for a domain with the
  supplied name located within the supplied elephantdb root
  directory."
  [local-root domain-name]
  (str local-root "/" domain-name))

;; A database ends up being the initial configuration map with much
;; more detail about the individual domains. The build-database
;; function merges in the proper domain-maps for each listed domain.

(defn build-database
  [{:keys [domains hosts replication local-root hdfs-conf] :as conf-map}]
  (assoc conf-map
    :domains (u/update-vals
              domains
              (fn [domain-name remote-path]
                (let [local-path (domain-path local-root domain-name)]
                  (build-domain
                   local-root hdfs-conf remote-path hosts replication))))))

;; A full database ends up looking something like the commented out
;; block below. Previously, a large number of functions would try and
;; update all databases at once. With Clojure's concurrency mechanisms
;; we can treat each domain as its own thing and dispatch futures to
;; take care of each in turn.

(comment
  {:replication 1
   :port 3578
   :download-cap 1024
   :local-root "/Users/sritchie/Desktop/domainroot"
   :hosts ["localhost"]
   
   :hdfs-conf {"fs.default.name"
               "hdfs://hadoop-devel-nn.local.twitter.com:8020"}
   :blob-conf {"fs.default.name"
               "hdfs://hadoop-devel-nn.local.twitter.com:8020"}
   :domains {"graph" {:remote-store <remote-domain-store>
                      :local-store <local-domain-store>
                      :serializer <serializer>
                      :status     <status-atom>
                      :domain-data (atom nil)
                      :shard-index 'shard-index
                      :current-version (atom some-number)
                      :version-map  (atom [1 2 3 4])}

             "docs" {:remote-store <remote-domain-store>
                     :local-store <local-domain-store>
                     :serializer <serializer>
                     :status     <status-atom>
                     :domain-data (atom nil)
                     :shard-index 'shard-index
                     :current-version (atom some-number)
                     :version-map  (atom [1 2 3 4])}}})
