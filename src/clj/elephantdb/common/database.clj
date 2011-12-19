(ns elephantdb.common.database
  (:require [hadoop-util.core :as h]
            [elephantdb.common.util :as u]
            [elephantdb.common.thrift :as thrift]
            [elephantdb.common.logging :as log]))

;; ## Database Manipulation Functions

(defn domain-path
  "Returns the root path that should be used for a domain with the
  supplied name located within the supplied elephantdb root
  directory."
  [local-root domain-name]
  (str local-root "/" domain-name))

(defn domain-get
  "Retrieves the requested domain (by name) from the supplied
  database."
  [{:keys [domains]} domain-name]
  (or (get domains domain-name)
      (thrift/domain-not-found-ex domain-name)))

(defn purge-unused-domains!
  "Walks through the supplied local directory, recursively deleting
   all directories with names that aren't present in the supplied
   `domains`."
  [{:keys [local-dir domains]}]
  (letfn [(domain? [path]
            (and (.isDirectory path)
                 (not (contains? (into #{} (keys domains))
                                 (.getName path)))))]
    (u/dofor [domain-path (-> local-root h/mk-local-path .listFiles)
              :when (domain? domain-path)]
             (log/info "Destroying un-served domain at: " domain-path)
             (h/delete (h/local-filesystem)
                       (.getPath domain-path)
                       true))))

;; ## Database Creation
;;
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
                      :status <status-atom>
                      :shard-index 'shard-index
                      :domain-data (atom {:version 123534534
                                          :shards {1 <persistence>
                                                   3 <persistence>}})}

             "docs" {:remote-store <remote-domain-store>
                     :local-store <local-domain-store>
                     :serializer <serializer>
                     :status <status-atom>
                     :shard-index 'shard-index
                     :domain-data (atom {:version 123534534
                                         :shards {1 <persistence>
                                                  3 <persistence>}})}}})
