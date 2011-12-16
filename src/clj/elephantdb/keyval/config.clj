;; Namespace includes all functions necessary to destructure, read,
;; write and create elephantdb config maps.

(ns elephantdb.keyval.config
  (:use [hadoop-util.core :only (filesystem local-filesystem)])
  (:require [elephantdb.common.config :as conf])
  (:import [elephantdb DomainSpec Utils]
           [elephantdb.persistence Coordinator]))

;; ## Local and Global Configs
;;
;; TODO: Discuss what's included in the local and global
;; configurations.
(def example-global-config
  {:replication 2
   :hosts ["elephant1.server" "elephant2.server" "elephant3.server"]
   :port 3578
   :domains {"graph" "s3n://mybucket/elephantdb/graph"
             "docs"  "/data/docdb"}})

(def DEFAULT-GLOBAL-CONFIG
  {:replication 1
   :port 3578})

(def DEFAULT-LOCAL-CONFIG
  {:max-online-download-rate-kb-s 128
   :update-interval-s 60
   :local-db-conf {}
   :hdfs-conf {}})

(defmulti persistence-str class)

(defmethod persistence-str String
  [persistence] persistence)

(defmethod persistence-str Class
  [persistence] (.getName persistence))

(defmethod persistence-str Coordinator
  [persistence] (.getName (class persistence)))

;; TODO: REMOVE THIS, since it's captured inside of the
;; domain-spec.
;;
;; TODO: Remove persistence options from pallet deploy too.
(defn persistence-options
  [db-conf persistence]
  (get db-conf (persistence-str persistence) {}))

(defn read-local-config
  [local-config-path]
  (merge DEFAULT-LOCAL-CONFIG
         (conf/read-clj-config (local-filesystem)
                          local-config-path)))

(defn read-global-config
  [global-config-path local-config]
  (merge DEFAULT-GLOBAL-CONFIG
         (conf/read-clj-config (filesystem (:blob-conf local-config))
                               global-config-path)))
