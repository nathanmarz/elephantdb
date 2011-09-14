;; Namespace includes all functions necessary to destructure, read,
;; write and create elephantdb config maps.

(ns elephantdb.config
  (:use elephantdb.hadoop)
  (:require [clojure.contrib.duck-streams :as d])
  (:import [elephantdb DomainSpec Utils]
           [elephantdb.persistence LocalPersistenceFactory]))

;; ## Local and Global Configs
;;
;; TODO: Discuss what's included in the local and global
;; configurations.

;; { :replication 2
;;   :hosts ["elephant1.server" "elephant2.server" "elephant3.server"]
;;   :port 3578
;;   :domains {"graph" "s3n://mybucket/elephantdb/graph"
;;             "docs"  "/data/docdb"
;;             }
;; }

(def DEFAULT-GLOBAL-CONFIG
  {:replication 1
   :port 3578})

(def DEFAULT-LOCAL-CONFIG
  {:max-online-download-rate-kb-s 128
   :update-interval-s 60
   :local-db-conf {}
   :hdfs-conf {}})

(defstruct domain-spec-struct :persistence-factory :num-shards)

(defn read-clj-config
  "Reads a clojure map from the specified path, on the specified
  filesystem. Example usage:

  (read-clj-config (local-filesystem) \"/path/to/local-config.clj\")"
  [fs str-path]
  (let [p (path str-path)]
    (when (.exists fs p)
      (read-string (Utils/convertStreamToString
                    (.open fs p))))))

(defn write-clj-config!
  "Writes the supplied `conf` map to `str-path` on the supplied
  filesystem."
  [conf fs str-path]
  {:pre [(map? conf)]}
  (with-open [w (d/writer (.create fs (path str-path) false))]
    (.print w conf)))

(defn convert-java-domain-spec [spec]
  (struct domain-spec-struct
          (.getLPFactory spec)
          (.getNumShards spec)))

(defn convert-clj-domain-spec [spec-map]
  (DomainSpec. (:persistence-factory spec-map)
               (:num-shards spec-map)))

(defn read-domain-spec [fs path]
  (when-let [spec (DomainSpec/readFromFileSystem fs path)]
    (convert-java-domain-spec spec)))

(defn write-domain-spec! [spec-map fs path]
  (let [spec (convert-clj-domain-spec spec-map)]
    (.writeToFileSystem spec fs path)))

(defmulti persistence-str class)
(defmethod persistence-str String [persistence] persistence)
(defmethod persistence-str Class [persistence] (.getName persistence))
(defmethod persistence-str LocalPersistenceFactory [persistence] (.getName (class persistence)))

(defn persistence-options
  [db-conf persistence]
  (get db-conf (persistence-str persistence) {}))

(defn read-local-config
  [local-config-path]
  (merge DEFAULT-LOCAL-CONFIG
         (read-clj-config (local-filesystem)
                          local-config-path)))

(defn read-global-config
  [global-config-path local-config]
  (merge DEFAULT-GLOBAL-CONFIG
         (read-clj-config (filesystem (:hdfs-conf local-config))
                          global-config-path)))
