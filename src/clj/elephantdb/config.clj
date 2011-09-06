(ns elephantdb.config
  "Point of this namespace is to contain all io, settings, etc around
configs."
  (:use elephantdb.hadoop)
  (:require [clojure.contrib.duck-streams :as d])
  (:import [elephantdb DomainSpec Utils]
           [elephantdb.persistence LocalPersistenceFactory]))

;; We need to get everything in here, and start to remove token

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

(defn global-config-cache-path
  "Returns the default location for the cached global config on each
  edb machine."
  ([local-config]
     (global-config-cache-path local-config "GLOBAL-CONF"))
  ([local-config filename]
     (str-path (:local-dir local-config)
               (str filename ".clj"))))

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
  filesytem."
  [conf fs str-path]
  {:pre [(map? conf)]}
  (with-open [w (d/writer (.create fs (path str-path) false))]
    (.print w conf)))

(defn read-cached-global-config
  "Configs are read/written this way b/c hadoop forks the process
  using the write-clj-config! method"
  [local-config]
  (let [p (global-config-cache-path local-config)]
    (when (.exists (d/file-str p))
      (read-string (slurp p)))))

(defn cache-global-config!
  "Writes global config to cache-path specified in the local config
  map."
  [local-config global-config]
  (d/spit (global-config-cache-path local-config)
          global-config))

(defn cache? [global-config token]
  (= (:token global-config) token))

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

(defn persistence-options [local-config persistence]
  (get (:local-db-conf local-config)
       (persistence-str persistence)
       {}))

(defn read-local-config [local-config-path]
  (merge DEFAULT-LOCAL-CONFIG
         (read-clj-config (local-filesystem)
                          local-config-path)))

(defn read-global-config
  [global-config-path local-config]
  (merge DEFAULT-GLOBAL-CONFIG
         (read-clj-config (filesystem (:hdfs-conf local-config))
                          global-config-path)))
