(ns elephantdb.common.config
  "Functions for common configuration between elephantDB base and
   interfaces."
  (:use [hadoop-util.core :only (path)])
  (:import [elephantdb DomainSpec Utils]
           [elephantdb.persistence PersistenceCoordinator]))

(def ^{:doc "Example, meant to be ignored."}
  example-global-conf
  {:replication 1
   :port 3578
   :hosts ["localhost"]
   :domains {"graph" "/mybucket/elephantdb/graph"
             "docs"  "/data/docdb"}})

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
  (let [stream (.create fs (path str-path) false)]
    (spit stream conf)))

(defn convert-java-domain-spec [spec]
  {:coordinator  (.getCoordinator spec)
   :shard-scheme (.getShardScheme spec)
   :num-shards   (.getNumShards spec)})

(defn convert-clj-domain-spec [spec-map]
  (DomainSpec. (:coordinator spec-map)
               (:shard-scheme spec-map)
               (:num-shards spec-map)))

(defn read-domain-spec
  "A domain spec is stored with shards in the VersionedStore. Look to
  s3 for an example here."
  [fs path]
  (when-let [spec (DomainSpec/readFromFileSystem fs path)]
    (convert-java-domain-spec spec)))

(defn write-domain-spec! [spec-map fs path]
  (let [spec (convert-clj-domain-spec spec-map)]
    (.writeToFileSystem spec fs path)))
