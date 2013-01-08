(ns elephantdb.common.config
  "Functions for common configuration between elephantDB base and
   interfaces."
  (:require [hadoop-util.core :as h])
  (:import [elephantdb DomainSpec Utils]))

;; ## Configuration
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
   :download-rate-limit 1024
   :local-root "/Users/sritchie/Desktop/domainroot"
   :hosts ["localhost"]
   :domains {"graph" "/mybucket/elephantdb/graph"
             "docs"  "/data/docdb"}
   :hdfs-conf {"fs.default.name"
               "hdfs://hadoop-devel-nn.local.twitter.com:8020"}
   :blob-conf {"fs.default.name"
               "hdfs://hadoop-devel-nn.local.twitter.com:8020"}})

;; ## Local and Global Configs

(defn read-clj-config
  "Reads a clojure map from the specified path, on the specified
  filesystem. Example usage:

  (read-clj-config (local-filesystem)
                  \"/path/to/local-config.clj\")"
  [fs str-path]
  (let [p (h/path str-path)]
    (when (.exists fs p)
      (read-string (Utils/convertStreamToString
                    (.open fs p))))))


(def example-global-config
  {:replication 2
   :port 3578
   :hosts ["elephant1.server" "elephant2.server" "elephant3.server"]
   :domains {"graph" "s3n://mybucket/elephantdb/graph"
             "docs"  "/data/docdb"}})

(def DEFAULT-GLOBAL-CONFIG
  {:replication 1
   :port 3578})

(def DEFAULT-LOCAL-CONFIG
  {:download-rate-limit 128
   :update-interval-s   60})

(defn read-local-config
  "Reads in the local configuration file from the supplied local
  configuration path. Path must point to a file containing a clojure
  map."
  [local-config-path]
  (merge DEFAULT-LOCAL-CONFIG
         (read-clj-config (h/local-filesystem)
                          local-config-path)))

(defn read-global-config
  "Reads in the global configuration file from the supplied hdfs
  configuration path. Path must point to a file containing a clojure
  map. The HDFS credentials are pulled from supplied the local
  configuration map."
  [global-config-path local-config]
  (merge DEFAULT-GLOBAL-CONFIG
         (read-clj-config (h/filesystem (:blob-conf local-config))
                          global-config-path)))

;; ## TODO: Move to deployer or testing namespace. We don't end using
;; ## this in EDB 2.0.

(defn write-clj-config!
  "Writes the supplied `conf` map to `str-path` on the supplied
  filesystem."
  [conf fs path-str]
  {:pre [(map? conf)]}
  (let [stream (.create fs (h/path path-str) false)]
    (spit stream conf)))

(defn convert-java-domain-spec [^DomainSpec spec]
  {:coordinator  (.getCoordinator spec)
   :shard-scheme (.getShardScheme spec)
   :num-shards   (.getNumShards spec)})

(defn convert-clj-domain-spec
  [{:keys [coordinator shard-scheme num-shards]}]
  {:pre [(and coordinator shard-scheme num-shards)]}
  (DomainSpec. coordinator shard-scheme num-shards))

(defn read-domain-spec
  "A domain spec is stored with shards in the VersionedStore. Look to
  s3 for an example here."
  [fs path]
  (when-let [spec (DomainSpec/readFromFileSystem fs path)]
    (convert-java-domain-spec spec)))

(defn write-domain-spec! [spec-map fs path]
  (let [spec (convert-clj-domain-spec spec-map)]
    (.writeToFileSystem spec fs path)))
