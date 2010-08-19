(ns elephantdb.config
  (:require [clojure.contrib [duck-streams :as d]])
  (:use [elephantdb hadoop])
  (:import [elephantdb DomainSpec Utils])
  (:import [elephantdb.persistence LocalPersistenceFactory]))


; { :replication 2
;   :hosts ["elephant1.server" "elephant2.server" "elephant3.server"]
;   :port 3578
;   :domains {"graph" "s3n://mybucket/elephantdb/graph"
;             "docs"  "/data/docdb"
;             }
; }

(def DEFAULT-GLOBAL-CONFIG
     {
      :replication 1
      :port 3578
      })

(def DEFAULT-LOCAL-CONFIG
     {
      :max-online-download-rate-kb-s 128
      :local-db-conf {}
      :hdfs-conf {}
      })

(defstruct domain-spec-struct :persistence-factory :num-shards)

(defn local-global-config-cache [local-config]
  (str-path (:local-dir local-config) "GLOBAL-CONF.clj"))

;; TODO: do an eval? any security risks with that?
(defn read-clj-config [fs str-path]
  (if-not (.exists fs (path str-path))
    nil
    (with-in-str (Utils/convertStreamToString
                  (.open fs (path str-path)))
      (read))))

(defn write-clj-config! [conf fs str-path]
  (with-open [w (d/writer (.create fs (path str-path) false))]
    (.print w conf)
    ))

(defn read-cached-global-config [local-config]
  (let [p (local-global-config-cache local-config)]
    (read-clj-config (local-filesystem) p)
    ))

(defn cache-global-config! [local-config global-config]
  (let [p (local-global-config-cache local-config)]
    (write-clj-config! global-config (local-filesystem) p)
    ))

(defn cache? [global-config token]
  (= (:token global-config) token))

(defn convert-java-domain-spec [spec]
  (struct domain-spec-struct (.getLPFactory spec) (.getNumShards spec)))

(defn convert-clj-domain-spec [spec-map]
  (DomainSpec. (:persistence-factory spec-map) (:num-shards spec-map)))

(defn read-domain-spec [fs path]
  (let [spec (DomainSpec/readFromFileSystem fs path)]
    (if-not spec
      nil
      (convert-java-domain-spec spec))))

(defn write-domain-spec! [spec-map fs path]
  (let [spec (convert-clj-domain-spec spec-map)]
    (.writeToFileSystem spec fs path)))

(defmulti persistence-str class)
(defmethod persistence-str String [persistence] persistence)
(defmethod persistence-str Class [persistence] (.getName persistence))
(defmethod persistence-str LocalPersistenceFactory [persistence] (.getName (class persistence)))

(defn persistence-options [local-config persistence]
  (if-let [local-db-conf (:local-db-conf local-config)]
    (get local-db-conf (persistence-str persistence {}))
    {}
    ))
