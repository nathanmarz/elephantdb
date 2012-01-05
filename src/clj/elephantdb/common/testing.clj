(ns elephantdb.common.testing
  (:require [hadoop-util.core :as h]
            [hadoop-util.test :as t]
            [jackknife.core :as u]
            [jackknife.logging :as log]
            [elephantdb.common.domain :as dom])
  (:import [java.util Arrays]
           [elephantdb.store DomainStore]
           [elephantdb.persistence ShardSet]
           [elephantdb Utils DomainSpec]
           [org.apache.hadoop.io IntWritable]
           [elephantdb.hadoop ElephantOutputFormat
            ElephantOutputFormat$Args LocalElephantManager]
           [org.apache.hadoop.mapred JobConf]))

;; ## Domain Testing

(defn specs-match?
  "Returns true of the specs of all supplied DomainStores match, false
  otherwise."
  [& stores]
  (apply = (map #(.getSpec %)
                stores)))

;; `existing-shard-set` is good for testing, but we don't really need
;; it in a working production system (since the domain knows what
;; shards should be holding.)

(defn existing-shard-set
  "Returns a sequence of all shards present on the fileystem for the
  supplied store and version. Useful for testing."
  [store version]
  (let [num-shards (.. store getSpec getNumShards)
        filesystem (.getFileSystem store)]
    (->> (range num-shards)
         (filter (fn [idx]
                   (->> (.shardPath store idx version)
                        (.exists filesystem))))
         (into #{}))))

(defn version-well-formed?
  "Does the supplied version within the domain have all of its
  shards?"
  [domain version]
  (= (dom/shard-set domain version)
     (-> (.localStore domain)
         (existing-shard-set version))))

;; ## Byte Array Testing

(defn barr [& xs]
  (when xs
    (byte-array (map byte xs))))

(defn barr=
  ([x] true)
  ([^bytes x ^bytes y] (java.util.Arrays/equals x y))
  ([x y & more]
     (if (barr= x y)
       (if (next more)
         (recur y (first more) (next more))
         (barr= y (first more)))
       false)))

(defn count= [& colls]
  (apply = (map count colls)))

(defn barrs= [& arrs]
  (and (count= arrs)
       (every? identity
               (apply map (fn [& vals]
                            (or (every? nil? vals)
                                (apply barr= vals)))
                      arrs))))

;; ## Example Specs

(defn berkeley-spec
  "Returns a DomainSpec initialized with BerkeleyDB, a HashMod scheme
  and the supplied shard-count."
  [shard-count]
  (DomainSpec. "elephantdb.persistence.JavaBerkDB"
               "elephantdb.partition.HashModScheme"
               shard-count))

;; ## Population Helpers

(defn elephant-writer
  [spec output-dir tmp-dir & {:keys [indexer update-dir]}]
  (let [args  (ElephantOutputFormat$Args. spec output-dir)]
    (when indexer
      (set! (.indexer args) indexer))
    (when update-dir
      (set! (.updateDirHdfs args) update-dir))
    (.getRecordWriter (ElephantOutputFormat.)
                      nil
                      (doto (JobConf.)
                        (Utils/setObject ElephantOutputFormat/ARGS_CONF args)
                        (LocalElephantManager/setTmpDirs [tmp-dir]))
                      nil
                      nil)))

(defn create-version
  [vs & {:keys [version force?]}]
  (if version
    (do (when (.hasVersion vs version)
          (.deleteVersion vs version))
        (.createVersion vs version))
    (.createVersion vs)))

(defn create-domain!
  "Accepts a domain-spec, a path and a pre-sharded map of documents
  and creates a pre-filled domain at the supplied path. create-domain!
  supports optional keyword arguments:

  :version -- the version used to create the domain."
  [path spec sharded-docs & {:keys [version]}]
  (let [store        (DomainStore. path spec)
        version-path (create-version store
                                     :version version
                                     :force? true)
        version      (.parseVersion store version-path)]
    (doseq [[idx doc-seq] sharded-docs]
      (with-open [shard (do (.createShard store idx version)
                            (.openShardForAppend store idx version))]
        (doseq [doc doc-seq]
          (.index shard doc))))
    (.succeedVersion store version-path)))

;; ## Hadoop Writer-based Additions

(defn presharded->writer!
  "Accepts a domain-spec, a path and a pre-sharded map of
  documents. sharded-docs is a map of shard->doc-seq."
  [spec path sharded-docs & {:keys [version]}]
  (t/with-local-tmp [_ tmp]
    (let [store         (DomainStore. path spec)
          version-path  (create-version store
                                        :version version
                                        :force? true)]
      (with-open [writer (elephant-writer spec path tmp)]
        (doseq [[idx doc-seq] sharded-docs
                doc           doc-seq]
          (.write writer (IntWritable. idx) doc)))
      (.succeedVersion store version-path))))

(defn mk-presharded->writer!
  "Accepts a function that translates between the items in the
  sequence and a document suitable for indexing into the supplied
  domain and returns a function taht acts just like
  presharded->writer!"
  [converter-fn]
  (fn [spec path sharded-items & {:keys [version]}]
    (let [sharded-docs (u/val-map converter-fn sharded-items)]
      (presharded->writer! spec path sharded-docs :version version))))

;; ## Shard Mocking

(defn key->shard [scheme key shard-count]
  (.shardIndex scheme key shard-count))

(defmacro with-sharding-fn [shard-fn & body]
  `(if ~shard-fn
     (with-redefs [key->shard ~shard-fn]
       ~@body)
     (do ~@body)))

(defn shard-index
  "Accepts a DomainSpec and a sequence of shard keys and returns a
  sequence of shard indexes."
  [spec key]
  (let [shard-count (.getNumShards spec)
        scheme      (.getShardScheme spec)]
    (key->shard scheme key shard-count)))

(defn shard-docs
  "Accepts a DomainSpec and a sequence of <shard-key, document> pairs
  and returns a map of shard->doc-seq. A custom sharding function can
  be supplied with the `:shard-fn` keyword."
  [spec doc-seq & {:keys [shard-fn]
                   :or {shard-fn key->shard}}]
  (with-sharding-fn shard-fn
    (->> doc-seq
         (group-by (fn [[idx _]] (shard-index spec idx)))
         (u/val-map (partial map second)))))

(defn unsharded->writer!
  "doc-seq is a sequence of <shard-key, document> pairs. Otherwise
  this is the same as presharded->writer."
  [spec path doc-seq & {:keys [version shard-fn]}]
  (let [sharded-docs (shard-docs doc-seq :shard-fn shard-fn)]
    (presharded->writer! spec path sharded-docs :version version)))

;; ## Generic Extensions for other persistences

(defn mk-sharder
  "Accepts a function meant to accept some document or thing and
  return a pair of <shard-key, document> suitable for indexing into a
  Persistence. Returns a function similar to `shard-docs`."
  [converter-fn]
  (fn [spec item-seq & {:keys [shard-fn]}]
    (let [doc-seq (map converter-fn item-seq)]
      (shard-docs spec doc-seq :shard-fn shard-fn))))

(defn mk-unsharded->writer!
  "Accepts a sharding function of the same type passed in to
  `mk-sharder`. `sharder-fn` must accept one of the items in the
  document seq and return a pair of <shard-key, document> suitable for
  indexing into ElephantDB.

  Returns a function that acts like unsharded->writer!, but accepts a
  sequence of the inputs to `sharder-fn` vs a sequence of <shard-key,
  doc> pairs."
  [converter-fn]
  (fn [spec path item-seq & {:keys [version shard-fn]}]
    (let [item-sharder (mk-sharder converter-fn)
          sharded-docs (item-sharder item-seq :shard-fn shard-fn)]
      (presharded->writer! spec path sharded-docs :version version))))

