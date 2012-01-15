(ns elephantdb.test.common
  (:require [hadoop-util.core :as h]
            [hadoop-util.test :as t]
            [jackknife.core :as u]
            [jackknife.logging :as log]
            [elephantdb.common.domain :as dom]
            [elephantdb.common.database :as db])
  (:import [java.nio ByteBuffer]
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

(defn barrs=
  [& barr-seqs]
  (and (apply count= barr-seqs)
       (every? true? (apply map barr= barr-seqs))))

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

;; ## Name Generation

(defn uuid-stream
  "Generates an infinite stream of UUIDs."
  []
  (repeatedly #(t/uuid)))

(defn domain-path-map
  "Accepts a sequence of domain names and returns a map of domain-name
  to a path nested within root."
  [root name-seq]
  (->> (uuid-stream)
       (map (partial str root "/"))
       (interleave name-seq)
       (apply hash-map)))

;; ## Hadoop Writer-based Additions

(defn create-domain-with-writer!
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

;; ## Local Creation

(defn create-domain!
  "Accepts a domain-spec, a path and a pre-sharded map of documents
  and creates a pre-filled domain at the supplied path. create-domain!
  supports optional keyword arguments:

  :version -- the version used to create the domain."
  [spec path sharded-docs & {:keys [version]}]
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

(defn mk-domain-creator
  "Accepts a function that translates between the items in the
  sequence and a document suitable for indexing into the supplied
  domain and returns a function taht acts just like
  create-domain!"
  [converter-fn]
  (fn [spec path sharded-items & {:keys [version]}]
    (let [sharded-docs (u/val-map (partial map converter-fn)
                                  sharded-items)]
      (create-domain! spec path sharded-docs :version version))))

;; ## Shard Mocking

(defn test-key->shard [scheme key shard-count]
  (.shardIndex scheme key shard-count))

(defmacro with-sharding-fn [shard-fn & body]
  `(if ~shard-fn
     (do (with-redefs [test-key->shard ~shard-fn]
        ~@body))
     (do ~@body)))

(defn shard-index
  "Accepts a DomainSpec and a sequence of shard keys and returns a
  sequence of shard indexes."
  [spec key]
  (let [shard-count (.getNumShards spec)
        scheme      (.getShardScheme spec)]
    (test-key->shard scheme key shard-count)))

(defn shard-docs
  "Accepts a DomainSpec and a sequence of <shard-key, document> pairs
  and returns a map of shard->doc-seq. A custom sharding function can
  be supplied with the `:shard-fn` keyword."
  [spec doc-seq & {:keys [shard-fn]}]
  (with-sharding-fn shard-fn
    (->> doc-seq
         (group-by (fn [[idx _]] (shard-index spec idx)))
         (u/val-map (partial map second)))))

;; And again, we're at a function that's very similar to create-domain
;; from above.

(defn create-unsharded-domain!
  "doc-seq is a sequence of <shard-key, document> pairs vs a map of
  shard->document-seq. Otherwise this is the same as create-domain!."
  [spec path doc-seq & {:keys [version shard-fn]}]
  (let [sharded-docs (shard-docs spec doc-seq :shard-fn shard-fn)]
    (create-domain! spec path sharded-docs :version version)))

(defmacro with-basic-domain
  "Used as:

   (with-basic-domain [my-domain domain-spec
                       [doc-1 doc-2...]
                       :version 5
                       :shard-fn (constantly 10)]
          (seq my-domain))

  A domain with the supplied domain-spec is bound to `sym` inside the
  body of `with-domain`."
  [[sym spec doc-seq & {:keys [version shard-fn]}] & body]
  `(log/with-log-level :off
     (t/with-fs-tmp [fs# path#]
       (create-unsharded-domain! ~spec path# ~doc-seq
                                 :version ~version
                                 :shard-fn ~shard-fn)
       (let [~sym (dom/build-domain path#)]
         ~@body))))

;; ## Generic Extensions for other persistences

(defn mk-sharder
  "Accepts a function meant to accept some document or thing and
  return a pair of <shard-key, document> suitable for indexing into a
  Persistence. Returns a function similar to `shard-docs`."
  [converter-fn]
  (fn [spec item-seq & {:keys [shard-fn]}]
    (let [doc-seq (map converter-fn item-seq)]
      (shard-docs spec doc-seq :shard-fn shard-fn))))

(defn mk-unsharded-domain-creator
  "Accepts a sharding function of the same type passed in to
  `mk-sharder`. `converter-fn` must accept one of the items in the
  document seq and return a pair of <shard-key, document> suitable for
  indexing into ElephantDB.

  Returns a function that acts like create-unsharded-domain!, but
  accepts a sequence of the inputs to `sharder-fn` vs a sequence of
  <shard-key, doc> pairs.

  You can also supply the following keyword arguments:

  :version  -- domain version.
  :shard-fn -- fn from shardkey->shard index."
  [converter-fn]
  (fn [spec path item-seq & {:keys [version shard-fn]}]
    (let [item-sharder (mk-sharder converter-fn)
          sharded-docs (item-sharder spec item-seq :shard-fn shard-fn)]
      (create-domain! spec path sharded-docs :version version))))


;; ## Test Database
;;
;; TODO:
;;
;; * Options for loaded vs unloaded database
;; * Sharded vs unsharded

(defn build-test-db
  "Accepts a local and remote root directory and a map of domain name
  -> presharded map of documents (a map with <shard-idx, doc-seq>
  pairs) and returns a Database object.

  Accepts an optional configuration map as a final argument."
  [local-root remote-root domain-map & [conf-map]]
  {:pre [(or (not conf-map) (map? conf-map))]}
  (let [spec     (berkeley-spec 4)
        path-map (domain-path-map remote-root (keys domain-map))]
    (doseq [[domain-name remote-path] path-map]
      (create-domain! spec remote-path (domain-map domain-name)))
    (db/build-database (merge conf-map
                              {:local-root local-root
                               :domains    path-map}))))

(defmacro with-database
  "Generates a database with the supplied sequence of domain names and
  binds it to `sym` inside of the form."
  [[sym domain-seq] & body]
  `(t/with-fs-tmp [fs# remote#]
     (t/with-local-tmp [lfs# local#]
       (let [~sym (build-test-db local# remote# ~domain-seq)]
         ~@body))))

(defn build-unsharded-test-db
  "Accepts a local and remote root directory and a map of domain name
  -> presharded map of documents (a map with <shard-idx, doc-seq>
  pairs) and returns a Database object.

  Accepts an optional configuration map as a final argument."
  [local-root remote-root domain-map & [conf-map]]
  {:pre [(or (not conf-map) (map? conf-map))]}
  (let [spec     (berkeley-spec 4)
        path-map (domain-path-map remote-root (keys domain-map))]
    (doseq [[domain-name remote-path] path-map]
      (create-unsharded-domain! spec remote-path (domain-map domain-name)))
    (db/build-database (merge conf-map
                              {:local-root local-root
                               :domains    path-map}))))


(defmacro with-unsharded-database
  "Generates a database with the supplied sequence of domain names and
  binds it to `sym` inside of the form."
  [[sym domain-seq] & body]
  `(t/with-fs-tmp [fs# remote#]
     (t/with-local-tmp [lfs# local#]
       (let [~sym (build-unsharded-test-db local# remote# ~domain-seq)]
         ~@body))))
