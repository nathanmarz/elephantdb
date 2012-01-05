(ns elephantdb.common.testing
  (:require [hadoop-util.core :as h]
            [hadoop-util.test :as t]
            [jackknife.logging :as log]
            [elephantdb.common.domain :as dom])
  (:import [java.util Arrays]
           [elephantdb.store DomainStore]
           [elephantdb DomainSpec]))

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

(defn mk-test-spec
  "Returns a DomainSpec initialized with BerkeleyDB, a HashMod scheme
  and the supplied shard-count."
  [shard-count]
  (DomainSpec. "elephantdb.persistence.JavaBerkDB"
               "elephantdb.partition.HashModScheme"
               shard-count))

(defn mk-populated-store!
  "Accepts a path, a DomainSpec and any number of pairs of shard-key
  and indexable document, and indexes the supplied documents into the
  supplied persistence."
  [path spec pair-seq & {:keys [version]}]
  (let [version      (rand-int 10)
        store        (DomainStore. path spec)
        shard-set    (.getShardSet store version)
        version-path (if version
                       (do (when (.hasVersion store version)
                             (.deleteVersion store version))
                           (.createVersion store version))
                       (.createVersion store))]
    (doseq [[idx doc-seq] (group-by #(.shardIndex shard-set (first %))
                                    pair-seq)]
      (with-open [shard (do (.createShard shard-set idx)
                            (.openShardForAppend shard-set idx))]
        (doseq [doc (map second doc-seq)]
          (.index shard doc))))
    (doto store
      (.succeedVersion version-path))))
