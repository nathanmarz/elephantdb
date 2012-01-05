(ns elephantdb.common.testing
  (:require [hadoop-util.core :as h]
            [jackknife.logging :as log]
            [elephantdb.common.domain :as dom])
  (:import [java.util UUID Arrays]
           [elephantdb.store DomainStore]
           [elephantdb DomainSpec]))

;; ## Domain Testing

(defn specs-match?
  "Returns true of the specs of all supplied DomainStores match, false
  otherwise."
  [& stores]
  (apply = (map (fn [^DomainStore x]
                  (.getSpec x))
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
  [path spec & pairs]
  (let [version      (rand-int 10)
        store        (DomainStore. path spec)
        shard-set    (.getShardSet store version)
        version-path (.createVersion store version)]
    (doseq [[idx doc-seq] (group-by #(.shardIndex shard-set (first %))
                                    pairs)]
      (with-open [shard (.openShardForAppend shard-set idx)]
        (doseq [doc (map second doc-seq)]
          (.index shard doc))))
    (doto store
      (.succeedVersion version-path))))

;; ## Hadoop Testing Utilities
;;
;; TODO: Most of this code is now located inside of
;; `hadoop-util.test`. Move it out for good.

(defn uuid []
  (str (UUID/randomUUID)))

;; TODO: Move to hadoop-util.
(defn delete-all [fs paths]
  (dorun
   (for [t paths]
     (.delete fs (h/path t) true))))

(defmacro with-fs-tmp
  [[fs-sym & tmp-syms] & body]
  (let [tmp-paths (mapcat (fn [t]
                            [t `(str "/tmp/unittests/" (uuid))])
                          tmp-syms)]
    `(let [~fs-sym (h/filesystem)
           ~@tmp-paths]
       (.mkdirs ~fs-sym (h/path "/tmp/unittests"))
       (try ~@body
            (finally
             (delete-all ~fs-sym [~@tmp-syms]))))))

(defmacro def-fs-test
  [name fs-args & body]
  `(deftest ~name
     (with-fs-tmp ~fs-args
       ~@body)))

(defn local-temp-path []
  (str (System/getProperty "java.io.tmpdir") "/" (uuid)))

(defmacro with-local-tmp
  [[fs-sym & tmp-syms] & [kw & more :as body]]
  (let [[log-lev body] (if (keyword? kw)
                         [kw more]
                         [:warn body])
        tmp-paths (mapcat (fn [t]
                            [t `(local-temp-path)])
                          tmp-syms)]
    `(log/with-log-level ~log-lev
       (let [~fs-sym (h/local-filesystem)
             ~@tmp-paths]
         (try ~@body
              (finally
               (delete-all ~fs-sym [~@tmp-syms])))))))

(defmacro def-local-fs-test [name local-args & body]
  `(deftest ~name
     (with-local-tmp ~local-args
       ~@body)))
