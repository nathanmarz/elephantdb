(ns elephantdb.hadoop.output-format-test
  (:use clojure.test
        elephantdb.common.testing
        elephantdb.keyval.testing)
  (:require [hadoop-util.core :as h]
            [elephantdb.common.util :as u])
  (:import elephantdb.DomainSpec
           [elephantdb.hadoop ElephantOutputFormat
            ElephantOutputFormat$Args ElephantUpdater]
           [org.apache.hadoop.io IntWritable BytesWritable]
           [elephantdb.persistence JavaBerkDB KeyValDocument]
           [elephantdb Utils]
           [elephantdb.store VersionedStore]
           [elephantdb.test StringAppendUpdater]
           [java.util ArrayList]
           [org.apache.hadoop.mapred JobConf]))

(def test-spec
  (DomainSpec. "elephantdb.persistence.JavaBerkDB"
               "elephantdb.persistence.HashModScheme"
               2))

(defn write-data
  [writer data]
  (u/dofor [[s records] data
            [k v] records]
           (.write writer
                   (IntWritable. s)
                   (BytesWritable.
                    (.serialize test-spec (KeyValDocument. k v))))))

(defn check-shards
  [fs lfs output-dir local-tmp expected]
  (.mkdirs lfs (h/path local-tmp))
  (u/dofor [[s records] expected]
           (let [local-shard-path (h/str-path local-tmp s)
                 _  (.copyToLocalFile fs (h/path output-dir (str s))
                                      (h/path local-shard-path))
                 persistence (.openPersistenceForRead (.getCoordinator test-spec)
                                                      local-shard-path
                                                      {})]
             (u/dofor [[k v] records]
                      (is (= v (.get persistence k))))
             (u/dofor [[_ non-records] (dissoc expected s)
                       [k _] non-records]
                      (is (nil? (.get persistence k)))))))

(def-fs-test test-output-format [fs output-dir]
  (with-local-tmp [lfs etmp tmp2]
    (let [data {0 {"0a" "00" "0b" "01"} 4 {"4a" "40"}}
          writer  (mk-elephant-writer 10 (JavaBerkDB.) output-dir etmp)]
      (write-data writer data)
      (.close writer nil)
      (is (= 2 (count (.listStatus fs (h/path output-dir)))))
      (check-shards fs lfs output-dir tmp2  data))))

(def-fs-test test-incremental [fs dir1 dir2]
  (with-local-tmp [lfs ltmp1 ltmp2]
    (mk-presharded-domain fs dir1 (JavaBerkDB.)
                          {0 [["a" "1"]]
                           1 [["b" "2"]
                              ["c" "3"]]})
    (let [data {0 {"a" "2" "d" "4"} 1 {"c" "4" "e" "4"} 2 {"x" "x"}}
          writer (mk-elephant-writer 3 (JavaBerkDB.) dir2 ltmp1
                                     :updater (StringAppendUpdater.)
                                     :update-dir (.mostRecentVersionPath
                                                  (VersionedStore. dir1)))]
      (write-data writer data)
      (.close writer nil)
      (check-shards fs lfs dir2 ltmp2
                    {0 {"a" "12" "d" "4"}
                     1 {"b" "2" "c" "34" "e" "4"}
                     2 {"x" "x"}}))))

(def-fs-test test-errors [fs dir1]
  )
