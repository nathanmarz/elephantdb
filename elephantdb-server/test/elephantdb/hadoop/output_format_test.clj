(ns elephantdb.hadoop.output-format-test
  (:use midje.sweet
        elephantdb.test.common
        elephantdb.test.keyval)
  (:require [hadoop-util.core :as h]
            [hadoop-util.test :as t]
            [jackknife.core :as u])
  (:import [elephantdb DomainSpec Utils]
           [elephantdb.hadoop ElephantOutputFormat
            ElephantOutputFormat$Args ElephantRecordWritable]
           [org.apache.hadoop.io IntWritable BytesWritable]
           [elephantdb.document KeyValDocument]
           [elephantdb.index Indexer]
           [elephantdb.persistence JavaBerkDB]
           [elephantdb.store VersionedStore]))

(def test-spec
  (berkeley-spec 2))

(defn write-data
  [writer data]
  (u/dofor [[s records] data
            [k v]      records]
           (.write writer
                   (IntWritable. s)
                   (ElephantRecordWritable. k v))))

(defn check-shards
  [fs lfs output-dir local-tmp expected]
  (.mkdirs lfs (h/path local-tmp))
  (u/dofor [[s records] expected]
           (let [local-shard-path (h/str-path local-tmp s)
                 persistence (do (.copyToLocalFile fs
                                                   (h/path output-dir (str s))
                                                   (h/path local-shard-path))
                                 (.openPersistenceForRead (.getCoordinator test-spec)
                                                          local-shard-path
                                                          {}))]
             (u/dofor [[k v] records]
                      (fact (barr= (.get persistence k) v) => truthy))
             (u/dofor [[_ non-records] (dissoc expected s)
                       [k _] non-records]
                      (fact (.get persistence k) => nil?)))))

(fact "Output format test."
  (t/with-fs-tmp [fs output-dir]
    (t/with-local-tmp [lfs etmp tmp2]
      (let [data {0 {(.getBytes "0a") (.getBytes "00")
                     (.getBytes "0b") (.getBytes "01")}
                  4 {(.getBytes "4a") (.getBytes "40")}}]
        (with-open [writer (elephant-writer test-spec
                                            output-dir
                                            etmp)]
          (write-data writer data))
        (fact (count (.listStatus fs (h/path output-dir))) => 2)
        (check-shards fs lfs output-dir tmp2  data)))))

(future-fact "test errors.")
