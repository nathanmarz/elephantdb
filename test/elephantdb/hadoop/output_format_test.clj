(ns elephantdb.hadoop.output-format-test
  (:use clojure.test
        hadoop-util.core
        elephantdb.common.hadoop
        elephantdb.common.util
        elephantdb.keyval.testing
        elephantdb.keyval.config)
  (:import [elephantdb.hadoop ElephantOutputFormat
            ElephantOutputFormat$Args
            ElephantRecordWritable ElephantUpdater]
           [org.apache.hadoop.io IntWritable]
           [elephantdb.persistence JavaBerkDB]
           [elephantdb Utils]
           [elephantdb.store VersionedStore]
           [elephantdb.test StringAppendUpdater]
           [java.util ArrayList]
           [org.apache.hadoop.mapred JobConf]))

(defn write-data [writer data]
  (dofor [[s records] data]
         (dofor [[k v] records]
                (.write
                 writer
                 (IntWritable. s)
                 (ElephantRecordWritable.
                  (.getBytes k)
                  (.getBytes v))))))

(defn check-shards [fs lfs output-dir local-tmp factory expected]
  (.mkdirs lfs (path local-tmp))
  (dofor [[s records] expected]
         (let [local-shard-path (str-path local-tmp s)
               _                (.copyToLocalFile fs (path output-dir (str s)) (path local-shard-path))
               persistence      (.openPersistenceForRead factory local-shard-path {})]
           (dofor [[k v] records]
                  (is (= v (String. (.get persistence (.getBytes k))))))
           (dofor [[_ non-records] (dissoc expected s)]
                  (dofor [[k _] non-records]
                         (is (= nil (.get persistence (.getBytes k))))))
           )))

(def-fs-test test-output-format [fs output-dir]
  (with-local-tmp [lfs etmp tmp2]
    (let [data {0 {"0a" "00" "0b" "01"} 4 {"4a" "40"}}
          writer  (mk-elephant-writer 10 (JavaBerkDB.) output-dir etmp)]
      (write-data writer data)
      (.close writer nil)
      (is (= 2 (count (.listStatus fs (path output-dir)))))
      (check-shards fs lfs output-dir tmp2 (JavaBerkDB.) data)
      )))

(def-fs-test test-incremental [fs dir1 dir2]
  (with-local-tmp [lfs ltmp1 ltmp2]
    (mk-presharded-domain fs dir1 (JavaBerkDB.)
                          {0 [[(.getBytes "a") (.getBytes "1")]]
                           1 [[(.getBytes "b") (.getBytes "2")]
                              [(.getBytes "c") (.getBytes "3")]]})
    (let [data {0 {"a" "2" "d" "4"} 1 {"c" "4" "e" "4"} 2 {"x" "x"}}
          writer (mk-elephant-writer 3 (JavaBerkDB.) dir2 ltmp1
                                     :updater (StringAppendUpdater.)
                                     :update-dir (.mostRecentVersionPath
                                                  (VersionedStore. dir1)))]
      (write-data writer data)
      (.close writer nil)
      (check-shards fs lfs dir2 ltmp2 (JavaBerkDB.)
                    {0 {"a" "12" "d" "4"}
                     1 {"b" "2" "c" "34" "e" "4"}
                     2 {"x" "x"}})
      )))

(def-fs-test test-errors [fs dir1]
  )
