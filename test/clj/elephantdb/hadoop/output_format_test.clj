(ns elephantdb.hadoop.output-format-test
  (:use clojure.test)
  (:use [elephantdb testing hadoop config util])
  (:import [elephantdb.hadoop ElephantOutputFormat ElephantOutputFormat$Args ElephantRecordWritable])
  (:import [org.apache.hadoop.io IntWritable])
  (:import [elephantdb.persistence JavaBerkDB])
  (:import [elephantdb Utils])
  (:import [java.util ArrayList])
  (:import [org.apache.hadoop.mapred JobConf]))

;; getRecordWriter(FileSystem fs, JobConf conf, String string, Progressable progressable)

(defn mk-writer [shards factory output-dir tmpdir]
  (.getRecordWriter
    (ElephantOutputFormat.)
    nil
    (doto
      (JobConf.)
      (Utils/setObject
        ElephantOutputFormat/ARGS_CONF
        (doto
          (ElephantOutputFormat$Args.
            (convert-clj-domain-spec
              {:num-shards shards
               :persistence-factory factory})
            output-dir)
          (.setTmpDirs (ArrayList. [tmpdir])))))
    nil
    nil ))

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

(deffstest test-output-format [fs output-dir]
  (with-local-tmp [lfs etmp tmp2]
    (let [data {0 {"0a" "00" "0b" "01"} 4 {"4a" "40"}}
          writer  (mk-writer 10 (JavaBerkDB.) output-dir etmp)]
      (write-data writer data)
      (.close writer nil)
      (is (= 2 (count (.listStatus fs (path output-dir)))))
      (check-shards fs lfs output-dir tmp2 (JavaBerkDB.) data)
      )))

(deftest test-errors)
