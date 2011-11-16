(ns elephantdb.hadoop.exporter-test
  (:use clojure.test
        hadoop-util.core
        elephantdb.common.log
        elephantdb.common.hadoop
        [elephantdb testing util config])
  (:require [elephantdb.thrift :as thrift])
  (:import [elephantdb DomainSpec]
           [elephantdb.persistence JavaBerkDB]
           [elephantdb.hadoop Exporter]
           [org.apache.hadoop.io BytesWritable SequenceFile
            SequenceFile$CompressionType]))

(defn- write-seqfile-records [fs dir pairs]
  (mkdirs fs dir)
  (with-open [writer (SequenceFile/createWriter fs
                                                (.getConf fs)
                                                (path dir "part0000")
                                                BytesWritable BytesWritable
                                                SequenceFile$CompressionType/NONE)]
    (doseq [[k v] pairs]
      (.append writer
               (BytesWritable. k)
               (BytesWritable. v)))))

(deftest test-basic-export
  (let [data [[(barr 1) (barr 11)]
              [(barr 2) (barr 22)]
              [(barr 3) (barr 33)]
              [(barr 4) (barr 44)]
              [(barr 5) (barr 55)]
              [(barr 6) (barr 66)]
              [(barr 7) (barr 77)]
              [(barr 8) (barr 88)]
              [(barr 9) (barr 99)]
              [(barr 10) (barr 100)]]]
    (with-fs-tmp [fs input domain]
      (write-seqfile-records fs input data)
      (Exporter/export input domain (DomainSpec. (JavaBerkDB.) 2))
      (with-single-service-handler [handler {"test" domain}]
        (check-domain "test" handler data)))))
