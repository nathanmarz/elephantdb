(ns elephantdb.hadoop.input-format-test
  (:use midje.sweet
        elephantdb.test.common
        elephantdb.test.keyval)
  (:import [elephantdb Utils]
           [elephantdb.persistence JavaBerkDB]
           [elephantdb.partition HashModScheme]
           [elephantdb.hadoop ElephantInputFormat ElephantInputFormat$Args]
           [org.apache.hadoop.mapred JobConf]))

(defn read-reader [reader]
  (let [key     (.createKey reader)
        value   (.createValue reader)
        reads   (repeatedly
                 #(if (.next reader key value)
                    [true [(Utils/getBytes key) (Utils/getBytes value)]]
                    [false [nil nil]]))
        ret-val (doall (map second (take-while first reads)))]
    (.close reader)
    ret-val))

(defn read-domain [dpath]
  (let [input-format (ElephantInputFormat.)
        args (ElephantInputFormat$Args. dpath)
        conf (doto (JobConf.)
               (Utils/setObject ElephantInputFormat/ARGS_CONF args))
        splits (.getSplits input-format conf 0)
        readers (for [s splits]
                  (.getRecordReader input-format s conf nil))]
    (mapcat read-reader readers)))

(let [pairs [[(barr 0) (barr 0 2)]
             [(barr 1) (barr 10 21)]
             [(barr 2) (barr 9 1)]
             [(barr 3) (barr 0 2 3)]
             [(barr 4) (barr 0)]
             [(barr 5) (barr 1)]
             [(barr 6) (barr 3)]
             [(barr 7) (barr 9 9 9 9)]
             [(barr 8) (barr 9 9 9 1)]
             [(barr 9) (barr 9 9 2)]
             [(barr 10) (barr 3)]]]
  (future-fact "The record reader should produce what's been added to the
        domain."
               ;; TODO: write with-sharded-domain
               (with-sharded-domain [dpath
                                     {:num-shards 6
                                      :coordinator (JavaBerkDB.)
                                      :shard-scheme (HashModScheme.)}
                                     pairs]
                 (read-domain dpath) => pairs)))

;; TODO: test read specific version vs read most recent
