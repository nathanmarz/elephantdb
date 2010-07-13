(ns elephantdb.hadoop
  (:import [org.apache.hadoop.fs FileSystem Path]
           [org.apache.hadoop.conf Configuration])
  (:use [elephantdb log]))

(defmulti conf-set (fn [obj] (class (:value obj))))

(defmethod conf-set String [{key :key value :value conf :conf}]
  (.set conf key value))

(defmethod conf-set Integer [{key :key value :value conf :conf}]
  (.setInt conf key value))

(defmethod conf-set Float [{key :key value :value conf :conf}]
  (.setFloat conf key value))

(defn path
  ([str-or-path]
    (if (instance? Path str-or-path) str-or-path (Path. str-or-path)))
  ([parent child] (Path. parent child)))

(defn configuration [conf-map]
  (let [ret (Configuration.)]
    (doall
      (for [config conf-map]
        (conf-set {:key (first config) :value (last config) :conf ret})))
    ret))

(defn filesystem
  ([] (FileSystem/get (Configuration.)))
  ([conf-map]
    (FileSystem/get (configuration conf-map))))

(defn mkdirs [fs path]
  (.mkdirs fs (Path. path)))

(defn delete
  ([fs path] (delete fs path false))
  ([fs path rec]
  (.delete fs (Path. path) rec)))

(defn clear-dir [fs path]
  (delete fs path true)
  (mkdirs path))

(defn local-filesystem [] (FileSystem/getLocal (Configuration.)))

;; kb-sec-rate to nil to go as fast as possible, local-dir shouldn't exist
(defn copy-local [fs remote-dir local-dir kb-sec-rate]
  ;; TODO: finish
  )
