(ns elephantdb.common.spec-test
  (:use clojure.test)
  (:import [elephantdb DomainSpec]
           [elephantdb.document KeyValDocument]))

(defn domain-spec
  [coord-class shard-scheme shard-count]
  (DomainSpec. coord-class shard-scheme shard-count))

(def test-spec
  (domain-spec "elephantdb.persistence.JavaBerkDB"
               "elephantdb.partition.HashModScheme"
               2))

(defn kv-doc [k v]
  (KeyValDocument. k v))

(defn doc= [x y]
  (and (= (.key x) (.key y))
       (= (.value x) (.value y))))

(deftest round-trip-test
  (are [k v] (let [original (kv-doc k v)
                   round-tripped (->> original
                                      (.serialize test-spec)
                                      (.deserialize test-spec))]
               (doc= original round-tripped))
       1       2
       "three" "four!"))

