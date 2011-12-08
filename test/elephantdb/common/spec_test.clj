(ns elephantdb.common.spec-test
  (:use clojure.test)
  (:import [elephantdb DomainSpec]
           [elephantdb.persistence KeyValDocument]))

(defn domain-spec
  [coord-class shard-scheme shard-count]
  (DomainSpec. coord-class shard-scheme shard-count))

(def test-spec
  (domain-spec "elephantdb.persistence.JavaBerkDB"
               "elephantdb.persistence.HashModScheme"
               2))

(defn kv-doc [k v]
  (KeyValDocument. k v))

(deftest round-trip-test
  (are [k v] (let [original (kv-doc k v)
                   round-tripped (->> original
                                      (.serialize test-spec)
                                      (.deserializer test-spec))]
               (= original round-tripped))
       1       2
       [1 2 3] "four!"))


