(ns elephantdb.common.spec-test
  (:use midje.sweet)
  (:import [elephantdb DomainSpec Utils]
           [elephantdb.document KeyValDocument]))

(defn domain-spec
  [coord-class shard-scheme shard-count]
  (DomainSpec. coord-class shard-scheme shard-count))

(defn kv-doc [k v]
  (KeyValDocument. k v))

(defn round-trip [item]
  (let [serializer (Utils/makeSerializer
                    (domain-spec "elephantdb.persistence.JavaBerkDB"
                                 "elephantdb.partition.HashModScheme"
                                 2))]
    (->> item
         (.serialize serializer)
         (.deserialize serializer))))
(tabular
 (fact
   "Key Value documents should be able to be round-tripped through a
    serializer."
   (let [doc           (kv-doc ?key ?val)
         round-tripped (round-trip doc)]
     (.key doc)   => (.key round-tripped)
     (.value doc) => (.value round-tripped)))
 ?key    ?val
 1       2
 "three" "four!")

