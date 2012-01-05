(ns elephantdb.common.config-test
  (:use elephantdb.common.config
        elephantdb.common.testing
        midje.sweet)
  (:require [hadoop-util.test :as t])
  (:import [elephantdb.persistence JavaBerkDB]
           [elephantdb.partition HashModScheme]
           [elephantdb DomainSpec]))

(defn class-name
  "Returns a string representation of the supplied object's class
  name."
  [x]
  (.getName (.getClass x)))

(defn normalize
  "Resolves the class names of the non-primitives fields in the
  supplied spec-map."
  [spec-map]
  (-> spec-map
      (update-in [:coordinator]  class-name)
      (update-in [:shard-scheme] class-name)))

(defn round-trip-spec
  "Passes the supplied DomainSpec through ElephantDB's serialization
  methods, returning the result."
  [spec]
  (t/with-fs-tmp [fs tmp]
    (write-domain-spec! spec fs tmp)
    (read-domain-spec fs tmp)))

(def clj-spec
  {:num-shards   20
   :coordinator  (JavaBerkDB.)
   :shard-scheme (HashModScheme.)})

(tabular
 (fact
   "Conversion from a Clojure map -> DomainSpec requires all three
   keys."
   (convert-clj-domain-spec ?spec-map) => ?tester)
 ?spec-map                        ?tester
 clj-spec                        truthy
 (dissoc clj-spec :num-shards)   (throws AssertionError)
 (dissoc clj-spec :coordinator)  (throws AssertionError)
 (dissoc clj-spec :shard-scheme) (throws AssertionError))

(fact "Clojure spec-map should survive a round trip."
  (normalize
   (round-trip-spec clj-spec)) => (normalize clj-spec))

(fact "Check individual attributes."
  (t/with-fs-tmp [fs tmp]
    (write-domain-spec! clj-spec fs tmp)
    (let [jspec (DomainSpec/readFromFileSystem fs tmp)]
      (.getNumShards jspec)                => 20
      (class-name (.getCoordinator jspec)) => "elephantdb.persistence.JavaBerkDB"
      (class-name (.getShardScheme jspec)) => "elephantdb.partition.HashModScheme"
      (convert-clj-domain-spec clj-spec)  => jspec)))

;; ## Configurations

(defn round-trip-conf
  "Passes the supplied clojure data structure in and out of
  ElephantDB's configuration printer."
  [config]
  (t/with-fs-tmp [fs tmp]
    (write-clj-config! config fs tmp)
    (read-clj-config fs tmp)))

(tabular
 (fact
   "Configuration reading and writing should preserve the whole clojure
  data structure."
   (round-trip-conf ?data-structure) => ?data-structure)
 ?data-structure
 {:blah 2 :a "eee" :c [1 2 "a"]}
 {:foo {:lalala {:a 1 "c" 3}}})

