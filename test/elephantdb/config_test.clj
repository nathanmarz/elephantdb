(ns elephantdb.config-test
  (:use clojure.test
        elephantdb.common.testing)
  (:require [elephantdb.common.config :as conf])
  (:import [elephantdb.persistence JavaBerkDB]
           [elephantdb DomainSpec]))

(defn- norm-spec [spec-map]
  (update-in spec-map
             [:coordinator]
             #(.getName (.getClass %))))

(defn- specs= [s1 s2]
  (= (norm-spec s1)
     (norm-spec s2)))

(def-fs-test test-rw-domain-spec [fs tmp]
  (let [spec {:num-shards 20 :coordinator (JavaBerkDB.)}]
    (conf/write-domain-spec! spec fs tmp)
    (is (specs= spec (conf/read-domain-spec fs tmp))))
  (let [jspec (DomainSpec/readFromFileSystem fs tmp)]
    (is (= 20 (.getNumShards jspec)))
    (is (= "elephantdb.persistence.JavaBerkDB" (-> (.getCoordinator jspec)
                                                   (.getClass)
                                                   (.getName))))))

(def-fs-test test-rw-clj-configs
  [fs tmp1 tmp2]
  (let [config1 {:blah 2 :a "eee" :c [1 2 "a"]}
        config2 {:foo {:lalala {:a 1 "c" 3}}}]
    (conf/write-clj-config! config1 fs tmp1)
    (conf/write-clj-config! config2 fs tmp2)
    (is (= config1 (conf/read-clj-config fs tmp1)))
    (is (= config2 (conf/read-clj-config fs tmp2)))))
