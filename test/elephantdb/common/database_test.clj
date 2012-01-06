(ns elephantdb.common.database-test
  (:use elephantdb.common.database
        elephantdb.test.common
        midje.sweet)
  (:require [elephantdb.common.domain :as domain]
            [hadoop-util.test :as t])
  (:import [elephantdb.document KeyValDocument]))

(let [spec (berkeley-spec 4)
      docs {0 [(KeyValDocument. 1 2)]
            1 [(KeyValDocument. 3 4)]}]
  (t/with-local-tmp [_ root]
    (t/with-fs-tmp [_ d1 d2 d3]
      (create-domain! spec d1 docs)
      (create-domain! spec d1 docs)
      (create-domain! spec d1 docs)
      (let [db (build-database
                {:local-root root
                 :domains {"one"   d1
                           "two"   d2
                           "three" d3}})]
        ))))
