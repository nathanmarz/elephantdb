(ns elephantdb.persistence.berkeleydb-test
  (:use clojure.test
        elephantdb.persistence.lp-tester)
  (:import elephantdb.persistence.JavaBerkDB))

(deftest test-berkeley-db
  (test-get-put (JavaBerkDB.))
  (test-iterate (JavaBerkDB.)))
