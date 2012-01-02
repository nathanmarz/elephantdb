(ns elephantdb.persistence.java-berkdb-test
  (:use clojure.test
        elephantdb.persistence.local-persistence-tester)
  (:import elephantdb.persistence.JavaBerkDB))

(deftest test-berkeley-db
  (test-get-put (JavaBerkDB.))
  (test-iterate (JavaBerkDB.)))
