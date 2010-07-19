(ns elephantdb.persistence.java-berkdb-test
  (:use elephantdb.persistence.local-persistence-tester)
  (:import elephantdb.persistence.JavaBerkDB))

(test-lp JavaBerkDB)