(ns elephantdb.persistence.lp-tester
  (:use midje.sweet
        elephantdb.common.testing
        elephantdb.keyval.testing)
  (:import [elephantdb.document KeyValDocument]))

(defn is-db-pairs? [coordinator path & pairs]
  (with-open [db (.openPersistenceForRead coordinator path {})]
    (fact (get-all db) => (just pairs))))

(defn test-get-put [coordinator]
  (with-local-tmp [_ tmp-path]
    (with-open [db (.createPersistence coordinator tmp-path {})]
      (fact (edb-get db "a") => nil)
      (index db "a" "1")
      (index db "b" "2")
      (facts
        (edb-get db "a") => "1"
        (edb-get db "b") => "2"
        (edb-get db "c") => nil))

    (with-open [db (.openPersistenceForRead coordinator tmp-path {})]
      (facts
        (edb-get db "a") => "1"
        (edb-get db "b") => "2"
        (edb-get db "c") => nil))

    (with-open [db (.openPersistenceForAppend coordinator tmp-path {})]
      (fact (edb-get db "a") => "1")
      (index db "a" "11")
      (fact (edb-get db "a") => "11"))))

(defn test-iterate [coordinator]
  (with-local-tmp [_ tmp-path]
    (create-pairs coordinator tmp-path ["a" "1"])
    (is-db-pairs? tmp-path ["a" "1"])
    (append-pairs coordinator tmp-path ["c" "3"] ["b" "4"])
    (is-db-pairs? tmp-path ["a" "1"] ["b" "4"] ["c" "3"])
    (append-pairs coordinator tmp-path ["a" "4"] ["d" "5"])
    (is-db-pairs? tmp-path ["a" "4"] ["b" "4"] ["c" "3"] ["d" "5"])))
