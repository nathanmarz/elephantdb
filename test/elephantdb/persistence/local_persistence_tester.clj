(ns elephantdb.persistence.local-persistence-tester
  (:use clojure.test
        elephantdb.common.testing
        elephantdb.keyval.testing)
  (:import [elephantdb.document KeyValDocument]))

(defn is-db-pairs? [coordinator path & pairs]
  (with-open [db (.openPersistenceForRead coordinator path {})]
    (is (= (set pairs)
           (set (get-all db))))))

(defn test-get-put [coordinator]
  (with-local-tmp [_ tmp-path]
    (with-open [db (.createPersistence coordinator tmp-path {})]
      (is (= nil (edb-get db "a")))
      (index db "a" "1")
      (index db "b" "2")
      (is (= "1" (edb-get db "a")))
      (is (= "2" (edb-get db "b")))
      (is (= nil (edb-get db "c"))))

    (with-open [db (.openPersistenceForRead coordinator t {})]
      (is (= "1" (edb-get db "a")))
      (is (= "2" (edb-get db "b")))
      (is (= nil (edb-get db "c"))))

    (with-open [db (.openPersistenceForAppend coordinator t {})]
      (is (= "1" (edb-get db "a")))
      (index db "a" "11")
      (is (= "11" (edb-get db "a"))))))

(defn test-iterate [coordinator]
  (with-local-tmp [_ tmp-path]
    (create-pairs coordinator t ["a" "1"])
    (is-db-pairs? tmp-path ["a" "1"])
    (append-pairs coordinator t ["c" "3"] ["b" "4"])
    (is-db-pairs? tmp-path ["a" "1"] ["b" "4"] ["c" "3"])
    (append-pairs coordinator  t ["a" "4"] ["d" "5"])
    (is-db-pairs? tmp-path ["a" "4"] ["b" "4"] ["c" "3"] ["d" "5"])))
