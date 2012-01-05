(ns elephantdb.persistence.keyval-test
  (:use midje.sweet
        elephantdb.common.testing
        elephantdb.keyval.testing)
  (:import [elephantdb.document KeyValDocument]
           [elephantdb.serialize KryoSerializer SerializationWrapper]
           [elephantdb.persistence JavaBerkDB]))

(defn prep-coordinator
  "If the supplied coordinator implements SerializationWrapper, sets
  the proper serialization; else does nothing. In either case,
  `prep-coordinator` returns the supplied coordinator."
  [coordinator]
  (if (instance? SerializationWrapper coordinator)
    (doto coordinator
      (.setSerializer (KryoSerializer.)))
    coordinator))

(defn is-db-pairs?
  "Returns true if the persistence housed by the supplied coordinator
  contains the supplied pairs, false otherwise."
  [coordinator path & pairs]
  (with-open [db (.openPersistenceForRead coordinator path {})]
    (fact (get-all db) => (just pairs))))

(defn test-get-put [coordinator]
  (let [coordinator (prep-coordinator coordinator)]
    (with-local-tmp [_ tmp-path]
      (.createPersistence coordinator tmp-path {})
      (with-open [db (.openPersistenceForAppend coordinator tmp-path {})]
        (fact (edb-get db "a") => nil)
        (edb-put db "a" "1")
        (edb-put db "b" "2")
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
        (edb-put db "a" "11")
        (fact (edb-get db "a") => "11")))))

(defn test-iterate [coord]
  (let [coord (prep-coordinator coord)]
    (with-local-tmp [_ tmp-path]
      (create-pairs coord tmp-path ["a" "1"])
      (is-db-pairs? coord tmp-path ["a" "1"])
      (append-pairs coord tmp-path ["c" "3"] ["b" "4"])
      (is-db-pairs? coord tmp-path ["a" "1"] ["b" "4"] ["c" "3"])
      (append-pairs coord tmp-path ["a" "4"] ["d" "5"])
      (is-db-pairs? coord tmp-path ["a" "4"] ["b" "4"] ["c" "3"] ["d" "5"]))))

;; ## Coordinator Testing
;;
;; These tests use the above functions to run Coordinators through a
;; test battery. The tests rely on interfaces, making them appropriate
;; for any defined Coordinator.

(facts
  "Tests that BerkeleyDB is able to put and get tuples."
  (test-get-put (JavaBerkDB.))
  (test-iterate (JavaBerkDB.)))
