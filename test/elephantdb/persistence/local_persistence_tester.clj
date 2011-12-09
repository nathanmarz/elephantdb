(ns elephantdb.persistence.local-persistence-tester
  (:use clojure.test)
  (:import elephantdb.DomainSpec
           [elephantdb.persistence KeyValDocument]))

(defmacro test-lp [lp-classname]
  (let [exprs ['(use (quote clojure.test))
               '(use (quote elephantdb.keyval.testing))
               '(use (quote elephantdb.common.testing))
               `(def ~'coordinator (new ~lp-classname))
               `(def ~'spec (DomainSpec. (.getName ~lp-classname)
                                         "elephantdb.persistence.HashModScheme"
                                         2))
               '(do
                  ;; technically should do sorting and stuff here too
                  (deflocalfstest test-get-put [lfs t]
                    (with-open [db (.createPersistence coordinator t spec {})]
                      (is (= nil (edb-get db "a")))
                      (index db "a" "1")
                      (index db "b" "2")
                      (is (= "1" (edb-get db "a")))
                      (is (= "2" (edb-get db "b")))
                      (is (= nil (edb-get db "c"))))

                    (with-open [db (.openPersistenceForRead coordinator t spec {})]
                      (is (= "1" (edb-get db "a")))
                      (is (= "2" (edb-get db "b")))
                      (is (= nil (edb-get db "c"))))

                    (with-open [db (.openPersistenceForAppend coordinator t spec {})]
                      (is (= "1" (edb-get db "a")))
                      (index db "a" "11")
                      (is (= "11" (edb-get db "a")))))

                  (defn is-db-pairs? [t & pairs]
                    (with-open [db (.openPersistenceForRead coordinator t spec {})]
                      (is (= (set pairs)
                             (set (get-all db))))))

                  (deflocalfstest test-iterate [lfs t]
                    (create-pairs coordinator t spec ["a" "1"])
                    (is-db-pairs? t ["a" "1"])
                    (append-pairs coordinator t spec ["c" "3"] ["b" "4"])
                    (is-db-pairs? t ["a" "1"] ["b" "4"] ["c" "3"])
                    (append-pairs coordinator  t spec ["a" "4"] ["d" "5"])
                    (is-db-pairs? t ["a" "4"] ["b" "4"] ["c" "3"] ["d" "5"])))]]
    `(do ~@exprs)))
