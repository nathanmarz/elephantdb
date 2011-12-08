(ns elephantdb.persistence.local-persistence-tester
  (:use clojure.test))

(defmacro test-lp [lp-classname]
  (let [exprs ['(use (quote clojure.test))
               '(use (quote elephantdb.keyval.testing))
               '(use (quote elephantdb.common.testing))
               `(def ~'coordinator (new ~lp-classname))
               '(do
                  ;; technically should do sorting and stuff here too
                  (deflocalfstest test-get-put [lfs t]
                    (with-open [db (.createPersistence coordinator t {})]
                      (is (= nil (get-string db "a")))
                      (add-string db "a" "1")
                      (add-string db "b" "2")
                      (is (= "1" (get-string db "a")))
                      (is (= "2" (get-string db "b")))
                      (is (= nil (get-string db "c"))))

                    (with-open [db (.openPersistenceForRead coordinator t {})]
                      (is (= "1" (get-string db "a")))
                      (is (= "2" (get-string db "b")))
                      (is (= nil (get-string db "c"))))

                    (with-open [db (.openPersistenceForAppend coordinator t {})]
                      (is (= "1" (get-string db "a")))
                      (add-string db "a" "11")
                      (is (= "11" (get-string db "a")))))

                  (defn is-db-pairs? [coordinator t pairs]
                    (with-open [db (.openPersistenceForRead coordinator t {})]
                      (is (= (set pairs)
                             (set (get-string-kvpairs db))))))

                  (deflocalfstest test-iterate [lfs t]
                    (create-string-pairs coordinator t [["a" "1"]])
                    (is-db-pairs? coordinator t [["a" "1"]])
                    (append-string-pairs coordinator t [["c" "3"] ["b" "4"]])
                    (is-db-pairs? coordinator t [["a" "1"] ["b" "4"] ["c" "3"]])
                    (append-string-pairs coordinator t [["a" "4"] ["d" "5"]])
                    (is-db-pairs? coordinator t [["a" "4"] ["b" "4"] ["c" "3"] ["d" "5"]])))]]
    `(do ~@exprs)))
