(ns elephantdb.persistence.local-persistence-tester
  (:use clojure.test))

(defmacro test-lp [lp-classname]
  (let [exprs ['(use (quote clojure.test))
               '(use (quote elephantdb.keyval.testing))
               `(def ~'factory (new ~lp-classname))
               '(do
                  ;; technically should do sorting and stuff here too
                  (deflocalfstest test-get-put [lfs t]
                    (with-open [db (.createPersistence factory t {})]
                      (is (= nil (get-string db "a")))
                      (add-string db "a" "1")
                      (add-string db "b" "2")
                      (is (= "1" (get-string db "a")))
                      (is (= "2" (get-string db "b")))
                      (is (= nil (get-string db "c"))))

                    (with-open [db (.openPersistenceForRead factory t {})]
                      (is (= "1" (get-string db "a")))
                      (is (= "2" (get-string db "b")))
                      (is (= nil (get-string db "c"))))

                    (with-open [db (.openPersistenceForAppend factory t {})]
                      (is (= "1" (get-string db "a")))
                      (add-string db "a" "11")
                      (is (= "11" (get-string db "a")))))

                  (defn is-db-pairs? [fact t pairs]
                    (with-open [db (.openPersistenceForRead fact t {})]
                      (is (= (set pairs)
                             (set (get-string-kvpairs db))))))

                  (deflocalfstest test-iterate [lfs t]
                    (create-string-pairs factory t [["a" "1"]])
                    (is-db-pairs? factory t [["a" "1"]])
                    (append-string-pairs factory t [["c" "3"] ["b" "4"]])
                    (is-db-pairs? factory t [["a" "1"] ["b" "4"] ["c" "3"]])
                    (append-string-pairs factory t [["a" "4"] ["d" "5"]])
                    (is-db-pairs? factory t [["a" "4"] ["b" "4"] ["c" "3"] ["d" "5"]])))]]
    `(do ~@exprs)))
