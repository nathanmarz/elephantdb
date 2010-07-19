(ns elephantdb.persistence.local-persistence-tester
  (:use clojure.test))

(defmacro test-lp [lp-classname]
  (let [exprs [
'(use (quote clojure.test))
'(use (quote elephantdb.testing))
`(def ~'factory (new ~lp-classname))
'(do

;; technically should do sorting and stuff here too
(deflocalfstest test-get-put [lfs t]
  (let [db (.createPersistence factory t {})]
    (is (= nil (get-string db "a")))
    (add-string db "a" "1")
    (add-string db "b" "2")
    (is (= "1" (get-string db "a")))
    (is (= "2" (get-string db "b")))
    (is (= nil (get-string db "c")))
    (.close db))
  (let [db (.openPersistenceForRead factory t {})]
    (is (= "1" (get-string db "a")))
    (is (= "2" (get-string db "b")))
    (is (= nil (get-string db "c")))
    (.close db))
  (let [db (.openPersistenceForAppend factory t {})]
    (is (= "1" (get-string db "a")))
    (add-string db "a" "11")
    (is (= "11" (get-string db "a")))
    (.close db)))

)]]
`(do ~@exprs)
))