(ns elephantdb.service-test
  (:use clojure.test)
  (:import [elephantdb.persistence JavaBerkDB])
  (:use [elephantdb service testing util]))


(deftest test-basic
  (with-presharded-domain
    [dpath
     (JavaBerkDB.)
     {0 [[(barr 0) (barr 0 0)]
         [(barr 1) (barr 1 1)]]
      1 [[(barr 10) (barr 10 0)]]
      2 [[(barr 20) (barr 20 0)]
         [(barr 21) (barr 21 1)]]
      3 [[(barr 30) (barr 30 0)]]}]
    (with-service-handler
      [elephant
       [(local-hostname) "host2"]
       {"test1" dpath}
       {"test1" {(local-hostname) [0 2] "host2" [1 3]}}]
      (is (barr= (barr 0 0) (.get elephant "test1" (barr 0))))
      )))
