(ns elephantdb.service-test
  (:use clojure.test)
  (:import [elephantdb.persistence JavaBerkDB])
  (:import [elephantdb.generated WrongHostException
            DomainNotFoundException])
  (:use [elephantdb service testing util]))

(defn get-val [elephant d k]
  (.get_data (.get elephant d k)))

(defn direct-get-val [elephant d k]
  (.get_data (.directGet elephant d k)))

(deftest test-basic
  (with-sharded-domain [dpath
                        {:num-shards 4
                         :persistence-factory (JavaBerkDB.)}
                        [[(barr 0) (barr 0 0)]
                         [(barr 1) (barr 1 1)]
                         [(barr 2) (barr 2 2)]
                         ]]
    (with-service-handler
      [elephant
       [(local-hostname)]
       {"test1" dpath}
       nil]
      (is (barr= (barr 0 0) (get-val elephant "test1" (barr 0))))
      (is (barr= (barr 1 1) (get-val elephant "test1" (barr 1))))
      (is (barr= (barr 2 2) (get-val elephant "test1" (barr 2))))
      (is (= nil (get-val elephant "test1" (barr 3))))
      )))

(deftest test-multi-server
  (with-presharded-domain
    ["test1"
     dpath
     (JavaBerkDB.)
     {0 [[(barr 0) (barr 0 0)]
         [(barr 1) (barr 1 1)]
         [(barr 2) nil]]
      1 [[(barr 10) (barr 10 0)]]
      2 [[(barr 20) (barr 20 0)]
         [(barr 21) (barr 21 1)]]
      3 [[(barr 30) (barr 30 0)]]}]
    (with-service-handler
      [elephant
       [(local-hostname) "host2"]
       {"test1" dpath}
       {"test1" {(local-hostname) [0 2] "host2" [1 3]}}]
      (is (barr=
           (barr 0 0)
           (get-val elephant "test1" (barr 0))))
      (is (barr=
           (barr 20 0)
           (get-val elephant "test1" (barr 20))))
      (is (=
           nil
           (get-val elephant "test1" (barr 2))))
      (is (thrown?
           WrongHostException
           (direct-get-val elephant "test1" (barr 10))))
      )))
