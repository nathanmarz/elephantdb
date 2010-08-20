(ns elephantdb.service-test
  (:use clojure.test)
  (:import [elephantdb.persistence JavaBerkDB])
  (:import [elephantdb.generated WrongHostException
            DomainNotFoundException])
  (:use [elephantdb service testing util config]))

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

(deftest test-multi-domain
  (let [data1 [[(barr 0) (barr 0 0)]
               [(barr 10) (barr 10 1)]
               [(barr 20) (barr 20 2)]
               [(barr 30) (barr 30 3)]]
        data2 [[(barr 5) (barr 5 0)]
               [(barr 15) (barr 15 15)]
               [(barr 105) (barr 110)]
               ]]
    (with-sharded-domain [dpath1
                          {:num-shards 2 :persistence-factory (JavaBerkDB.)}
                          data1]
      (with-sharded-domain [dpath2
                            {:num-shards 3 :persistence-factory (JavaBerkDB.)}
                            data2]
        (with-single-service-handler [handler {"d1" dpath1 "d2" dpath2}]
          (check-domain "d1" handler data1)
          (check-domain "d2" handler data2)
          (check-domain-not "d1" handler data2)
          (check-domain-not "d2" handler data1)
          )))))

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

(deftest test-caching
  (with-local-tmp [lfs local-dir]
    (with-fs-tmp [fs dtmp gtmp]
      (let [domain-spec {:num-shards 4 :persistence-factory (JavaBerkDB.)}
            local-config (mk-local-config local-dir)]
        (write-clj-config! {:replication 1
                            :hosts [(local-hostname)]
                            :domains {"test" dtmp}}
                           fs
                           gtmp)
        (mk-sharded-domain fs dtmp domain-spec
                           [[(barr 0) (barr 0 0)]
                            [(barr 1) (barr 1 1)]
                            [(barr 2) (barr 2 2)]
                            [(barr 3) (barr 3 3)]])
        ;; NOTE: should really just to the read global config in mk-service-handler
        (.shutdown
         (mk-service-handler (read-global-config gtmp local-config "111") local-dir "111" nil))
        (mk-sharded-domain fs dtmp domain-spec
                           [[(barr 0) (barr 0 1)]
                            [(barr 3) (barr 3 4)]
                            [(barr 4) (barr 4 5)]])
        (let [handler (mk-service-handler (read-global-config gtmp local-config "111") local-dir "111" nil)]
          (is (barr= (barr 0 0)
                     (get-val handler "test" (barr 0))))
          (is (barr= (barr 2 2)
                     (get-val handler "test" (barr 2))))
          (is (= nil (get-val handler "test" (barr 4))))
          (.shutdown handler))
        (let [handler (mk-service-handler (read-global-config gtmp local-config "112") local-dir "112" nil)]
          (is (barr= (barr 0 1)
                     (get-val handler "test" (barr 0))))
          (is (barr= (barr 4 5)
                     (get-val handler "test" (barr 4))))
          (is (= nil (get-val handler "test" (barr 2))))
          (.shutdown handler))
        ))))
