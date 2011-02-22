(ns elephantdb.service-test
  (:use clojure.test)
  (:import [elephantdb.persistence JavaBerkDB])
  (:import [elephantdb.generated WrongHostException
            DomainNotFoundException])
  (:use [elephantdb service testing util config hadoop])
  (:require [elephantdb [thrift :as thrift]]))

(defn get-val [elephant d k]
  (.get_data (.get elephant d k)))

(defn direct-get-val [elephant d k]
  (.get_data (first (.directMultiGet elephant d [k]))))

(defn multi-get-vals [elephant domain keys]
  (map (memfn get_data) (.multiGet elephant domain keys)))

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
           Exception
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

(deftest test-update-synched
  (with-local-tmp [lfs local-dir]
    (with-fs-tmp [fs dtmp1 dtmp2 gtmp]
      (let [domain-spec {:num-shards 4 :persistence-factory (JavaBerkDB.)}
            local-config (mk-local-config local-dir)]
        (write-clj-config! {:replication 1
                            :hosts [(local-hostname)]
                            :domains {"no-update" dtmp1 "do-update" dtmp2}}
                           fs
                           gtmp)

        ;; create version 1 for no-update
        (mk-sharded-domain fs dtmp1 domain-spec
                           [[(barr 0) (barr 0 0)]
                            [(barr 1) (barr 1 1)]
                            [(barr 2) (barr 2 2)]
                            [(barr 3) (barr 3 3)]] :version 1)

        ;; create version 1 for do-update
        (mk-sharded-domain fs dtmp2 domain-spec
                           [[(barr 0) (barr 10 10)]
                            [(barr 1) (barr 20 20)]
                            [(barr 2) (barr 30 30)]
                            [(barr 3) (barr 40 40)]] :version 1)

        ;; start edb and shutdown right away to get version 1 of both domains
        (.shutdown
         (mk-service-handler (read-global-config gtmp local-config "111") local-dir "111" nil))

        ;; create new version only for do-update domain
        ;; create version 1 for no-update (override to make sure it didn't reload this version)
        (mk-sharded-domain fs dtmp1 domain-spec
                           [[(barr 0) (barr 1)]
                            [(barr 1) (barr 2)]
                            [(barr 2) (barr 3)]
                            [(barr 3) (barr 4)]] :version 1)

        ;; create version 2 for do-update
        (mk-sharded-domain fs dtmp2 domain-spec
                           [[(barr 0) (barr 11 11)]
                            [(barr 1) (barr 22 22)]
                            [(barr 2) (barr 33 33)]
                            [(barr 3) (barr 44 44)]] :version 2)

        (let [handler (mk-service-handler (read-global-config gtmp local-config "222") local-dir "222" nil)]
          ;; domain no-update should not have changed
          (is (barr= (barr 0 0)
                     (get-val handler "no-update" (barr 0))))
          (is (barr= (barr 1 1)
                     (get-val handler "no-update" (barr 1))))
          (is (barr= (barr 2 2)
                     (get-val handler "no-update" (barr 2))))
          (is (barr= (barr 3 3)
                     (get-val handler "no-update" (barr 3))))

          ;; domain do-update should have changed
          (is (barr= (barr 11 11)
                     (get-val handler "do-update" (barr 0))))
          (is (barr= (barr 22 22)
                     (get-val handler "do-update" (barr 1))))
          (is (barr= (barr 33 33)
                     (get-val handler "do-update" (barr 2))))
          (is (barr= (barr 44 44)
                     (get-val handler "do-update" (barr 3))))

          (.shutdown handler))

        ;; now test with new version but different domain-spec
        (let [domain-spec-new {:num-shards 6 :persistence-factory (JavaBerkDB.)}]
          (delete fs dtmp2 true)
          (mk-sharded-domain fs dtmp2 domain-spec-new
                             [[(barr 0) (barr 55 55)]
                              [(barr 1) (barr 66 66)]
                              [(barr 2) (barr 77 77)]
                              [(barr 3) (barr 88 88)]] :version 3))

        (let [handler (mk-service-handler (read-global-config gtmp local-config "333") local-dir "333" nil)]
          (is (thrift/status-failed? (.getDomainStatus handler "do-update")))
          (is (thrift/status-ready? (.getDomainStatus handler "no-update")))
          (.shutdown handler)
        )))))



;; TODO: need to do something to prioritize hosts in tests (override get-priority-hosts)
(deftest test-multi-get
  (let [shards-to-pairs {0 [[(barr 0) (barr 0 0)]
                            [(barr 1) (barr 1 1)]
                            [(barr 2) nil]]
                         1 [[(barr 10) (barr 10 0)]]
                         2 [[(barr 20) (barr 20 0)]
                            [(barr 21) (barr 21 1)]
                            [(barr 22) nil]]
                         3 [[(barr 30) (barr 30 0)]]}
        domain-to-host-to-shards {"test1" {(local-hostname) [0 3]
                                           "host2" [1 0]
                                           "host3" [2 1]
                                           "host4" [3 2]}}]
    (with-presharded-domain
      ["test1"
       dpath
       (JavaBerkDB.)
       shards-to-pairs]
      (with-service-handler
        [elephant
         [(local-hostname) "host2"]
         {"test1" dpath}
         domain-to-host-to-shards
         ]
        (with-mocked-remote [domain-to-host-to-shards shards-to-pairs ["host4"]]          
          (is (barr=
               (barr 0 0)
               (get-val elephant "test1" (barr 0))))
          (is (barr=
               (barr 20 0)
               (get-val elephant "test1" (barr 20))))
          (is (=
               nil
               (get-val elephant "test1" (barr 2))))
          (is (barrs=
               [(barr 0 0) nil (barr 30 0)]
               (multi-get-vals elephant "test1" [(barr 0) (barr 22) (barr 30)])
               ))
          (is (barrs=
               [(barr 0 0) (barr 1 1) nil (barr 30 0) (barr 10 0)]
               (multi-get-vals elephant "test1" [(barr 0) (barr 1) (barr 2) (barr 30) (barr 10)])
               ))
          (is (= [] (multi-get-vals elephant "test1" [])))
          )
        (with-mocked-remote [domain-to-host-to-shards shards-to-pairs ["host3" "host4"]]
          (is (barrs=
               [(barr 0 0) (barr 10 0)]
               (multi-get-vals elephant "test1" [(barr 0) (barr 10)])
               ))
          (is (thrown?
               Exception
               (multi-get-vals elephant "test1" [(barr 0) (barr 22)])
               ))
          )
        ))))
