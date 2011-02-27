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

(defn expected-domain-data [handler domain & key-value-pairs]
  (doseq [pair (partition 2 key-value-pairs)]
    (let [key (first pair)
          values (second pair)]
      (if-not (nil? values)
        (is (barr= (apply barr values)
                   (get-val handler domain (barr key))))
        (is (= values (get-val handler domain (barr key))))))))

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
      (expected-domain-data elephant "test1"
                            0 [0 0]
                            1 [1 1]
                            2 [2 2]
                            3 nil)
      )))

(deftest test-multi-domain
  (let [data1 (domain-data 0 [0 0]
                           10 [10 1]
                           20 [20 2]
                           30 [30 3])
        data2 (domain-data 5 [5 0]
                           15 [15 15]
                           105 [110])]

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
      1 (domain-data 10 [10 0])
      2 (domain-data 20 [20 0]
                     21 [21 1])
      3 (domain-data 30 [30 0])}]
    (with-service-handler
      [elephant
       [(local-hostname) "host2"]
       {"test1" dpath}
       {"test1" {(local-hostname) [0 2] "host2" [1 3]}}]

      (expected-domain-data elephant "test1"
                            0 [0 0]
                            20 [20 0]
                            2 nil)
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
                           (domain-data 0 [0 0]
                                        1 [1 1]
                                        2 [2 2]
                                        3 [3 3]))
        ;; NOTE: should really just to the read global config in mk-service-handler
        (.shutdown
         (mk-service-handler (read-global-config gtmp local-config "111") local-dir "111" nil))
        (mk-sharded-domain fs dtmp domain-spec
                           (domain-data 0 [0 1]
                                        3 [3 4]
                                        4 [4 5]))
        (let [handler (mk-service-handler (read-global-config gtmp local-config "111") local-dir "111" nil)]
          (expected-domain-data handler "test"
                                0 [0 0]
                                2 [2 2]
                                4 nil)
          (.shutdown handler))
        (let [handler (mk-service-handler (read-global-config gtmp local-config "112") local-dir "112" nil)]
          (expected-domain-data handler "test"
                                0 [0 1]
                                4 [4 5]
                                2 nil)
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
                           (domain-data 0 [0 0]
                                        1 [1 1]
                                        2 [2 2]
                                        3 [3 3]) :version 1)

        ;; create version 1 for do-update
        (mk-sharded-domain fs dtmp2 domain-spec
                           (domain-data 0 [10 10]
                                        1 [20 20]
                                        2 [30 30]
                                        3 [40 40]) :version 1)

        ;; start edb and shutdown right away to get version 1 of both domains
        (.shutdown
         (mk-service-handler (read-global-config gtmp local-config "111") local-dir "111" nil))

        ;; create new version only for do-update domain
        ;; create version 1 for no-update (override to make sure it didn't reload this version)
        (mk-sharded-domain fs dtmp1 domain-spec
                           (domain-data 0 [1]
                                        1 [2]
                                        2 [3]
                                        3 [4]) :version 1)

        ;; create version 2 for do-update
        (mk-sharded-domain fs dtmp2 domain-spec
                           (domain-data 0 [11 11]
                                        1 [22 22]
                                        2 [33 33]
                                        3 [44 44]) :version 2)

        (let [handler (mk-service-handler (read-global-config gtmp local-config "222") local-dir "222" nil)]
          ;; domain no-update should not have changed
          (expected-domain-data handler "no-update"
                                0 [0 0]
                                1 [1 1]
                                2 [2 2]
                                3 [3 3])

          ;; domain do-update should have changed
          (expected-domain-data handler "do-update"
                                0 [11 11]
                                1 [22 22]
                                2 [33 33]
                                3 [44 44])

          (.shutdown handler))

        ;; now test with new version but different domain-spec
        (let [domain-spec-new {:num-shards 6 :persistence-factory (JavaBerkDB.)}]
          (delete fs dtmp2 true)
          (mk-sharded-domain fs dtmp2 domain-spec-new
                             (domain-data 0 [55 55]
                                          1 [66 66]
                                          2 [77 77]
                                          3 [88 88]) :version 3))

        (let [handler (mk-service-handler (read-global-config gtmp local-config "333") local-dir "333" nil)]
          (is (thrift/status-failed? (.getDomainStatus handler "do-update")))
          (is (thrift/status-ready? (.getDomainStatus handler "no-update")))
          (.shutdown handler))

        ;; if we delete a domain from the global conf, it should
        ;; remove the local version of it too (delete dir), when starting up edb
        (delete fs gtmp) ;; delete config
        (write-clj-config! {:replication 1
                            :hosts [(local-hostname)]
                            :domains {"no-update" dtmp1}}
                           fs
                           gtmp)
        (let [handler (mk-service-handler (read-global-config gtmp local-config "444") local-dir "444" nil)
              deleted-domain-path (.pathToFile lfs (path local-dir "do-update"))]
          (is (= 1 (.size (.getDomains handler))))
          (is (= "no-update" (first (.getDomains handler))))
          ;; make sure local path of domain has been deleted:
          (is (= false (.exists deleted-domain-path)))
          (.shutdown handler))
        ))))



;; TODO: need to do something to prioritize hosts in tests (override get-priority-hosts)
(deftest test-multi-get
  (let [shards-to-pairs {0 (domain-data 0 [0 0]
                                        1 [1 1]
                                        2 nil)
                         1 (domain-data 10 [10 0])
                         2 (domain-data 20 [20 0]
                                        21 [21 1]
                                        22 nil)
                         3 (domain-data 30 [30 0])}
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
          (expected-domain-data elephant "test1"
                                0 [0 0]
                                20 [20 0]
                                2 nil)
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

(deftest test-live-updating
  (with-local-tmp [lfs local-dir]
    (with-fs-tmp [fs dtmp1 dtmp2 gtmp]
      (let [domain-spec {:num-shards 4 :persistence-factory (JavaBerkDB.)}
            local-config (mk-local-config local-dir)]
        (write-clj-config! {:replication 1
                            :hosts [(local-hostname)]
                            :domains {"domain1" dtmp1 "domain2" dtmp2}}
                           fs
                           gtmp)

        ;; create version 1 for domain1
        (mk-sharded-domain fs dtmp1 domain-spec
                           (domain-data 0 [0 0]
                                        1 [1 1]
                                        2 [2 2]
                                        3 [3 3]) :version 1)

        ;; create version 1 for domain2
        (mk-sharded-domain fs dtmp2 domain-spec
                           (domain-data 0 [10 10]
                                        1 [20 20]
                                        2 [30 30]
                                        3 [40 40]) :version 1)

        ;; start up edb service
        (let [handler (mk-service-handler (read-global-config gtmp local-config "111") local-dir "111" nil)]

          ;; create version 2 for domain2
          (mk-sharded-domain fs dtmp2 domain-spec
                             (domain-data 0 [11 11]
                                          1 [22 22]
                                          2 [33 33]
                                          3 [44 44]) :version 2)

          ;; domain1 should not have changed
          (expected-domain-data handler "domain1"
                                0 [0 0]
                                1 [1 1]
                                2 [2 2]
                                3 [3 3])

          ;; domain2 should not have changed either
          (expected-domain-data handler "domain2"
                                0 [10 10]
                                1 [20 20]
                                2 [30 30]
                                3 [40 40])

          ;; nothing should happen for domain1
          (.update handler "domain1")

          ;; domain1 should not have changed
          (expected-domain-data handler "domain1"
                                0 [0 0]
                                1 [1 1]
                                2 [2 2]
                                3 [3 3])

          ;; updating domain2 should cause update and new values being returned
          (.update handler "domain2")

          ;; domain2 should have changed
          (expected-domain-data handler "domain2"
                                0 [11 11]
                                1 [21 21]
                                2 [31 31]
                                3 [41 41])

          ;; create version 2 for domain1
          (mk-sharded-domain fs dtmp1 domain-spec
                             (domain-data 0 [1 1]
                                          1 [2 2]
                                          2 [3 3]
                                          3 [4 4]) :version 2)

          ;; create version 3 for domain2
          (mk-sharded-domain fs dtmp2 domain-spec
                             (domain-data 0 [12 12]
                                          1 [23 23]
                                          2 [34 34]
                                          3 [45 45]) :version 3)

          ;; force update of all domains
          (.updateAll handler)

          ;; domain1 and domain 2 should have changed
          (expected-domain-data handler "domain1"
                                0 [1 1]
                                1 [2 2]
                                2 [3 3]
                                3 [4 4])

          (expected-domain-data handler "domain2"
                                0 [12 12]
                                1 [23 23]
                                2 [34 34]
                                3 [45 45])

          (.shutdown handler))
        ))))