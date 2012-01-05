(ns elephantdb.keyval.thrift-test
  (:use clojure.test
        midje.sweet
        elephantdb.keyval.testing
        elephantdb.common.testing
        [elephantdb.keyval.domain :only (to-map)]
        [elephantdb.common.config :only (read-global-config)])
  (:require [hadoop-util.core :as h]
            [hadoop-util.test :as t]
            [jackknife.core :as u]
            [jackknife.logging :as log]
            [elephantdb.common.config :as conf]
            [elephantdb.common.status :as status])
  (:import [elephantdb.persistence JavaBerkDB]))


(defn get-val [elephant d k]
  (.get_data (.get elephant d k)))

(defn direct-get-val [elephant d k]
  (.get_data (first (.directMultiGet elephant d [k]))))

(defn multi-get-vals [elephant domain keys]
  (map (memfn get_data)
       (.multiGet elephant domain keys)))

(defmacro expected-domain-data [handler domain & key-value-pairs]
  `(doseq [[key-sym# val-sym#] (partition 2 [~@key-value-pairs])]
     (if (seq val-sym#)
       (is (barr= (apply barr val-sym#)
                  (get-val ~handler ~domain (barr key-sym#))))
       (is (= val-sym# (get-val ~handler ~domain (barr key-sym#)))))))

(fact "test multiple domains."
  (let [data1 {0 [0 0]
               10 [10 1]
               20 [20 2]
               30 [30 3]}
        data2 {5 [5 0]
               15 [15 15]
               105 [110]}]
    (with-domain   [dpath1 (berkeley-spec 2) data1]
      (with-domain [dpath2 (berkeley-spec 3) data2]
        (with-single-service-handler [handler {"d1" dpath1
                                               "d2" dpath2}]
          (check-domain "d1" handler data1)
          (check-domain "d2" handler data2)
          (check-domain-not "d1" handler data2)
          (check-domain-not "d2" handler data1))))))

(fact "test multiple servers."
  (with-sharded-domain ["test1"
                        dpath
                        (JavaBerkDB.)
                        {0 {(barr 0) (barr 0 0)
                            (barr 1) (barr 1 1)
                            (barr 2) nil}
                         1 {10 [10 0]}
                         2 {20 [20 0]
                            21 [21 1]}
                         3 {30 [30 0]}}]
    (with-service-handler [elephant
                           [(u/local-hostname) "host2"]
                           {"test1" dpath}
                           {(u/local-hostname) [0 2] "host2" [1 3]}]
      (expected-domain-data elephant "test1"
                            0 [0 0]
                            20 [20 0]
                            2 nil)
      (direct-get-val elephant "test1" (barr 10)) => (throws Exception))))

(deftest test-update-synched
  (with-local-tmp [lfs local-dir]
    (t/with-fs-tmp [fs dtmp1 dtmp2 gtmp]
      (let [domain-spec {:num-shards 4 :coordinator (JavaBerkDB.)}
            local-config (mk-local-config local-dir)]
        (conf/write-clj-config! {:replication 1
                                 :hosts [(u/local-hostname)]
                                 :domains {"no-update" dtmp1 "do-update" dtmp2}}
                                fs
                                gtmp)

        ;; create version 1 for no-update
        (mk-sharded-domain dtmp1 domain-spec
                           (domain-data 0 [0 0]
                                        1 [1 1]
                                        2 [2 2]
                                        3 [3 3]) :version 1)

        ;; create version 1 for do-update
        (mk-sharded-domain dtmp2 domain-spec
                           (domain-data 0 [10 10]
                                        1 [20 20]
                                        2 [30 30]
                                        3 [40 40]) :version 1)

        ;; start edb and shutdown right away to get version 1 of both
        ;; domains
        (-> (read-global-config gtmp local-config)
            (mk-service-handler local-dir nil)
            (.shutdown))

        ;; create new version only for do-update domain
        ;;
        ;; create version 1 for no-update (override to make sure it
        ;; didn't reload this version)
        (mk-sharded-domain dtmp1 domain-spec
                           (domain-data 0 [1]
                                        1 [2]
                                        2 [3]
                                        3 [4]) :version 1)

        ;; create version 2 for do-update
        (mk-sharded-domain dtmp2 domain-spec
                           (domain-data 0 [11 11]
                                        1 [22 22]
                                        2 [33 33]
                                        3 [44 44]) :version 2)

        (let [handler (-> (read-global-config gtmp local-config)
                          (mk-service-handler local-dir nil))]
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
        (let [domain-spec-new {:num-shards 6 :coordinator (JavaBerkDB.)}]
          (h/delete fs dtmp2 true)
          (mk-sharded-domain dtmp2 domain-spec-new
                             (domain-data 0 [55 55]
                                          1 [66 66]
                                          2 [77 77]
                                          3 [88 88]) :version 3))

        (let [handler (-> (read-global-config gtmp local-config)
                          (mk-service-handler local-dir nil))]
          (facts
            (.getDomainStatus handler "do-update") => status/failed?
            (.getDomainStatus handler "no-update") => status/ready?)
          (.shutdown handler))
        
        ;; if we delete a domain from the global conf, it should
        ;; remove the local version of it too (delete dir), when starting up edb
        (h/delete fs gtmp) ;; delete config
        (conf/write-clj-config! {:replication 1
                                 :hosts [(u/local-hostname)]
                                 :domains {"no-update" dtmp1}}
                                fs
                                gtmp)
        (let [handler (-> (read-global-config gtmp local-config)
                          (mk-service-handler local-dir nil))
              deleted-domain-path (.pathToFile lfs (h/path local-dir "do-update"))]
          (facts
            (.size (.getDomains handler)) => 1
            (first (.getDomains handler))  => "no-update")
            
          "make sure local path of domain has been deleted:"
          (fact
            (.exists deleted-domain-path) => false)
          (.shutdown handler))))))

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
        domain-to-host-to-shards {"test1" {(u/local-hostname) [0 3]
                                           "host2" [1 0]
                                           "host3" [2 1]
                                           "host4" [3 2]}}]
    (with-presharded-domain ["test1" dpath (JavaBerkDB.) shards-to-pairs]
      (with-service-handler [elephant
                             [(u/local-hostname) "host2"]
                             {"test1" dpath}
                             {(u/local-hostname) [0 3]
                              "host2" [1 0]
                              "host3" [2 1]
                              "host4" [3 2]}]
        (with-mocked-remote [domain-to-host-to-shards shards-to-pairs ["host4"]]
          (expected-domain-data elephant
                                "test1"
                                0 [0 0]
                                20 [20 0]
                                2 nil)
          (is (barrs= [[0 0] nil [30 0]]
                      (multi-get-vals elephant "test1" [0 22 30])))
          (is (barrs= [[0 0] [1 1] nil [30 0] [10 0]]
                      (multi-get-vals elephant "test1" [0 1 2 30 10])))
          (is (= [] (multi-get-vals elephant "test1" []))))
        (with-mocked-remote [domain-to-host-to-shards shards-to-pairs ["host3" "host4"]]
          (is (barrs= [[0 0] [10 0]]
                      (multi-get-vals elephant "test1" [0 10])))
          (is (thrown? Exception (multi-get-vals elephant "test1" [0 22]))))))))

(deftest test-live-updating
  (with-local-tmp [lfs local-dir]
    (t/with-fs-tmp [fs dtmp1 dtmp2 gtmp]
      (let [domain-spec {:num-shards 4 :coordinator (JavaBerkDB.)}
            local-config (mk-local-config local-dir)]
        (conf/write-clj-config! {:replication 1
                                 :hosts [(u/local-hostname)]
                                 :domains {"domain1" dtmp1 "domain2" dtmp2}}
                                fs
                                gtmp)

        ;; create version 1 for domain1
        (mk-sharded-domain dtmp1 domain-spec
                           (domain-data 0 [0 0]
                                        1 [1 1]
                                        2 [2 2]
                                        3 [3 3]) :version 1)

        ;; create version 1 for domain2
        (mk-sharded-domain dtmp2 domain-spec
                           (domain-data 0 [10 10]
                                        1 [20 20]
                                        2 [30 30]
                                        3 [40 40]) :version 1)

        ;; start up edb service
        (let [handler (-> (read-global-config gtmp local-config)
                          (mk-service-handler local-dir nil))]

          ;; create version 2 for domain2
          (mk-sharded-domain dtmp2 domain-spec
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
          

          ;; wait a bit
          (while (.isUpdating handler)
            (Thread/sleep 100))
          
          ;; domain1 should not have changed
          (expected-domain-data handler "domain1"
                                0 [0 0]
                                1 [1 1]
                                2 [2 2]
                                3 [3 3])

          ;; updating domain2 should cause update and new values being returned
          (.update handler "domain2")

          ;; wait a bit
          (while (.isUpdating handler)
            (Thread/sleep 100))

          ;; domain2 should have changed
          (expected-domain-data handler "domain2"
                                0 [11 11]
                                1 [22 22]
                                2 [33 33]
                                3 [44 44])

          ;; create version 2 for domain1
          (mk-sharded-domain dtmp1 domain-spec
                             (domain-data 0 [1 1]
                                          1 [2 2]
                                          2 [3 3]
                                          3 [4 4]) :version 2)

          ;; create version 3 for domain2
          (mk-sharded-domain dtmp2 domain-spec
                             (domain-data 0 [12 12]
                                          1 [23 23]
                                          2 [34 34]
                                          3 [45 45]) :version 3)

          ;; force update of all domains
          (.updateAll handler)

          (facts
            (.getDomainStatus handler "domain1") => status/loading?
            (.getDomainStatus handler "domain2") => status/loading? 

            (.getDomainStatus handler "domain1") => status/ready? 
            (.getDomainStatus handler "domain2") => status/ready?) 

          ;; wait a bit
          (while (.isUpdating handler)
            (Thread/sleep 100))
          
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

          (is (status/ready? (.getDomainStatus handler "domain1")))
          (is (status/ready? (.getDomainStatus handler "domain2")))

          ;; make sure the old versions have been deleted locally
          (let [domain1-old-path1
                (.pathToFile lfs (h/path (h/str-path local-dir "domain1" "1")))
                domain2-old-path1
                (.pathToFile lfs (h/path (h/str-path local-dir "domain2" "1")))
                domain2-old-path2
                (.pathToFile lfs (h/path (h/str-path local-dir "domain2" "2")))]
            (facts
              (.exists domain1-old-path1) => false
              (.exists domain2-old-path1) => false
              (.exists domain2-old-path2) => false))
          (.shutdown handler))))))
