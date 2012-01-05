(ns elephantdb.keyval.core-test
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

(deftest test-basic
  (with-domain [domain (berkeley-spec 4)
                [[(barr 0) (barr 0 0)]
                 [(barr 1) (barr 1 1)]
                 [(barr 2) (barr 2 2)]]]
    (with-service-handler [elephant
                           [(u/local-hostname)]
                           {"test1" dpath}]
      (expected-domain-data elephant "test1"
                            0 [0 0]
                            1 [1 1]
                            2 [2 2]
                            3 nil))))
 
(fact "Domain should contain all input key-value pairs."
  (let [input-map {"key" "val"
                   "hey" "there"
                   1 2}]
    (with-domain [domain (berkeley-spec 4) input-map]
      (to-map domain) => input-map)))
