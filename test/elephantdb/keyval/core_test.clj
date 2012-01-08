(ns elephantdb.keyval.core-test
  (:use midje.sweet
        elephantdb.test.common
        elephantdb.test.keyval
        [elephantdb.keyval.domain :only (to-map)])
  (:require [elephantdb.common.status :as status])
  (:import [elephantdb.persistence JavaBerkDB]
           [elephantdb.document KeyValDocument]))

(defn mk-docseq [m]
  (for [[k v] m]
    (KeyValDocument. k v)))

(fact "Basic tests."
  (let [docs {0 (mk-docseq {(barr 0) (barr 0 0)
                            (barr 2) (barr 2 2)})
              1 (mk-docseq {(barr 1) (barr 1 1)})}]
    (with-service-handler [handler
                           {"test1" docs}
                           :conf-map {:update-interval-s 0.01}]
      (.getDomains handler)              => ["test1"]
      (.getDomainStatus handler "test1") => status/ready?

      "TODO: Update this after looking at Midje's collection
        checkers."
      "Is every status ready, once we have a service handler?"
      (vals
       (.get_domain_statuses
        (.getStatus handler))) => (partial every? status/ready?)

        "Test of directMultiGet."
        (letfn [(get-vals [keys]
                  (map #(.get_data %)
                       (.directMultiGet handler "test1" keys)))]
          (get-vals [(barr 2)]) => [(barr 0 0)]))))

(fact "Domain should contain all input key-value pairs."
  (let [input-map {"key" "val"
                   "hey" "there"
                   1 2}]
    (with-domain [domain (berkeley-spec 4) input-map]
      (to-map domain) => input-map)))
