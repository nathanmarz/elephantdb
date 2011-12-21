(ns elephantdb.common.status
  (:require [elephantdb.common.thrift :as t])
  (:import [elephantdb.store DomainStore]
           [elephantdb.generated DomainStatus$_Fields
            DomainStatus LoadingStatus ReadyStatus
            FailedStatus ShutdownStatus]))

;; Statuses are little state machines.

(defprotocol IStatus
  (ready? [_] "Am I ready?")
  (loading? [_] "Am I loading?")
  (failed?  [_] "Have I failed?")
  (shutdown? [_] "Am I shutting down?"))

(defprotocol IStateful
  (status [this] "Returns the current state object.")
  (to-ready [this] "Returns a new ready state.")
  (to-loading [this] "Returns a new loading-state.")
  (to-failed [this msg] "Returns a new failed state.")
  (to-shutdown [this] "Returns a new shutting-down state."))

(defrecord KeywordStatus [status]
  IStatus
  (ready? [x]    (-> x :status (= :ready)))
  (failed? [x]   (-> x :status (= :failed)))
  (shutdown? [x] (-> x :status (= :shutdown)))
  (loading? [x] (contains? #{:updating :loading}
                           (:status x)))
  
  IStateful
  (status [state] state)
  (to-ready   [state] (KeywordStatus. :ready))
  (to-failed  [state msg] (KeywordStatus. :failed))
  (to-shutdown [state] (KeywordStatus. :shutdown))
  (to-loading [state] (KeywordStatus. (if (ready? status)
                                        :updating
                                        :loading))))


