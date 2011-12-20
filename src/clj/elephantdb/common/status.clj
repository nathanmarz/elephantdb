(ns elephantdb.common.status
  (:require [elephantdb.common.thrift :as t])
  (:import [elephantdb.store DomainStore]
           [elephantdb.generated DomainStatus$_Fields
            DomainStatus LoadingStatus ReadyStatus
            FailedStatus ShutdownStatus]))

;; Statuses should be like state machines; I want some way to toggle
;; statuses in a functional manner.

(defprotocol IStatus
  (ready?   [_] "Is the current status ready?")
  (loading? [_] "Is the current status loading?")
  (updating? [_] "Is the current status updating?")
  (failed?  [_] "Is the current status failed?")
  (shutdown? [_] "Is the current status shutdown?"))

(defprotocol IStateful
  (status [this] "Returns the current state object.")
  (to-ready [this] "Returns a new ready state.")
  (to-loading [this] "Returns a new loading-state.")
  (to-updating [this] "Returns a new updating-state.")
  (to-failed [this msg] "Returns a new failed state.")
  (to-shutdown [this] "Returns a new shutting-down state."))

(defrecord KeywordStatus [status])

(extend-type KeywordStatus
  IStatus
  (ready? [x]   (-> x :status (= :ready)))
  (loading? [x] (-> x :status (= :loading)))
  (failed? [x]  (-> x :status (= :failed)))
  (shutdown? [x]  (-> x :status (= :shutdown)))
  
  IStateful
  (status [state] state)
  (to-ready   [state] (KeywordStatus. :ready))
  (to-loading [state] (KeywordStatus. :loading))
  (to-failed  [state msg] (KeywordStatus. :failed))
  (to-shutdown [state] (KeywordStatus. :shutdown)))

(extend-type DomainStatus
  IStatus
  (loading? [status]
    (= (.getSetField status) DomainStatus$_Fields/LOADING))
  
  (ready? [status]
    (= (.getSetField status) DomainStatus$_Fields/READY))

  (updating? [status]
    (boolean
     (and (ready? status)
          (.get_update_status (.get_ready status)))))
 
  (failed? [status]
    (= (.getSetField status) DomainStatus$_Fields/FAILED))

  (shutdown? [status]
    (= (.getSetField status) DomainStatus$_Fields/SHUTDOWN))

  IStateful
  (status [state] state)
  (to-loading [state] (t/loading-status))
  (to-ready [state] (t/ready-status))
  (to-updating [state] (t/ready-status :updating? true))
  (to-failed [state msg] (t/failed-status msg))
  (to-shutdown [state] (t/shutdown-status)))
