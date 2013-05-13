(ns elephantdb.common.status)

;; Statuses are little state machines.

(defprotocol IStatus
  (ready? [_] "Am I ready?")
  (loading? [_] "Am I loading?")
  (failed?  [_] "Have I failed?")
  (shutdown? [_] "Am I shutting down?"))

(defprotocol IStateful
  (get-status [this] "Returns the current state object.")
  (to-ready [this] "Returns a new ready state.")
  (to-loading [this] "Returns a new loading-state.")
  (to-failed [this msg] "Returns a new failed state.")
  (to-shutdown [this] "Returns a new shutting-down state."))

(defrecord KeywordStatus [status]
  IStatus
  (ready? [x] (contains? #{:ready :updating}
                         (:status x)))
  (failed? [x]   (-> x :status (= :failed)))
  (shutdown? [x] (-> x :status (= :shutdown)))
  (loading? [x] (contains? #{:updating :loading}
                           (:status x)))
  
  IStateful
  (get-status [state] state)
  (to-ready   [state] (KeywordStatus. :ready))
  (to-failed  [state msg] (KeywordStatus. :failed))
  (to-shutdown [state] (KeywordStatus. :shutdown))
  (to-loading [state]
    (KeywordStatus. (if (ready? state)
                      :updating
                      :loading))))

(def updating?
  (every-pred loading? ready?))

(defn swap-status!
  "Accepts a stateful thing wrapped in an atom and a transition
  function (from the elephantdb.common.status.IStateful interface) and
  returns the new status."
  [status transition-fn & args]
  (apply swap! status transition-fn args))
