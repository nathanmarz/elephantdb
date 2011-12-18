(ns elephantdb.common.status
  (:import [elephantdb.store DomainStore])
  (:import [elephantdb.generated DomainStatus$_Fields
            DomainStatus]))

(defprotocol IStatus
  (ready?   [_] "Is the current status ready?")
  (failed?  [_] "Is the current status failed?")
  (loading? [_] "Is the current status loading?"))

(defrecord KeywordStatus [status])

(extend-type KeywordStatus
  IStatus
  (ready? [x]   (-> x :status (= :ready)))
  (failed? [x]  (-> x :status (= :failed)))
  (loading? [x] (-> x :status (= :loading))))

(extend-type DomainStatus
  IStatus
  (ready? [status]
    (= (.getSetField status) DomainStatus$_Fields/READY))
  
  (failed? [status]
    (= (.getSetField status) DomainStatus$_Fields/FAILED))

  (loading? [status]
    (let [field (.getSetField status)]
      (or (= field DomainStatus$_Fields/LOADING)
          (and (= field DomainStatus$_Fields/READY)
               (.get_update_status (.get_ready status)))))))


