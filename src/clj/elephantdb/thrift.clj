(ns elephantdb.thrift
  (:import [elephantdb.generated LoadingStatus DomainStatus ReadyStatus FailedStatus]))

(defn loading-status []
  (DomainStatus/loading (LoadingStatus.)))

(defn failed-status [ex]
  (DomainStatus/failed (FailedStatus. (str ex))))

(defn ready-status [loading?]
  (DomainStatus/ready
    (doto (ReadyStatus.)
          (.set_update_status (when loading? (loading-status))))))
