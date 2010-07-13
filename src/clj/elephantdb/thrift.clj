(ns elephantdb.thrift
  (:import [elephantdb.generated LoadingStatus DomainStatus ReadyStatus]))

(defn loading-status []
  (DomainStatus/loading (LoadingStatus.)))

(defn ready-status [loading?]
  (DomainStatus/ready
    (doto (ReadyStatus.)
          (.set_update_status (when loading? (loading-status))))))
