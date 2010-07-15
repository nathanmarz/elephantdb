(ns elephantdb.thrift
  (:import [elephantdb.generated LoadingStatus DomainStatus DomainStatus$_Fields ReadyStatus
              FailedStatus ShutdownStatus
              DomainNotFoundException DomainNotLoadedException HostsDownException WrongHostException]))

(defn loading-status []
  (DomainStatus/loading (LoadingStatus.)))

(defn failed-status [ex]
  (DomainStatus/failed (FailedStatus. (str ex))))

(defn shutdown-status []
  (DomainStatus/shutdown (ShutdownStatus.)))

(defn ready-status [loading?]
  (DomainStatus/ready
    (doto (ReadyStatus.)
          (.set_update_status (when loading? (loading-status))))))

(defn status-ready? [#^DomainStatus domain-status]
  (= (.getSetField domain-status DomainStatus$_Fields/READY)))

(defn domain-not-found-ex [domain]
  (DomainNotFoundException. domain))

(defn domain-not-loaded-ex [domain]
  (DomainNotLoadedException. domain))

(defn wrong-host-ex []
  (WrongHostException.))

(defn hosts-down-ex [hosts]
  (HostsDownException. hosts))