(ns elephantdb.common.thrift
  (:import [elephantdb.generated DomainStatus$_Fields
            DomainStatus LoadingStatus  ReadyStatus
            FailedStatus ShutdownStatus Value Status
            DomainNotFoundException DomainNotLoadedException
            HostsDownException WrongHostException]))

(defn loading-status []
  (DomainStatus/loading (LoadingStatus.)))

(defn failed-status [ex]
  (DomainStatus/failed (FailedStatus. (str ex))))

(defn shutdown-status []
  (DomainStatus/shutdown (ShutdownStatus.)))

(defn ready-status [& {:keys [loading?]}]
  (DomainStatus/ready
   (doto (ReadyStatus.)
     (.set_update_status (when loading? (LoadingStatus.))))))

(defn status-ready? [^DomainStatus domain-status]
  (= (.getSetField domain-status) DomainStatus$_Fields/READY))

(defn status-failed? [^DomainStatus domain-status]
  (= (.getSetField domain-status) DomainStatus$_Fields/FAILED))

(defn status-loading? [^DomainStatus domain-status]
  (let [field (.getSetField domain-status)]
    (or (= field DomainStatus$_Fields/LOADING)
        (and (= field DomainStatus$_Fields/READY)
             (.get_update_status (.get_ready domain-status))))))

(defn domain-not-found-ex [domain]
  (DomainNotFoundException. domain))

(defn domain-not-loaded-ex [domain]
  (DomainNotLoadedException. domain))

(defn wrong-host-ex []
  (WrongHostException.))

(defn hosts-down-ex [hosts]
  (HostsDownException. hosts))

(defn mk-value [val]
  (doto (Value.)
    (.set_data val)))

(defn elephant-status [domain-status-map]
  (Status. domain-status-map))
