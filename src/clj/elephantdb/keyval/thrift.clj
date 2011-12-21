(ns elephantdb.keyval.thrift
  "Functions for connecting the an ElephantDB (key-value) service via
  Thrift."
  (:require [elephantdb.common.status :as stat])
  (:import [org.apache.thrift.protocol TBinaryProtocol]
           [org.apache.thrift.transport TTransport
            TFramedTransport TSocket]
           [elephantdb.generated ElephantDB$Client
            DomainStatus$_Fields Value Status
            DomainNotFoundException DomainNotLoadedException
            HostsDownException WrongHostException
            DomainStatus LoadingStatus 
            ReadyStatus FailedStatus ShutdownStatus]))

;; ## Status and Errors

(defn loading-status []
  (DomainStatus/loading (LoadingStatus.)))

(defn failed-status [ex]
  (DomainStatus/failed (FailedStatus. (str ex))))

(defn shutdown-status []
  (DomainStatus/shutdown (ShutdownStatus.)))

(defn ready-status [& {:keys [loading?]}]
  (DomainStatus/ready
   (doto (ReadyStatus.)
     (.set_update_status (when loading?
                           (LoadingStatus.))))))

(extend-type DomainStatus
  status/IStatus
  (ready? [status]
    (= (.getSetField status) DomainStatus$_Fields/READY))
 
  (failed? [status]
    (= (.getSetField status) DomainStatus$_Fields/FAILED))

  (shutdown? [status]
    (= (.getSetField status) DomainStatus$_Fields/SHUTDOWN))
  
  (loading? [status]
    (boolean
     (or (= (.getSetField status) DomainStatus$_Fields/LOADING)
         (and (stat/ready? status)
              (.get_update_status (.get_ready status))))))

  status/IStateful
  (status [state] state)
  (to-ready [state] (ready-status))
  (to-failed [state msg] (failed-status msg))
  (to-shutdown [state] (shutdown-status))
  (to-loading [state] (if (stat/ready? state)
                        (ready-status :loading? true)
                        (loading-status))))


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

;; ## Thrift Connection

(defn thrift-transport [host port]
  (TFramedTransport. (TSocket. host port)))

(defn elephant-client [transport]
  (ElephantDB$Client. (TBinaryProtocol. transport)))

(defmacro with-elephant-connection [host port client-sym & body]
  `(with-open [^TTransport conn# (thrift-transport ~host ~port)]
     (let [^ElephantDB$Client ~client-sym (elephant-client conn#)]
       ~@body)))

