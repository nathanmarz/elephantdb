(ns elephantdb.thrift
  (:import [elephantdb.generated LoadingStatus DomainStatus DomainStatus$_Fields ReadyStatus
              FailedStatus ShutdownStatus ElephantDB$Client Value Status
              DomainNotFoundException DomainNotLoadedException HostsDownException WrongHostException])
  (:import [org.apache.thrift.protocol TBinaryProtocol TProtocol])
  (:import [org.apache.thrift.transport TTransport TFramedTransport TSocket])
  )

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
  (= (.getSetField domain-status) DomainStatus$_Fields/READY))

(defn status-failed? [#^DomainStatus domain-status]
  (= (.getSetField domain-status) DomainStatus$_Fields/FAILED))

(defn domain-not-found-ex [domain]
  (DomainNotFoundException. domain))

(defn domain-not-loaded-ex [domain]
  (DomainNotLoadedException. domain))

(defn wrong-host-ex []
  (WrongHostException.))

(defn hosts-down-ex [hosts]
  (HostsDownException. hosts))

(defn mk-value [val]
  (doto (Value.) (.set_data val)))

(defn elephant-status [domain-status-map]
  (Status. domain-status-map))

(defn elephant-client-and-conn [host port]
  (let [transport (TFramedTransport. (TSocket. host port))
        prot (TBinaryProtocol. transport)
        client (ElephantDB$Client. prot)]
        (.open transport)
        [client transport] ))

(defmacro with-elephant-connection [host port client-sym & body]
  `(let [[#^ElephantDB$Client ~client-sym #^TTransport conn#] (elephant-client-and-conn ~host ~port)]
      (try
        ~@body
      (finally (.close conn#)))
    ))
