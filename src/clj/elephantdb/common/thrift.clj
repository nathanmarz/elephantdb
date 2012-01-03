(ns elephantdb.common.thrift
  (:require [jackknife.core :as u]
            [jackknife.logging :as log]
            [elephantdb.common.database :as db]
            [elephantdb.common.status :as status])
  (:import [org.apache.thrift7.protocol TBinaryProtocol$Factory]
           [org.apache.thrift7.server THsHaServer THsHaServer$Args]
           [org.apache.thrift7.transport TTransport
            TFramedTransport TSocket TNonblockingServerSocket]          
           [elephantdb.common.database Database]
           [elephantdb.generated ElephantDB
            ElephantDBShared$Iface ElephantDB$Processor
            DomainStatus$_Fields Status
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

(defn elephant-status [domain-status-map]
  (Status. domain-status-map))

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
         (and (status/ready? status)
              (.get_update_status (.get_ready status))))))

  status/IStateful
  (status [state] state)
  (to-ready [state] (ready-status))
  (to-failed [state msg] (failed-status msg))
  (to-shutdown [state] (shutdown-status))
  (to-loading [state] (if (status/ready? state)
                        (ready-status :loading? true)
                        (loading-status))))

(defn to-thrift [state]
  (condp #(%1 %2) state
    status/ready?    (ready-status)
    status/failed?   (failed-status)
    status/shutdown? (shutdown-status)
    status/loading?  (loading-status)
    status/updating? (ready-status :loading? true)))

(defn domain-not-found-ex [domain]
  (DomainNotFoundException. domain))

(defn domain-not-loaded-ex [domain]
  (DomainNotLoadedException. domain))

(defn wrong-host-ex []
  (WrongHostException.))

(defn hosts-down-ex [hosts]
  (HostsDownException. hosts))

(defn assert-domain
  "If the named domain doesn't exist in the supplied database, throws
  a DomainNotFoundException."
  [database domain-name]
  (when-not (db/domain-get database domain-name)
    (domain-not-found-ex domain-name)))

;; TODO: Fix this with a macro that lets us specify these behaviours
;; by default, but replace as necessary.

(defmacro defservice [])

(defn shared-interface [database]
  (reify ElephantDBShared$Iface
    (getDomainStatus [_ domain-name]
      "Returns the thrift status of the supplied domain-name."
      (assert-domain database domain-name)
      (status/status
       (db/domain-get database domain-name)))

    (getDomains [_]
      "Returns a sequence of all domain names being served."
      (db/domain-names database))

    (getStatus [_]
      "Returns a map of domain-name->status for each domain."
      (elephant-status
       (u/update-vals (db/domain->status database)
                      (fn [_ status] (to-thrift status)))))

    (isFullyLoaded [_]
      "Are all domains loaded properly?"
      (db/fully-loaded? database))

    (isUpdating [_]
      "Is some domain currently updating?"
      (db/some-updating? database))

    (update [_ domain-name]
      "If an update is available, updates the named domain and
         hotswaps the new version."
      (assert-domain database domain-name)
      (u/with-ret true
        (db/attempt-update! database domain-name)))

    (updateAll [_]
      "If an update is available on any domain, updates the domain's
         shards from its remote store and hotswaps in the new versions."
      (u/with-ret true
        (db/update-all! database)))))

;; ## Connections

(defn thrift-transport
  [host port]
  (TFramedTransport. (TSocket. host port)))

(defn thrift-server
  [service-handler port]
  (let [args (-> (TNonblockingServerSocket. port)
                 (THsHaServer$Args.)
                 (.workerThreads 64)
                 (.executorService 64)
                 (.protocolFactory (TBinaryProtocol$Factory.))
                 (.processor (ElephantDB$Processor. service-handler)))]
    (THsHaServer. args)))

(defn launch-database!
  [{:keys [port options] :as database}]
  (let [{interval :update-interval-s} options
        server (thrift-server database port)]
    (u/register-shutdown-hook #(.stop server))
    (log/info "Preparing database...")
    (db/prepare database)
    (log/info "Starting ElephantDB server...")
    (.serve server)))
