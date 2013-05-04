(ns elephantdb.ui.thrift
  (:import [elephantdb.generated Status DomainStatus DomainStatus$_Fields
            LoadingStatus ReadyStatus FailedStatus ShutdownStatus]))

(defn domain-status->elem [^DomainStatus status]
  (cond
   (= (.getSetField status) (DomainStatus$_Fields/READY)) [:span {:class "label label-success"} "Ready"]
   (= (.getSetField status) (DomainStatus$_Fields/LOADING)) [:span {:class "label label-info"} "Loading"]
   (= (.getSetField status) (DomainStatus$_Fields/FAILED)) [:span {:class "label label-error"} "Failed"]
   (= (.getSetField status) (DomainStatus$_Fields/SHUTDOWN)) [:span {:class "label label-warning"} "Shutdown"]))
