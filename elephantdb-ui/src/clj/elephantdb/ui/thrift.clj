(ns elephantdb.ui.thrift
  (:import [elephantdb.generated Status DomainStatus DomainStatus$_Fields
            LoadingStatus ReadyStatus FailedStatus ShutdownStatus
            DomainSpec DomainMetaData]))

(defn domain-status->elem [^DomainStatus status]
  (cond
   (= (.getSetField status) (DomainStatus$_Fields/READY)) [:span {:class "label label-success"} "Ready"]
   (= (.getSetField status) (DomainStatus$_Fields/LOADING)) [:span {:class "label label-info"} "Loading"]
   (= (.getSetField status) (DomainStatus$_Fields/FAILED)) [:span {:class "label label-error"} "Failed"]
   (= (.getSetField status) (DomainStatus$_Fields/SHUTDOWN)) [:span {:class "label label-warning"} "Shutdown"]))

(defn expand-domain-spec [spec]
  [(.get_num_shards spec)
   (.get_coordinator spec)
   (.get_shard_scheme spec)])

(defn expand-domain-metadata [metadata]
  (let [spec (.get_domain_spec metadata)]
    (concat [(.get_remote_version metadata)
             (.get_local_version metadata)]
            (expand-domain-spec spec))))
