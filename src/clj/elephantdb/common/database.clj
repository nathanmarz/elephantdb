(ns elephantdb.common.database
  (:require [hadoop-util.core :as h]
            [jackknife.core :as u]
            [jackknife.logging :as log]
            [elephantdb.common.domain :as domain]
            [elephantdb.common.status :as status])
  (:import [elephantdb.persistence Shutdownable]))

;; ## Database Manipulation Functions

(defn- domain-path
  "Returns the root path that should be used for a domain with the
  supplied name located within the supplied elephantdb root
  directory."
  [local-root domain-name]
  (str local-root "/" domain-name))

(defn domain-get
  "Retrieves the requested domain (by name) from the supplied
  database."
  [{:keys [domains]} domain-name]
  (get domains domain-name))

(defn attempt-update!
  "If an update is available, updates the named domain and hotswaps
   the new version. Returns a future."
  [database domain-name]
  (future
    (when-let [domain (domain-get database domain-name)]
      (domain/attempt-update! domain))))

(defn update-all!
  "If an update is available on any domain, updates the domain's
  shards from its remote store and hotswaps in the new versions."
  [database]
  (future
    (u/do-pmap domain/attempt-update!
               (vals (:domains database)))))

(defn fully-loaded?
  [{:keys [domains]}]
  (every? (some-fn status/ready? status/failed?)
          (vals domains)))

(defn some-updating?
  [{:keys [domains]}]
  (let [domains (vals domains)]
    (some status/loading? (map status/status domains))))

(defn domain->status
  "Returns a map of domain name -> status."
  [{:keys [domains]}]
  (u/val-map status/status domains))

(def domain-names
  "Returns a sequence of all domain names for which the supplied
   database is responsible."
  (comp keys :domains))

(defn purge-unused-domains!
  "Walks through the supplied local directory, recursively deleting
   all directories with names that aren't present in the supplied
   `domains`."
  [local-root name-seq]
  (letfn [(domain? [path]
            (and (.isDirectory path)
                 (not (contains? (into #{} name-seq)
                                 (.getName path)))))]
    (u/dofor [domain-path (-> local-root h/mk-local-path .listFiles)
              :when (domain? domain-path)]
             (log/info "Destroying un-served domain at: " domain-path)
             (h/delete (h/local-filesystem)
                       (.getPath domain-path)
                       true))))

(defn launch-updater!
  [database]
  (let [interval-secs (get-in database [:options :update-intervals-s])
        interval-ms   (* 1000 interval-secs)]
    (future
      (log/info (format "Starting updater process with an interval of: %s seconds."
                        interval-secs))
      (while true
        (Thread/sleep interval-ms)
        (log/info "Updater process: firing update on all domains.")
        (update-all! database)))))

;; ## Database Creation
;;
;; A "database" is the initial configuration map with much more detail
;; about each individual domain. The `build-database` function swaps
;; out each domain's remote path for a populated `Domain` record with
;; all 

(defprotocol Preparable
  (prepare [_] "Perform preparatory steps."))

(defrecord Database [local-root port domains options]
  Preparable
  (prepare [{:keys [local-root domains] :as database}]
    (launch-updater! database)
    (u/register-shutdown-hook #(.shutdown database))
    (future
      (purge-unused-domains! local-root
                             (keys domains))
      (doseq [domain (vals domains)]
        (domain/boot-domain! domain))))

  Shutdownable
  (shutdown [this]
    (log/info "ElephantDB received shutdown notice...")
    (doseq [^Shutdownable domain (vals (:domains this))]
      (.shutdown domain))))

(defn build-database
  [{:keys [domains hosts replication port local-root hdfs-conf] :as conf-map}]
  (let [throttle (domain/throttle (:download-rate-limit conf-map))]
    (Database. local-root
               port
               (u/update-vals
                domains
                (fn [domain-name remote-path]
                  (let [local-path (domain-path local-root domain-name)]
                    (domain/build-domain local-root
                                         :hosts hosts
                                         :replication replication
                                         :hdfs-conf hdfs-conf
                                         :remote-path remote-path
                                         :throttle throttle))))
               (dissoc conf-map
                       :domains :local-root :port))))

;; A full database ends up looking something like the commented out
;; block below. Previously, a large number of functions would try and
;; update all databases at once. With Clojure's concurrency mechanisms
;; we can treat each domain as its own thing and dispatch futures to
;; take care of each in turn.

(comment
  {:replication 1
   :port 3578
   :download-rate-limit 1024
   :local-root "/Users/sritchie/Desktop/domainroot"
   :hosts ["localhost"]
   :hdfs-conf {"fs.default.name"
               "hdfs://hadoop-devel-nn.local.twitter.com:8020"}
   :blob-conf {"fs.default.name"
               "hdfs://hadoop-devel-nn.local.twitter.com:8020"}
   :domains {"graph" {:remoteStore <remote-domain-store>
                      :localStore <local-domain-store>
                      :serializer <serializer>
                      :status <status-atom>
                      :shardIndex {:hosts->shards {}
                                   :shard->host {}}
                      :domainData (atom {:version 123534534
                                         :shards {1 <persistence>
                                                  3 <persistence>}})}

             "docs" {:remoteStore <remote-domain-store>
                     :localStore <local-domain-store>
                     :serializer <serializer>
                     :status <status-atom>
                     :shardIndex {:hosts->shards {}
                                  :shard->host {}}
                     :domainData (atom {:version 123534534
                                        :shards {1 <persistence>
                                                 3 <persistence>}})}}})
