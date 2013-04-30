(ns elephantdb.common.database
  (:use [metrics.timers :only (timer)])
  (:require [hadoop-util.core :as h]
            [jackknife.core :as u]
            [jackknife.seq :as seq]
            [jackknife.logging :as log]
            [elephantdb.common.domain :as domain]
            [elephantdb.common.status :as status])
  (:import [elephantdb.persistence Shutdownable]
           [java.io File]))

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

(def domain-names
  "Returns a sequence of all domain names for which the supplied
   database is responsible."
  (comp keys :domains))

(defn attempt-update!
  "If an update is available, updates the named domain and hotswaps
   the new version. Returns a future."
  [database domain-name]
  (when-let [domain (domain-get database domain-name)]
    (domain/attempt-update! domain)))

(defn update-all!
  "If an update is available on any domain, updates the domain's
  shards from its remote store and hotswaps in the new versions."
  [database]
  (doseq [domain (domain-names database)]
    (domain/attempt-update! (domain-get database domain))))

(defn fully-loaded?
  [{:keys [domains]}]
  (every? (some-fn status/ready? status/failed?)
          (vals domains)))

(defn some-loading?
  [{:keys [domains]}]
  (let [domains (vals domains)]
    (seq/some? status/loading? (map status/get-status domains))))

(defn domain->status
  "Returns a map of domain name -> status."
  [{:keys [domains]}]
  (u/val-map status/get-status domains))

(defn purge-unused-domains!
  "Walks through the supplied local directory, recursively deleting
   all directories with names that aren't present in the supplied
   `domains`."
  [local-root name-seq]
  (letfn [(domain? [^File path]
            (and (.isDirectory path)
                 (not (contains? (into #{} name-seq)
                                 (.getName path)))))]
    (u/dofor [^File domain-path (-> local-root h/mk-local-path .listFiles)
              :when (domain? domain-path)]
             (log/info "Destroying un-served domain at: " domain-path)
             (h/delete (h/local-filesystem)
                       (.getPath domain-path)
                       true))))

(defn launch-updater!
  [database interval-secs]
  (let [interval-ms (* 1000 interval-secs)
        updater (future
                  (log/info "Starting updater process with an"
                            " interval of: " interval-secs " seconds.")
                  (while true
                    (log/debug "Updater process: firing update on all domains.")
                    (update-all! database)
                    (Thread/sleep interval-ms)))]
    (u/register-shutdown-hook #(do (log/info "Killing updater...")
                                   (future-cancel updater)))
    updater))

;; ## Database Creation
;;
;; A "database" is the initial configuration map with much more detail
;; about each individual domain. The `build-database` function swaps
;; out each domain's remote path for a populated `Domain` record with
;; all 

(defprotocol Preparable
  (prepare [_] "Perform preparatory steps."))

(defrecord Database [local-root port domains metrics options]
  Preparable
  (prepare [this]
    (log/info "Preparing database...")
    (u/register-shutdown-hook #(.shutdown this))
    (future
      (purge-unused-domains! local-root (keys domains))
      (doseq [domain (vals domains)]
        (domain/boot-domain! domain))))

  Shutdownable
  (shutdown [_]
    (log/info "ElephantDB received shutdown notice...")
    (doseq [^Shutdownable domain (vals domains)]
      (.shutdown domain))))

(defn build-meters [domain-name]
  (let [domain-name (clojure.string/replace domain-name #"-" "_")
        hostname (clojure.string/replace (.getCanonicalHostName (java.net.InetAddress/getLocalHost)) #"\." "_")]
    {:direct-get-response-time (timer [(str hostname ".elephantdb.domain") domain-name "direct_get_response_time"])
     :multi-get-response-time (timer [(str hostname ".elephantdb.domain") domain-name "multi_get_response_time"])}))

(defn metrics-get [{:keys [metrics]} domain-name]
  (get metrics domain-name))

(defn build-database
  "Returns a database linking to a bunch of read-only domains."
  [{:keys [domains port local-root] :as conf-map}]
  (let [throttle (domain/throttle (:download-rate-limit conf-map))
        options   (select-keys conf-map [:hosts :replication :hdfs-conf
                                         :remote-path :throttle])
        options   (into {} (remove (comp nil? second) options))]
    (Database. local-root
               (or port 3578) ;; TODO: Merge this default in elsewhere.
               (u/update-vals
                domains
                (fn [domain-name remote-path]
                  (let [local-path (domain-path local-root domain-name)]
                    (apply domain/build-domain local-path
                           :remote-path remote-path
                           (apply concat options)))))
               (u/update-vals
                domains
                (fn [domain-name _]
                  (build-meters domain-name)))
               (dissoc conf-map :domains :local-root :port))))

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
