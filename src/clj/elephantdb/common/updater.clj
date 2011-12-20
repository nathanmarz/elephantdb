;; This is all quite dirty; this needs to work in parallel with the
;; hadoop throttled downloader, etc.

(ns elephantdb.common.updater
  (:require [clojure.string :as s]
            [hadoop-util.core :as h]
            [jackknife.logging :as log]
            [jackknife.core :as u]
            [elephantdb.common.status :as status]
            [elephantdb.common.domain :as dom]
            [elephantdb.common.thrift :as thrift]
            [elephantdb.common.loader :as loader]
            [elephantdb.common.hadoop :as hadoop]))

;; When we first start up the service, we trigger an unthrottled
;; download. It'd be good if we had access to the download supervisor
;; inside of prepare-local-domain, and could decide whether or not to
;; throttle the thing.

(defn try-thrift
  "Applies each kv pair to the supplied `func` in parallel, farming
  the work out to futures; each domain is set to `initial-status` at
  the beginning of the func, and `thrift/ready-status` on successful
  completion. (Failures are marked as appropriate.)

  After completing all functions, we remove all versions of each
  domain but the last."
  [domains-info local-dir initial-status func]
  (u/with-ret (u/p-dofor [[domain info] domains-info]
                         (try (dom/set-domain-status! info initial-status)
                              (func domain info)
                              (dom/set-domain-status! info (thrift/ready-status))
                              (catch Throwable t
                                (log/error t "Error when loading domain " domain)
                                (dom/set-domain-status! info
                                                        (thrift/failed-status t)))))
    (try (log/info "Removing all old versions of updated domains!")
         (domain/cleanup-domains! (keys domains-info) local-dir)
         (catch Throwable t
           (log/error t "Error when cleaning old versions.")))))

;; TODO: Test this deal with the finished loaders, etc.
(defn update-and-sync-status!
  "Triggers throttled update routine for all domains keyed in
  `domains-info`. Once these complete, atomically swaps them into for
  the current data and registers success."
  [edb-config rw-lock domains-info loader-state]
  (let [{:keys [hdfs-conf local-dir]} edb-config
        remote-path-map (:domains edb-config)
        remote-fs (h/filesystem hdfs-conf)
        
        ;; Do we throttle? TODO: Use this arg.
        throttle? (->> (vals domains-info)
                       (map domain/domain-status)
                       (some status/ready?))]
    (try-thrift domains-info
                local-dir
                (thrift/ready-status :loading? true)
                (fn [domain info]
                  (let [remote-path (clojure.core/get remote-path-map domain)
                        remote-vs (dom/try-domain-store remote-fs remote-path)]
                    (when remote-vs
                      (let [local-domain-root (str (h/path local-dir domain))
                            local-vs (dom/local-store local-domain-root remote-vs)]
                        (if (dom/needs-update? local-vs remote-vs)
                          (let [state (get (:download-states loader-state) domain)
                                new-data
                                (do (loader/load-domain local-vs remote-vs state)
                                    (dom/retrieve-shards! local-vs (keys state)))]
                            (dom/set-domain-data! rw-lock domain info new-data)
                            (log/info
                             "Finished loading all updated domains from remote."))
                          (let [{:keys [finished-loaders download-states]}
                                loader-state]
                            (swap! finished-loaders +
                                   (count (download-states domain))))))))))))

(defn get-readable-domain-info [domains-info domain]
  (let [info (domains-info domain)]
    (when-not info
      (throw (thrift/domain-not-found-ex domain)))
    (when-not (status/ready? (dom/domain-status info))
      (throw (thrift/domain-not-loaded-ex domain)))
    info))

(defn service-updating?
  [service-handler download-supervisor]
  (boolean
   (or (some status/loading?
             (-> service-handler .getStatus .get_domain_statuses vals))
       (and @download-supervisor
            (not (.isDone @download-supervisor))))))

(defn flattened-count [xs]
  (reduce + (map count xs)))

(defn update-domains
  [download-supervisor domains-info edb-config rw-lock]
  (let [{max-kbs :max-online-download-rate-kb-s
         local-dir :local-dir} edb-config
         download-state (-> domains-info

                            ;; this was a map of domain-name->shard-set
                            (dom/all-shards)
                            (hadoop/mk-loader-state))
         shard-amount (flattened-count (vals (:download-states download-state)))]
    (log/info "UPDATER - Updating domains: " (s/join ", " (keys domains-info)))
    (reset! download-supervisor (loader/start-download-supervisor
                                 shard-amount max-kbs download-state))
    (future (update-and-sync-status! edb-config
                                     rw-lock
                                     domains-info
                                     download-state))))

(defn trigger-update
  [service-handler download-supervisor domains-info edb-config rw-lock]
  (u/with-ret true
    (if (service-updating? service-handler download-supervisor)
      (log/info "UPDATER - Not updating as update process still in progress.")
      (update-domains download-supervisor domains-info edb-config rw-lock))))
