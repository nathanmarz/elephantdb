(ns elephantdb.common.loader
  "This namespace handles shard downloading. the actual download
  mechanics are inside of elephantdb.common.hadoop; this namespace
  triggers downloads and opens and closes domains and shards."
  (:use [elephantdb.keyval.config :only (persistence-options)])
  (:require [hadoop-util.core :as h]
            [elephantdb.common.hadoop :as hadoop]
            [elephantdb.common.logging :as log]
            [elephantdb.common.config :as conf]
            [elephantdb.common.util :as u])
  (:import [elephantdb.store DomainStore]
           [elephantdb.common.hadoop ShardState DownloadState]))

(defn- shard-path
  [domain-version shard]
  (h/str-path domain-version shard))

(defn open-domain-shard
  "Opens and returns a LocalPersistence object standing in for the
  shard at the supplied path."
  [persistence-factory db-conf local-shard-path]
  (log/info "Opening LP " local-shard-path)
  (u/with-ret (.openPersistenceForRead
               persistence-factory
               local-shard-path
               (persistence-options db-conf persistence-factory))
    (log/info "Opened LP " local-shard-path)))

(defn open-domain
  "Returns a sequence of LocalPersistence objects on success."
  [db-conf local-domain-root shards]
  (let [lfs (h/local-filesystem)
        local-store (DomainStore. lfs local-domain-root)
        domain-spec (conf/read-domain-spec lfs local-domain-root)
        local-version-path (.mostRecentVersionPath local-store)
        future-lps (u/dofor [s shards]
                            [s (future (open-domain-shard
                                        (:persistence-factory domain-spec)
                                        db-conf
                                        (shard-path local-version-path s)))])]
    (u/with-ret (u/val-map (memfn get) future-lps)
      (log/info "Finished opening domain at " local-domain-root))))

(defn close-shard
  "Returns nil; throws IOException when some sort of failure occurs."
  [[shard lp] domain]
  (try (.close lp)
       (catch Throwable t
         (log/error t
                    "Error when closing local persistence for domain: "
                    domain
                    " and shard: "
                    shard)
         (throw t))))

(defn close-domain
  "Closes all shards in the supplied domain. TODO: If a shard throws
  an error, is this behavior predictable?"
  [domain domain-data]
  (log/info (format "Closing domain: %s with data: %s" domain domain-data))
  (doseq [shard-data domain-data]
    (close-shard shard-data domain))
  (log/info "Finished closing domain: " domain))

;; TODO: respect the max copy rate

;; TODO: do a streaming recursive copy that can be rate limited (rate
;; limited with the other shards...)
(defn load-domain-shard!
  [fs persistence-factory persistence-opts local-shard-path remote-shard-path state]
  (if (.exists fs (h/path remote-shard-path))
    (do (log/info "Copying " remote-shard-path " to " local-shard-path)
        (hadoop/copy-local fs remote-shard-path local-shard-path state)
        (log/info "Copied " remote-shard-path " to " local-shard-path))
    (do (log/info "Shard " remote-shard-path " did not exist. Creating empty LP")
        (.close (.createPersistence persistence-factory
                                    local-shard-path
                                    persistence-opts)))))

(defn load-domain
  "returns a map from shard to LP."
  [domain fs local-db-conf local-domain-root remote-path state]
  (log/info "Loading domain at " remote-path " to " local-domain-root)
  (let [lfs           (h/local-filesystem)
        remote-vs     (DomainStore. fs remote-path)
        factory       (-> (.getSpec remote-vs)
                          (conf/convert-java-domain-spec)
                          (:persistence-factory))
        local-vs       (DomainStore. lfs local-domain-root (.getSpec remote-vs))
        remote-version (.mostRecentVersion remote-vs)
        local-v-path   (.createVersion local-vs remote-version)
        remote-v-path  (.versionPath remote-vs remote-version)
        _              (h/mkdirs lfs local-v-path)
        domain-state   (get (:shard-states state) domain)
        shards         (keys domain-state)
        shard-loaders  (u/dofor [s shards]
                                (u/with-ret-bound
                                  [f (future
                                       (load-domain-shard!
                                        fs
                                        factory
                                        (persistence-options local-db-conf
                                                             factory)
                                        (shard-path local-v-path s)
                                        (shard-path remote-v-path s)
                                        (domain-state s)))]
                                  (swap! (:shard-loaders state) conj f)))]
    (u/future-values shard-loaders)
    (.succeedVersion local-vs local-v-path)
    (log/info (format "Successfully loaded domain at %s to %s with version %s."
                      remote-path
                      local-domain-root
                      remote-version))
    (open-domain local-db-conf local-domain-root shards)))

(defn supervise-shard
  [max-kbs total-kb ^ShardState shard-state]
  (let [downloaded-kb  (:downloaded-kb shard-state)
        sleep-interval (:sleep-interval shard-state)]
    (let [dl-kb @downloaded-kb
          sleep-ms (rand 1000)] ;; sleep random amount of time up to 1s
      (swap! total-kb + dl-kb)
      (reset! sleep-interval
              (if (>= @total-kb max-kbs)
                sleep-ms
                0))
      (reset! downloaded-kb 0))))

(defn supervise-downloads
  [amount-shards max-kbs interval-ms ^DownloadState state]
  (let [domain-to-shard-states (:shard-states state)
        finished-loaders (:finished-loaders state)
        shard-loaders (:shard-loaders state)
        total-kb (atom 0)]
    (log/info (format "Starting download supervisor for %d shard loaders."
                      amount-shards))
    (loop []
      (reset! total-kb 0)
      (Thread/sleep interval-ms)
      (let [loader-count (->> @shard-loaders
                              (filter (memfn isDone))
                              (count)
                              (+ @finished-loaders))]
        (if (< loader-count amount-shards)
          (do (doseq [[_ shard-states] domain-to-shard-states
                      [_ s-state] shard-states]
                (supervise-shard max-kbs total-kb s-state))
              (recur))
          (log/info "Download supervisor finished"))))))

(defn start-download-supervisor
  [amount-shards max-kbs ^DownloadState state]
  (let [interval-factor-secs 0.1 ;; check every 0.1s
        interval-ms (int (* interval-factor-secs 1000))
        max-kbs-val (int (* max-kbs interval-factor-secs))]
    (future
      (reset! (:finished-loaders state) 0)
      (when (and (pos? amount-shards) ;; only monitor if there's a download throttle
                 (pos? max-kbs))      ;; and shards to be downloaded
        (supervise-downloads amount-shards max-kbs-val interval-ms state)))))
