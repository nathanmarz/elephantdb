(ns elephantdb.common.loader
  "This namespace handles shard downloading. the actual download
  mechanics are inside of elephantdb.common.hadoop; this namespace
  triggers downloads and opens and closes domains and shards."
  (:require [hadoop-util.core :as h]
            [elephantdb.common.hadoop :as hadoop]
            [elephantdb.common.logging :as log]
            [elephantdb.common.config :as conf]
            [elephantdb.common.util :as u])
  (:import [elephantdb.store DomainStore]
           [elephantdb.common.hadoop DownloadState LoaderState]))

;; TODO: respect the max copy rate

;; TODO: do a streaming recursive copy that can be rate limited (rate
;; limited with the other shards...)
;;
;; todo; do we need this? (h/mkdirs lfs local-v-path)
(defn load-shard!
  [local-vs remote-vs shard-idx version state]
  (let [remote-fs   (.getFileSystem remote-vs)
        local-path  (.createVersion local-vs version)
        remote-path (.versionPath remote-vs version)
        path-fn     #(h/path (.shardPath % shard-idx version))
        remote-shard-path (path-fn remote-vs)
        local-shard-path  (path-fn local-vs)]
    (if (.exists remote-fs remote-shard-path)
      (do (log/info "Copying " remote-shard-path " to " local-shard-path)
          (hadoop/rcopy remote-fs remote-shard-path local-shard-path state)
          (log/info "Copied " remote-shard-path " to " local-shard-path))
      (do (log/info "Shard " remote-shard-path " did not exist. Creating empty LP")
          (.close (.createShard local-vs shard-idx version))))))

;; TODO: What if both stores have the same versions?
(defn load-domain
  "Transfers data from the latest version at the remote store to the
  local store. Returns true on success."
  [local-vs remote-vs domain-state]
  (let [shards        (keys domain-state)
        remote-version (.mostRecentVersion remote-vs)
        shard-loaders (for [shard-idx shards]
                        (let [download-state (domain-state shard-idx)]
                          (u/with-ret-bound
                            [f (future
                                 (load-shard! local-vs
                                              remote-vs
                                              shard-idx
                                              remote-version
                                              download-state))]
                            (swap! (:shard-loaders download-state) conj f))))]
    (u/future-values shard-loaders)
    (.succeedVersion local-vs remote-version)
    (u/with-ret true
      (log/info (format "Successfully loaded domain at %s to %s with version %s."
                        (.getRoot remote-vs)
                        (.getRoot local-vs)
                        remote-version)))))

(defn start-download-supervisor
  [amount-shards max-kbs ^LoaderState state]
  (let [interval-factor-secs 0.1 ;; check every 0.1s
        interval-ms (int (* interval-factor-secs 1000))
        max-kbs-val (int (* max-kbs interval-factor-secs))]
    (future
      (reset! (:finished-loaders state) 0)
      (when (and (pos? amount-shards)   ; and shards to be downloaded
                 (pos? max-kbs))  ; only monitor if there's a download
                                        ; throttle
        (log/info (format "Starting download supervisor for %d shard loaders."
                          amount-shards))
        (hadoop/supervise-downloads amount-shards max-kbs-val interval-ms state)
        (log/info "Download supervisor finished")))))
