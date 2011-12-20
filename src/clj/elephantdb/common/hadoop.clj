(ns elephantdb.common.hadoop
  "Namespace responsible for recursively transferring directories from
   a distributed filestore (or another local filestore) to the local
   filesystem on the current machine."
  (:use hadoop-util.core)
  (:require [jackknife.core :as u])
  (:import [java.io File FileNotFoundException
            FileOutputStream BufferedOutputStream]
           [org.apache.hadoop.fs FileSystem Path]
           [org.apache.hadoop.conf Configuration]))

;; ## Download State
;;
;; When transferring domains between filesystems, ElephantDB tracks
;; its progress with a stateful object that tracks the current "sleep
;; interval" as well as the total size of data downloaded since the
;; last reset. This is useful for throttling; on reset, the system can
;; examine the amount of data that's piled up since the sleep
;; interval was last reset to zero and adjust accordingly.

(defrecord DownloadState
    [downloaded-kb sleep-interval])

;; The system only triggers a download when the sleep interval is
;; zero. Throttling only occurs when a supervisor and the downloading
;; threads share a particular download state; if only the downloading
;; threads have access, the download won't be throttled.
;;
;; The plan is to move the Download State and all logic connected to
;; it out into the
;; [hadoop-util](https://github.com/sritchie/hadoop-util) project, so
;; it can co-exist with the other filesystem manipulation tools we've
;; been building.

;; ## Loader State
;;
;; This construct is particular to ElephantDB.

;; * download-states: Map of domain name to list of DownloadStates per
;; shard of a domain
;; * finished-loaders: incremented when a shard has finished loading
;; * shard-loaders: Vector of all shard-loader futures used by the
;; download supervisor

(defrecord LoaderState
    [download-states finished-loaders shard-loaders])

(defn fresh-download-state
  "Initializes a download state with stateful values initialized to
  zero."
  []
  (DownloadState. (atom 0)
                  (atom 0)))

;; mk-download-states and mk-loader-state will stay with elephantDB.

(defn mk-download-states
  "For a given shard set, returns a "
  [shard-set]
  (->> (repeatedly fresh-download-state)
       (interleave shard-set)
       (apply hash-map)))

(defn mk-loader-state
  "Generator a new loader state from the supplied domain->shard map."
  [domains-to-shards]
  (-> (u/val-map mk-download-states domains-to-shards)
      (LoaderState. (atom 0) (atom []))))

(defn file-type
  "Accepts a hadoop filesystem object and some path and returns a
  namespace-qualified type keyword."
  [^FileSystem fs ^Path path]
  (cond (.isDirectory fs path) ::directory
        (.isFile fs path)      ::file))

;; Copying is handled differentl

(defmulti copy (fn [& [fs path]]
                 (file-type fs path)))

(defmethod copy ::file
  [^FileSystem fs ^Path remote-path local-path ^bytes buffer state]
  (let [{:keys [downloaded-kb sleep-interval]} state]
    (with-open [is (.open fs remote-path)
                os (BufferedOutputStream. (FileOutputStream. local-path))]
      (loop []
        (if (pos? @sleep-interval)
          (do (u/sleep @sleep-interval)
              (recur))
          (let [amt (.read is buffer)]
            (when (pos? amt)
              (.write os buffer 0 amt)
              (swap! downloaded-kb + (int (/ amt 1024))) ;; increment downloaded-kb
              (recur))))))))

(defmethod copy ::directory
  [fs ^Path remote-path local-path ^bytes buffer state]
  (.mkdir (File. local-path))
  (doseq [status (.listStatus fs remote-path)]
    (let [remote-subpath (.getPath status)
          local-subpath (str-path local-path (.getName remote-subpath))]
      (copy fs remote-subpath local-subpath buffer state))))

(defn rcopy
  "Copies information at the supplied remote-path over to the supplied local-path.

  Arguments are Filesystem, remote shard path, target local path, and
  a ShardState record for communicating results back up the call
  chain."
  ([remote-fs remote-path target-path]
     (rcopy remote-fs remote-path target-path (fresh-download-state)))
  ([^FileSystem remote-fs ^String remote-path ^String target-path state]
     (let [buffer      (byte-array (* 1024 15))
           remote-path (path remote-path)
           source-name (.getName remote-path)
           target-path (File. target-path)
           target-path (cond
                        (not (.exists target-path)) target-path
                        (.isFile target-path) (u/throw-illegal
                                               (str "File exists: " target-path))
                        (.isDirectory target-path) (str-path target-path source-name)
                        :else (u/throw-illegal
                               (format "Unknown error, %s is neither file nor dir."
                                       target-path)))]
       (if (.exists remote-fs remote-path)
         (copy remote-fs remote-path target-path buffer state)
         (throw (FileNotFoundException.
                 (str "Couldn't find remote path: " remote-path)))))))

(defn supervise-shard
  [max-kbs total-kb download-state]
  (let [{:keys [downloaded-kb sleep-interval]} download-state]
    (let [dl-kb @downloaded-kb
          sleep-ms (rand 1000)] ;; sleep random amount of time up to 1s
      (swap! total-kb + dl-kb)
      (reset! sleep-interval
              (if (>= @total-kb max-kbs)
                sleep-ms
                0))
      (reset! downloaded-kb 0))))

(defn supervise-downloads
  [amount-shards max-kbs interval-ms ^LoaderState state]
  (let [{:keys [download-states finished-loaders shard-loaders]} state
        total-kb (atom 0)]
    (loop []
      (reset! total-kb 0)
      (u/sleep interval-ms)
      (let [loader-count (->> @shard-loaders
                              (filter (memfn isDone))
                              (count)
                              (+ @finished-loaders))]
        (when (< loader-count amount-shards)
          (doseq [[_ states] download-states
                  [_ state] states]
            (supervise-shard max-kbs total-kb state))
          (recur))))))
