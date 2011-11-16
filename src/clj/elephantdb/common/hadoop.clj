(ns elephantdb.common.hadoop
  (:use hadoop-util.core)
  (:require [elephantdb.util :as u])
  (:import [java.io File FileNotFoundException
            FileOutputStream BufferedOutputStream]
           [org.apache.hadoop.fs FileSystem Path]
           [org.apache.hadoop.conf Configuration]))

;; ShardState:
;; downloaded-kb:    holds the total amount of bytes of data downloaded since the last
;;                   reset to 0
;;                   is used to determine the current download rate (kb/s)
;; sleep-interval:   time each shard loader process should sleep if not allowed to download.
;;                   if it's set to 0, that means it can download.
(defrecord ShardState [downloaded-kb sleep-interval])

;; DownloadState:
;; shard-states:     Map of domain name to list of ShardStates per shard of a domain
;; finished-loaders: gets incremented when a shard has finished loading
;; shard-loaders:    Vector of all shard-loader futures used by the download supervisor
(defrecord DownloadState [shard-states finished-loaders shard-loaders])

(defn mk-shard-states
  [shard-set]
  (->> (repeatedly #(ShardState. (atom 0)
                                 (atom 0)))
       (interleave shard-set)
       (apply hash-map)))

(defn mk-loader-state
  "Create new LoaderState"
  [domains-to-shards]
  (-> domains-to-shards
      (u/map-mapvals mk-shard-states)
      (DownloadState. (atom 0)
                      (atom []))))

(declare copy-local*)

(defn- copy-file-local
  [^FileSystem fs ^Path path ^String target-local-path ^bytes buffer ^ShardState state]
  (let [downloaded-kb (:downloaded-kb state)
        sleep-interval (:sleep-interval state)]
    (with-open [is (.open fs path)
                os (BufferedOutputStream.
                    (FileOutputStream. target-local-path))]
      (loop [sleep-time @sleep-interval]
        (u/sleep sleep-time)
        (if (pos? @sleep-interval)
          (recur @sleep-interval)
          (let [amt (.read is buffer)]
            (when (pos? amt)
              (.write os buffer 0 amt)
              (swap! downloaded-kb + (int (/ amt 1024))) ;; increment downloaded-kb
              (recur @sleep-interval))))))))

(defn copy-dir-local
  [^FileSystem fs ^Path path ^String target-local-path ^bytes buffer ^ShardState state]
  (.mkdir (File. target-local-path))
  (doseq [c (seq (.listStatus fs path))]
    (let [subpath (.getPath c)]
      (copy-local* fs
                   subpath
                   (str-path target-local-path (.getName subpath))
                   buffer
                   state))))

(defn- copy-local*
  [^FileSystem fs ^Path path target-local-path buffer state]
  (let [status (.getFileStatus fs path)]
    (if (.isDir status)
      (copy-dir-local fs path target-local-path buffer state)
      (copy-file-local fs path target-local-path buffer state))))

(defn copy-local
  [^FileSystem fs ^String spath ^String local-path ^ShardState state]
  (let [target-file (File. local-path)
        source-name (.getName (Path. spath))
        buffer (byte-array (* 1024 15))
        dest-path (cond (not (.exists target-file)) local-path
                        (.isFile target-file) (throw
                                               (IllegalArgumentException.
                                                (str "File exists " local-path)))
                        (.isDirectory target-file) (str-path local-path source-name)
                        :else (throw
                               (IllegalArgumentException.
                                (str "Unknown error, local file is neither file nor dir "
                                     local-path))))]
    (if (.exists fs (path spath))
      (copy-local* fs (path spath) dest-path buffer state)
      (throw (FileNotFoundException.
              (str "Could not find on remote " spath))))))
