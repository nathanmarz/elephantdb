(ns elephantdb.hadoop
  (:use elephantdb.log
        [elephantdb.util :only (map-mapvals)])
  (:import [java.io File FileNotFoundException FileOutputStream BufferedOutputStream])
  (:import [org.apache.hadoop.fs FileSystem Path]
           [org.apache.hadoop.conf Configuration]))

(defmulti conf-set (fn [obj] (class (:value obj))))

(defmethod conf-set String [{key :key value :value conf :conf}]
  (.set conf key value))

(defmethod conf-set Integer [{key :key value :value conf :conf}]
  (.setInt conf key value))

(defmethod conf-set Float [{key :key value :value conf :conf}]
  (.setFloat conf key value))

(defn path
  ([str-or-path]
     (if (instance? Path str-or-path)
       str-or-path
       (Path. str-or-path)))
  ([parent child]
     (Path. parent child)))

(defn str-path
  ([part1] part1)
  ([part1 part2 & components]
     (apply str-path (str (path part1 (str part2))) components)))

(defn configuration [conf-map]
  (let [ret (Configuration.)]
    (doall
     (for [config conf-map]
       (conf-set {:key (first config) :value (last config) :conf ret})))
    ret))

(defn filesystem
  ([] (FileSystem/get (Configuration.)))
  ([conf-map]
     (FileSystem/get (configuration conf-map))))

(defn mkdirs [fs path]
  (.mkdirs fs (Path. path)))

(defn delete
  ([fs path] (delete fs path false))
  ([fs path recursive?]
     (.delete fs (Path. path) recursive?)))

(defn clear-dir [fs path]
  (delete fs path true)
  (mkdirs fs path))

(defn local-filesystem [] (FileSystem/getLocal (Configuration.)))

(defn mk-local-path [local-dir]
  (.pathToFile (local-filesystem)
               (path local-dir)))

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

(defn mk-shard-states [shards]
  (->> shards
       (map (fn [s]
              [s (ShardState. (atom 0)
                              (atom 0))]))
       (into {})))

(defn mk-loader-state
  "Create new LoaderState"
  [domains-to-shards]
  (let [shard-states (map-mapvals domains-to-shards
                                  mk-shard-states)]
    (DownloadState. shard-states (atom 0) (atom []))))

(declare copy-local*)

(defn- copy-file-local
  [^FileSystem fs ^Path path ^String target-local-path ^bytes buffer ^ShardState state]
  (let [downloaded-kb (:downloaded-kb state)
        sleep-interval (:sleep-interval state)]
    (with-open [is (.open fs path)
                os (BufferedOutputStream.
                    (FileOutputStream. target-local-path))]
      (loop []
        (if (= @sleep-interval 0)
          (let [amt (.read is buffer)]
            (when (> amt 0)
              (.write os buffer 0 amt)
              (swap! downloaded-kb + (int (/ amt 1024))) ;; increment downloaded-kb
              (recur)))
          (do
            (Thread/sleep @sleep-interval)
            (recur)))))))

(defn copy-dir-local
  [#^FileSystem fs #^Path path #^String target-local-path #^bytes buffer ^ShardState state]
  (.mkdir (File. target-local-path))
  (let [contents (seq (.listStatus fs path))]
    (doseq [c contents]
      (let [subpath (.getPath c)]
        (copy-local* fs subpath (str-path target-local-path (.getName subpath)) buffer state)))))

(defn- copy-local*
  [^FileSystem fs ^Path path target-local-path buffer state]
  (let [status (.getFileStatus fs path)]
    (if (.isDir status)
      (copy-dir-local fs path target-local-path buffer state)
      (copy-file-local fs path target-local-path buffer state))))

(defn copy-local
  [#^FileSystem fs #^String spath #^String local-path ^ShardState state]
  (let [target-file (File. local-path)
        source-name (.getName (Path. spath))
        buffer (byte-array (* 1024 15))
        dest-path (cond
                   (not (.exists target-file)) local-path
                   (.isFile target-file) (throw
                                          (IllegalArgumentException.
                                           (str "File exists " local-path)))
                   (.isDirectory target-file) (str-path local-path source-name)
                   :else (throw
                          (IllegalArgumentException.
                           (str "Unknown error, local file is neither file nor dir " local-path))))]
    (if (.exists fs (path spath))
      (copy-local* fs (path spath) dest-path buffer state)
      (throw
       (FileNotFoundException.
        (str "Could not find on remote " spath))))))
