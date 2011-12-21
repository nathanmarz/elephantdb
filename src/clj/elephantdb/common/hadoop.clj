(ns elephantdb.common.hadoop
  "Namespace responsible for recursively transferring directories from
   a distributed filestore (or another local filestore) to the local
   filesystem on the current machine."
  (:require [hadoop-util.core :as h])
  (:import [java.io File FileNotFoundException
            FileOutputStream BufferedOutputStream]
           [org.apache.hadoop.fs FileSystem Path]))

;; This can all go; I've moved it out into `hadoop-util.transfer`.

(defn check-in
  "Report the current downloaded number of kilobytes to the supplied
  agent."
  [throttle-agent kbs]
  (letfn [(bump-kbs [{:keys [sleep-ms last-check
                             max-kbs kb-pool]:as m} kbs]
            (when (pos? max-kbs)
              (let [m (update-in m [:kb-pool] + kbs)
                    current-time (System/currentTimeMillis)
                    secs         (-> current-time (- last-check) (/ 1000))
                    rate         (/ kb-pool secs)]
                (cond
                 (> rate max-kbs) (assoc m
                                    :sleep-ms (rand 1000)
                                    :last-check current-time
                                    :kb-pool 0)
                 (pos? sleep-ms)  (assoc m
                                    :sleep-ms 0)
                 :else m))))]
    (send throttle-agent bump-kbs kbs)))

(defn sleep-interval
  "Returns the current sleep interval specified by the supplied
  throttling agent."
  [throttle-agent]
  (if throttle-agent
    (:sleep-ms @throttle-agent)
    0))

(defn update-limit
  "Updates the throttling agent's rate limit (in kb/s)."
  [throttle-agent new-limit]
  {:pre [(pos? new-limit)]}
  (letfn [(bump [m new-limit]
            (assoc m :max-kbs new-limit))]
    (send throttle-agent bump new-limit)))

(defn throttle
  "Returns a throttling agent. Any positive kb-per-second rate will
  cause throttling; if the rate is zero or negative, downloads will
  proceed without a throttle."
  [kb-per-second]
  (agent {:last-check (System/currentTimeMillis)
          :max-kbs kb-per-second
          :kb-pool  0
          :sleep-ms 0}))

(defn file-type
  "Accepts a hadoop filesystem object and some path and returns a
  namespace-qualified type keyword."
  [^FileSystem fs ^Path path]
  (cond (.isDirectory fs path) ::directory
        (.isFile fs path)      ::file))

;; Using a multimethod for the file transfer recursion removed the
;; need for a conditional check within the directory copy.

(defmulti copy
  (fn [& [fs path]]
    (file-type fs path)))

(defmethod copy ::file
  [^FileSystem fs ^Path remote-path local-path ^bytes buffer throttle]
  (with-open [is (.open fs remote-path)
              os (BufferedOutputStream. (FileOutputStream. local-path))]
    (loop []
      (let [sleep-ms (sleep-interval throttle)]
        (if (pos? sleep-ms)
          (do (Thread/sleep sleep-ms)
              (recur))
          (let [amt (.read is buffer)]
            (when (pos? amt)
              (.write os buffer 0 amt)
              (check-in throttle (/ amt 1024))
              (recur))))))))

(defmethod copy ::directory
  [^FileSystem fs ^Path remote-path local-path buffer throttle]
  (.mkdir (File. local-path))
  (doseq [status (.listStatus fs remote-path)]
    (let [remote-subpath (.getPath status)
          local-subpath (h/str-path local-path (.getName remote-subpath))]
      (copy fs remote-subpath local-subpath buffer throttle))))

;; TODO: Support transfers between filesystems rather than assuming a
;; local target. This will allow ElephantDB to transfer domains back
;; up to the remote host.

(defn rcopy
  "Copies information at the supplied remote-path over to the supplied
  local-path. `rcopy` takes an optional throttling agent; see
  `elephantdb.common.hadoop/throttle` for more details."
  [remote-fs remote-path target-path & {:keys [throttle]}]
  (let [buffer      (byte-array (* 1024 15))
        remote-path (h/path remote-path)
        source-name (.getName remote-path)
        target-path (File. target-path)
        target-path (cond
                     (not (.exists target-path)) target-path
                     (.isFile target-path)
                     (throw (IllegalArgumentException.
                             (str "File exists: " target-path)))
                     (.isDirectory target-path)
                     (h/str-path target-path source-name)
                     :else
                     (throw (IllegalArgumentException.
                             (format "Unknown error, %s is neither file nor dir."
                                     target-path))))]
    (if (.exists remote-fs remote-path)
      (copy remote-fs remote-path target-path buffer throttle)
      (throw (FileNotFoundException.
              (str "Couldn't find remote path: " remote-path))))))
