(ns elephantdb.common.util
  (:import [java.net InetAddress]
           [java.util.concurrent.locks ReentrantReadWriteLock]))

(defn >=s
  ">= for strings."
  [s1 s2]
  (= s2 (first (sort [s1 s2]))))

(defmacro safe-assert
  "TODO: Fix my version comparison."
  ([x] `(safe-assert ~x ""))
  ([x msg]
     (if (>=s (clojure-version) "1.3.0")
       `(assert ~x ~msg)
       `(assert ~x))))

(defn sleep
  "Sleeps for the supplied length of time. Negative numbers are
  treated as zero."
  [len]
  (when (pos? len)
    (Thread/sleep len)))

(defn register-shutdown-hook [shutdown-func]
  (-> (Runtime/getRuntime)
      (.addShutdownHook (Thread. shutdown-func))))

(defn repeat-seq
  [amt aseq]
  (apply concat (repeat amt aseq)))

;; TODO: Remove.
(defn flattened-count [xs]
  (reduce + (map count xs)))

(defmacro dofor [bindings & body]
  `(doall (for ~bindings (do ~@body))))

(defmacro p-dofor [bindings & body]
  `(let [futures# (dofor ~bindings
                         (future ~@body))]
     (future-values futures#)))

(defn do-pmap [fn & colls]
  (doall (apply pmap fn colls)))

(defmacro do-risky
  "Executes each form in sequence, as with doseq; on completion, if
  any of the forms has thrown some sort of error, do-risky throws the
  last one."
  [bindings & more]
  `(let [error# (atom nil)]
     (u/p-dofor ~bindings
                (try ~@more
                     (catch Throwable t#
                       (reset! error# t#))))
     (when-let [e# @error#]
       (throw e#))))

(defn update-vals [f m]
  (into {} (for [[k v] m]
             [k (f k v)])))

(defn val-map [f m]
  (into {} (for [[k v] m]
             [k (f v)])))

(defn reverse-multimap
  "{:a [1 2] :b [1] :c [3]} -> {1 [:a :b] 2 [:a] 3 [:c]}"
  [amap]
  (apply merge-with concat
         (mapcat
          (fn [[k vlist]]
            (for [v vlist] {v [k]}))
          amap)))

(defn local-hostname []
  (.getHostAddress (InetAddress/getLocalHost)))

(defn future-values [futures]
  (dofor [f futures]
         (.get f)))

(def separate
  "Accepts a predicate and a sequence, and returns:

   [(filter pred xs) (remove pred xs)]"
  (juxt filter remove))

(defn prioritize [pred coll]
  (apply concat (separate pred coll)))

;; TODO: Remove
(defn remove-val [v aseq]
  (filter (partial not= v)
          aseq))

(defn mk-rw-lock []
  (ReentrantReadWriteLock.))

(defmacro with-read-lock [rw-lock & body]
  `(let [rlock# (.readLock ~rw-lock)]
     (try
       (.lock rlock#)
       ~@body
       (finally (.unlock rlock#)))))

(defmacro with-write-lock [rw-lock & body]
  `(let [wlock# (.writeLock ~rw-lock)]
     (try
       (.lock wlock#)
       ~@body
       (finally (.unlock wlock#)))))

(defmacro with-ret-bound [[sym val] & body]
  `(let [~sym ~val]
     ~@body
     ~sym))

(defmacro with-ret [val & body]
  `(with-ret-bound [ret# ~val]
     ~@body))

;; ## Error Handlers

(defn throw-illegal [s]
  (throw (IllegalArgumentException. s)))

(defn throw-runtime [s]
  (throw (RuntimeException. s)))
