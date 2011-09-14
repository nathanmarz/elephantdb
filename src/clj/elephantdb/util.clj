(ns elephantdb.util
  (:import [java.net InetAddress]
           [java.util.concurrent.locks ReentrantReadWriteLock]))

(defn register-shutdown-hook [shutdown-func]
  (-> (Runtime/getRuntime)
      (.addShutdownHook (Thread. shutdown-func))))

(defn repeat-seq
  [amt aseq]
  (apply concat (repeat amt aseq)))

(defn flattened-count [xs]
  (reduce + (map count xs)))

(defmacro dofor [bindings & body]
  `(doall (for ~bindings (do ~@body))))

(defn update-vals
  "TODO: Note that this is the same as map-mapvals, just with a 2 arg
  function."
  [amap f]
  (into {} (dofor [[k v] amap]
                  [k (f k v)])))

(defn map-mapvals [amap f]
  (into {} (dofor [[k v] amap]
                  [k (f v)])))

(defn reverse-multimap
  "{:a [1 2] :b [1] :c [3]} -> {1 [:a :b] 2 [:a] 3 [:c]}"
  [amap]
  (apply merge-with
         concat
         (mapcat
          (fn [[k vlist]]
            (for [v vlist] {v [k]}))
          amap)))

(defn local-hostname []
  (.getHostAddress (InetAddress/getLocalHost)))

(defn find-first-next [pred aseq]
  (loop [[curr & restseq] aseq]
    (if (pred curr)
      [curr restseq]
      (recur restseq))))

(defn future-values [futures]
  (dofor [f futures]
         (.get f)))

(defn remove-val [v aseq]
  (filter (partial not= v)
          aseq))

(defn third [aseq]
  (nth aseq 2))

(defn parallel-exec [funcs]
  (if (empty? funcs)
    []
    (let [[thisfn & restfns] funcs
          future-rest (dofor [f restfns] (future-call f))]
      (cons (thisfn) (map deref future-rest)))))

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
