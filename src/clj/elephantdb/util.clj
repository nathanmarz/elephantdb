(ns elephantdb.util
  (:import [java.net InetAddress])
  (:import [java.util.concurrent.locks ReentrantReadWriteLock]))

(defn repeat-seq
  ([aseq]
    (apply concat (repeat aseq)))
  ([amt aseq]
    (apply concat (repeat amt aseq))
    ))

(defn map-mapvals [f amap]
  (into {} (for [[k v] amap] [k (f v)])))

(defn reverse-multimap
  "{:a [1 2] :b [1] :c [3]} -> {1 [:a :b] 2 [:a] 3 [:c]}"
  [amap]
  (reduce
    (fn [m [k v]]
      (reduce
        (fn [m v]
          (let [existing (get m v [])]
          (assoc m v (conj existing k))))
          m v))
    {} amap))

(defn local-hostname []
  (.getCanonicalHostName (InetAddress/getLocalHost)))

(defn find-first-next [pred aseq]
  (loop [[curr & restseq] aseq]
    (if (pred curr) [curr restseq] (recur restseq))))

(defmacro dofor [bindings & body]
  `(doall (for ~bindings (do ~@body))))

(defn future-values [futures]
  (dofor [f futures]
    (.get f)))

(defn remove-val [v aseq]
  (filter (partial not= v) aseq))

(defn mk-rw-lock []
  (ReentrantReadWriteLock.))

(defn third [aseq]
  (nth aseq 2))

(defn parallel-exec [funcs]
  (if (empty? funcs)
    []
    (let [[thisfn & restfns] funcs
          future-rest (dofor [f restfns] (future-call f))]
      (cons (thisfn) (map deref future-rest))
      )))

(defmacro read-locked [rw-lock & body]
  `(let [rlock# (.readLock ~rw-lock)]
      (try
        (.lock rlock#)
        ~@body
      (finally (.unlock rlock#)))))

(defmacro write-locked [rw-lock & body]
  `(let [wlock# (.writeLock ~rw-lock)]
      (try
        (.lock wlock#)
        ~@body
      (finally (.unlock wlock#)))))

(defmacro with-ret-binded [[sym val] & body]
  `(let [~sym ~val]
     ~@body
     ~sym
     ))

(defmacro with-ret [val & body]
  `(with-ret-binded [ret# ~val]
     ~@body
     ))
