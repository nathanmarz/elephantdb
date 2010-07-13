(ns elephantdb.util
  (:import [java.net InetAddress]))

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
  (.getHostName (InetAddress/getLocalHost)))

(defn find-first-next [pred aseq]
  (loop [[curr & restseq] aseq]
    (if (pred curr) [curr restseq] (recur restseq))))

(defmacro dofor [bindings & body]
  `(doall (for ~bindings (do ~@body))))