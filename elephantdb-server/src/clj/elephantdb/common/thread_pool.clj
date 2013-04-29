(ns elephantdb.common.thread-pool
  (:import (java.util.concurrent Executors ExecutorService ThreadPoolExecutor ArrayBlockingQueue TimeUnit)))
 
; fixed thread pool as default
(def ^{:dynamic true} 
  *thread-pool* (Executors/newFixedThreadPool (+ 2 (.. Runtime getRuntime availableProcessors))))
 
(defn future-call+
  "Similar to clojure.core/future-call but with rebindable threadpool."
  [f]
  ; the position of the type hint ^Callable ensures that the returned future will return the calculated value
  (let [fut (.submit *thread-pool* ^Callable (#'clojure.core/binding-conveyor-fn f))]
    (reify
      clojure.lang.IDeref
        (deref [_] (#'clojure.core/deref-future fut))
      clojure.lang.IBlockingDeref
        (deref
          [_ timeout-ms timeout-val]
          (#'clojure.core/deref-future fut timeout-ms timeout-val))
      clojure.lang.IPending
        (isRealized [_] (.isDone fut))
      java.util.concurrent.Future
        (get [_] (.get fut))
        (get [_ timeout unit] (.get fut timeout unit))
        (isCancelled [_] (.isCancelled fut))
        (isDone [_] (.isDone fut))
        (cancel [_ interrupt?] (.cancel fut interrupt?)))))
  
(defmacro future+
  "Similar to clojure.core/future but with rebindable threadpool."
  [& body] `(future-call+ (^{:once true} fn* [] ~@body)))
 
(defn shutdown-thread-pool
  []
  (.shutdown *thread-pool*))
 
(defmacro with-bounded-queue-executor
  "All future+ calls in the scope of this macro are executed in a bounded queue executor with the given thread count and queue size."
  [[thread-count, queue-size], & body]
 `(binding [*thread-pool* (ThreadPoolExecutor. ~thread-count, ~thread-count, 0, TimeUnit/MILLISECONDS, (ArrayBlockingQueue. ~queue-size))]
    (let [result# ~@body]
      (shutdown-thread-pool)
      result#)))

(defmacro with-cached-executor
  "All future+ calls in the scope of this macro are executed in a cached thread pool executor,"
  [& body]
  `(binding [*thread-pool* (Executors/newCachedThreadPool)]
     (let [result# ~@body]
       (shutdown-thread-pool)
       result#)))
