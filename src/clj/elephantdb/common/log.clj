(ns elephantdb.common.log
  (:import [org.apache.log4j PropertyConfigurator Logger Level]))

;; TODO: Delete and move to jackknife.logging
(defn configure-logging [path]
  (PropertyConfigurator/configure path))

(def log-levels
  {:fatal Level/FATAL
   :warn  Level/WARN
   :info  Level/INFO
   :debug Level/DEBUG
   :off   Level/OFF})

(defmacro with-log-level [level & body]
  `(let [with-lev#  (log-levels ~level)
         logger#    (Logger/getRootLogger)
         prev-lev#  (.getLevel logger#)]
     (try (.setLevel logger# with-lev#)
          ~@body
          (finally
           (.setLevel logger# prev-lev#)))))
