(ns elephantdb.common.logging
  (:require [clojure.tools.logging :as log])
  (:import [org.apache.log4j PropertyConfigurator Logger Level]))

;; TODO: Delete and move to jackknife.logging
(defn configure-logging [path]
  (PropertyConfigurator/configure path))

(defn info [& args]
  (log/info (apply str args)))

(defn warning [& args]
  (log/warn (apply str args)))

(defn error [e & args]
  (log/error e (apply str args)))

(defn debug [& args]
  (log/debug (apply str args)))

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
