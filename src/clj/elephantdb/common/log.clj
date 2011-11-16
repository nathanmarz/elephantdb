(ns elephantdb.common.log
  (:require [clojure.tools.logging :as log])
  (:import [org.apache.log4j PropertyConfigurator Logger Level]))

(defn configure-logging [path]
  (PropertyConfigurator/configure path))

(defn log-message [& args]
  (log/info (apply str args)))

(defn log-warning [& args]
  (log/warn (apply str args)))

(defn log-error [e & args]
  (log/error (apply str args) e))

(defn log-debug [& args]
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
