(ns elephantdb.log
  (:require [clojure.contrib.logging :as log])
  (:import [org.apache.log4j PropertyConfigurator]))

(defn configure-logging [path]
  (PropertyConfigurator/configure path))

(defn log-message [& args]
  (log/info (apply str args)))

(defn log-error [e & args]
  (log/error (apply str args) e))

(defn log-debug [& args]
  (log/debug (apply str args)))
