(ns elephantdb.log
  (:require [clojure.contrib [logging :as log]]))

(defn log-message [& args]
  (log/info (apply str args)))

(defn log-error [e & args]
  (log/error (apply str args) e))