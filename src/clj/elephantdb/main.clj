(ns elephantdb.main
  (:use [elephantdb config hadoop])
  (:require [elephantdb.log :as log]
            [elephantdb.service :as service]
            [elephantdb.util :as util])
  (:import [org.apache.thrift.server THsHaServer THsHaServer$Options]
           [org.apache.thrift.protocol TBinaryProtocol TBinaryProtocol$Factory]
           [org.apache.thrift TException]
           [elephantdb.generated ElephantDB ElephantDB$Processor]
           [org.apache.thrift.transport TNonblockingServerTransport TNonblockingServerSocket])
  (:gen-class))

;; # Main Access
;;
;; This namespace is the main access point to the edb code. Running
;; elephantdb.main Boots up the server, and an updater process that
;; watches all domains and trigger an atomic update in the background
;; when some new version appears in domains.

(defn launch-updater! [interval-secs service-handler]
  (let [interval-ms (* 1000 interval-secs)]
    (future
      (log/log-message (format "Starting updater process with an interval of: %s seconds..."
                               interval-secs))
      (while true
        (Thread/sleep interval-ms)
        (log/log-message "Updater process: Checking if update is possible...")
        (.updateAll service-handler)))))

(defn launch-server
  [global-config local-config]
  (let [options (THsHaServer$Options.)
        _ (set! (. options maxWorkerThreads) 64)
        service-handler (service/service-handler global-config
                                                 local-config
                                                 (System/currentTimeMillis))
        server (THsHaServer.
                (ElephantDB$Processor. service-handler)
                (TNonblockingServerSocket. (:port global-config))
                (TBinaryProtocol$Factory.) options)]
    (util/register-shutdown-hook #(do (.shutdown service-handler)
                                      (.stop server)))
    (log/log-message "Starting updater process...")
    (launch-updater! (:update-interval-s local-config) service-handler)
    (log/log-message "Starting ElephantDB server...")
    (.serve server)))

(defn -main
  "Main booting function for all of EDB. Pass in:

  `global-config-hdfs-path`: the path of `global-config.clj`, located
    on ec2

  `local-config-path`: the path to `local-config.clj` on this machine."
  [global-config-hdfs-path local-config-path]
  (log/configure-logging "log4j/log4j.properties")
  (let [local-config  (read-local-config local-config-path)
        global-config (read-global-config global-config-hdfs-path
                                          local-config)]
    (launch-server! global-config local-config)))
