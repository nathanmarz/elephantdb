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

(defn launch-updater! [interval-secs service-handler]
  (let [interval-ms (* 1000 interval-secs)]
    (future
      (log/log-message "Starting updater process with an interval of: " interval-secs " seconds...")
      (loop []
        (Thread/sleep interval-ms)
        (log/log-message "Updater process: Checking if update is possible...")
        (.updateAll service-handler)
        (recur)))))

(defn launch-server! [global-config local-config token]
  (let [options (THsHaServer$Options.)
        _ (set! (. options maxWorkerThreads) 64)
        service-handler (service/service-handler global-config local-config token)
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

  `local-config-path`: the path to `local-config.clj` on this machine

  `token`: some string, usually a timestamp.

  the token is stored locally. If the token changes and there's newer
data on the server, elephantdb will load it. Otherwise, elephantdb
just uses whatever is local."
  [global-config-hdfs-path local-config-path token]
  (log/configure-logging "log4j/log4j.properties")
  (let [local-config  (read-local-config local-config-path)
        global-config (read-global-config global-config-hdfs-path
                                          local-config token)]
    (launch-server! global-config local-config token)))
