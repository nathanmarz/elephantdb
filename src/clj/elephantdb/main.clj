(ns elephantdb.main
  (:import [org.apache.thrift.server THsHaServer THsHaServer$Options])
  (:import [org.apache.thrift.protocol TBinaryProtocol TBinaryProtocol$Factory])
  (:import [org.apache.thrift TException])
  (:import [org.apache.log4j PropertyConfigurator])
  (:import [elephantdb.generated ElephantDB ElephantDB$Processor])
  (:import [org.apache.thrift.transport TNonblockingServerTransport TNonblockingServerSocket])
  (:require [elephantdb [service :as service]])
  (:use [elephantdb config log hadoop])
  (:gen-class))

(defn launch-updater! [interval-secs service-handler]
  (let [interval-ms (* 1000 interval-secs)]
    (future
      (log-message "Starting updater process with an interval of: " interval-secs " seconds...")
      (loop []
        (Thread/sleep interval-ms)
        (log-message "Updater process: Checking if update is possible...")
        (.updateAll service-handler)
        (recur)))))

(defn launch-server! [global-config local-config token]
  (let
      [options (THsHaServer$Options.)
       _ (set! (. options maxWorkerThreads) 64)
       service-handler (service/service-handler global-config local-config token)
       server (THsHaServer.
               (ElephantDB$Processor. service-handler)
               (TNonblockingServerSocket. (:port global-config))
               (TBinaryProtocol$Factory.) options)]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. (fn []
                                 (.shutdown service-handler)
                                 (.stop server))))
    (log-message "Starting updater process...")
    (launch-updater! (:update-interval-s local-config) service-handler)
    (log-message "Starting ElephantDB server...")
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
  (PropertyConfigurator/configure "log4j/log4j.properties")
  (let [local-config (-> (local-filesystem)
                         (read-clj-config  local-config-path)
                         (merge DEFAULT-LOCAL-CONFIG))
        global-config (read-global-config global-config-hdfs-path
                                          local-config token)]
    (launch-server! global-config local-config token)))
