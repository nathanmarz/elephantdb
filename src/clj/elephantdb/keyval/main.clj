(ns elephantdb.keyval.main
  (:use elephantdb.common.config)
  (:require [jackknife.core :as u]
            [jackknife.logging :as log]
            [elephantdb.keyval.service :as service])
  (:gen-class))

;; # Main Access
;;
;; This namespace is the main access point to the edb
;; code. elephantdb.keyval/-main Boots up the ElephantDB service and
;; an updater process that watches all domains and trigger an atomic
;; update in the background when some new version appears.

(defn launch-server!
  [{:keys [port update-interval-s] :as conf-map}]
  (let [handler (service/service-handler conf-map)
        server  (service/thrift-server handler port)]
    (u/register-shutdown-hook #(do (.shutdown handler)
                                   (.stop server)))
    (log/info "Starting updater process...")
    (service/launch-updater! handler update-interval-s)
    (log/info "Starting ElephantDB server...")
    (service/prepare handler)
    (.serve server)))

;; TODO: Booting needs a little work; I'll do this along with the
;; deploy.

(defn -main
  "Main booting function for all of EDB. Pass in:

  `global-config-hdfs-path`: the hdfs path of `global-config.clj`

  `local-config-path`: the path to `local-config.clj` on this machine."
  [global-config-hdfs-path local-config-path]
  (log/configure-logging "log4j/log4j.properties")
  (let [local-config   (read-local-config local-config-path)
        global-config  (read-global-config global-config-hdfs-path
                                           local-config)]
    (launch-server! (merge global-config local-config))))
