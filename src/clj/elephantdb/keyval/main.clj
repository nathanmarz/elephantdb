(ns elephantdb.keyval.main
  (:use elephantdb.common.config)
  (:require [jackknife.core :as u]
            [jackknife.logging :as log]
            [elephantdb.common.database :as db]
            [elephantdb.common.thrift :as thrift])
  (:gen-class))

;; # Main Access
;;
;; This namespace is the main access point to the edb
;; code. elephantdb.keyval/-main Boots up the ElephantDB service and
;; an updater process that watches all domains and trigger an atomic
;; update in the background when some new version appears.

(defn launch-server!
  [{:keys [port options] :as database}]
  (let [{interval :update-interval-s} options
        server (thrift/thrift-server database port)]
    (u/register-shutdown-hook #(.stop server))
    (log/info "Preparing database...")
    (db/prepare database)
    (log/info "Starting ElephantDB server...")
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
                                           local-config)
        database       (db/build-database
                        (merge global-config local-config))]
    
    (launch-server! database)))
