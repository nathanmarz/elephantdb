(ns elephantdb.deploy.provision
  (:use [clojure.contrib.def :only (defnk)]
        clojure.contrib.command-line
        pallet.compute
        pallet.core
        pallet.resource
        [pallet.configure :only (pallet-config compute-service-properties)]
        [pallet.blobstore :only (blobstore-from-map)]
        ;; TODO: Replace this with equivalent pallet command
        [org.jclouds.compute :only (nodes-with-tag)])
  (:require [elephantdb.deploy.util :as util]
            [elephantdb.deploy.node :as node]
            [pallet.request-map :as rm]
            [elephantdb.deploy.crate.edb-configs :as edb-configs])
  (:gen-class))

(defn- print-ips-for-tag! [aws tag-str]
  (let [running-node (filter running? (nodes-with-tag tag-str aws))]
    (println "TAG:     " tag-str)
    (println "PUBLIC:  " (map primary-ip running-node))
    (println "PRIVATE: " (map private-ip running-node))))

(defn ips! [ring]
  (let [{:keys [group-name]} (node/edb-group-spec ring)
        aws (compute-service-from-map (:deploy-creds (edb-config)))]
    (print-ips-for-tag! aws (name group-name))))

(defn- converge-edb!
  [ring count local?]
  (let [{:keys [data-creds deploy-creds]} (edb-configs/edb-config)
        deploy-creds (update-in deploy-creds
                                [:environment :user]
                                util/resolve-keypaths)
        compute (if local?
                  (service "virtualbox")
                  (compute-service-from-map deploy-creds))]
    (converge {(node/edb-group-spec ring :local? local?) count}
              :compute compute
              :environment (merge (:environment deploy-creds)
                                  {:ring ring
                                   :blobstore (blobstore-from-map data-creds)
                                   :edb-s3-keys data-creds}))))

(defnk start! [ring :local? false]
  (let [{count :node-count} (edb-configs/read-global-conf! ring)]
    (converge-edb! ring count local?))
  (println "Cluster Started.")
  (ips! ring))

(defnk stop! [ring :local? false]
  (converge-edb! ring 0 local?)
  (print "Cluster Stopped."))

(defn -main [& args]
  (with-command-line args
    "Provisioning tool for ElephantDB Clusters."
    [[start? "Start Cluster?"]
     [stop? "Shutdown Cluster?"]
     [local? "Local mode?"]
     [ring "ElephantDB Ring Name"]
     [ips? "Cluster IPs"]]
    (if-not ring
      (println "Please pass in a ring name with --ring.")
      (cond  start? (start! ring local?)
             stop? (stop! ring local?)
             ips? (ips! ring)
             :else (println "Must pass --start or --stop")))))
