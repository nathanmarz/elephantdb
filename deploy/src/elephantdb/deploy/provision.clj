(ns elephantdb.deploy.provision
  (:use [clojure.contrib.command-line]
        [org.jclouds.compute :only [nodes-with-tag]]
        [pallet
         compute core
         resource
         [configure :only [pallet-config compute-service-properties]]
         [blobstore :only [blobstore-from-config]]])
  (:require [elephantdb.deploy
             [node :as node]]
            [pallet.request-map :as rm]
            [elephantdb.deploy.crate
             [edb-configs :as edb-configs]])
  (:gen-class))

(defn- print-ips-for-tag! [aws tag-str]
  (let [running-node (filter running? (nodes-with-tag tag-str aws))]
    (println "TAG:     " tag-str)
    (println "PUBLIC:  " (map primary-ip running-node))
    (println "PRIVATE: " (map private-ip running-node))))

(defn ips! [ring]
  (let [{:keys [group-name]} (node/edb-group-spec ring)
        aws (compute-service-from-config-file "elephantdb-deploy")]
    (print-ips-for-tag! aws (name group-name))))

(defn- converge-edb! [ring count]
  (converge {(node/edb-group-spec ring) count}
          :compute (compute-service-from-config-file "elephantdb-deploy")
          :environment
          {:blobstore (blobstore-from-config (pallet-config) ["elephantdb-data"])
           :ring ring
           :edb-s3-keys (compute-service-properties (pallet-config) ["elephantdb-data"])}))

(defn start! [ring]
  (let [count (:node-count (edb-configs/read-global-conf! ring))]
    (converge-edb! ring count))
  (print "Cluster Started")
  (ips! ring))

(defn stop! [ring]
  (converge-edb! ring 0)
  (print "Cluster Stopped"))

(defn -main [& args]
  (let [aws (compute-service-from-config-file "elephantdb-deploy")]
    (with-command-line args
      "Provisioning tool for ElehpantDB Clusters"
      [[start? "Start Cluster?"]
       [stop? "Shutdown Cluster?"]
       [ring "ElephantDB Ring Name"]
       [ips? "Cluster IPs"]]
      (assert (not (nil? ring)))
      (cond 
       start? (start! ring)
       stop? (stop! ring)
       ips? (ips! ring)
       :else (println "Must pass --start or --stop")))))

