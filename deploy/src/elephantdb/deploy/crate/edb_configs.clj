(ns elephantdb.deploy.crate.edb-configs
  (:use pallet.compute
        [clojure.string :only (join)]
        [clojure.contrib.map-utils :only (deep-merge-with)]
        [org.jclouds.blobstore :only (upload-blob)]
        [pallet.session :only (nodes-in-group)]
        [pallet.configure :only (pallet-config compute-service-properties)]
        [pallet.resource.remote-file :only (remote-file)])
  (:require [pallet.request-map :as rm]
            [pallet.session :as session]))

(defn edb-config []
  (compute-service-properties (pallet-config) ["elephantdb"]))

(defn edb-ring-config [ring]
  (let [ring (keywordize ring)]
    (-> (edb-config) :configs ring)))

(defn keywordize [x]
  (if (keyword? x)
    x
    (keyword x)))

(defn read-global-conf! [ring]
  (:global (edb-ring-config ring)))

(defn read-local-conf! [ring]
  (:local (edb-ring-config ring)))

;; HACK: Construct ec2 internal hostname from internal ip.  Needed
;; until Nathan allows internal ip in global-conf.clj
(defn internal-hostname [node]
  (format "ip-%s.ec2.internal"
          (->> (private-ip node)
               (re-seq #"\d+")
               (join "-"))))

(defn- global-conf-with-hosts
  [session local-config]
  (let [hosts (map internal-hostname (session/nodes-in-group session))]
    (prn-str (assoc local-config :hosts hosts))))

;; TODO: Move path out to config staging path in pallet config.
(defn upload-global-conf!
  [session]
  (let [ring (-> session :environment :ring)
        local-conf (read-global-conf! ring)
        s3-conf (global-conf-with-hosts session local-conf)
        s3-key (format "configs/elephantdb/%s/global-conf.clj" ring)]
    (upload-blob "hdfs2" s3-key s3-conf (:blobstore session))
    session))

;; TODO: Pull this stuff out of local-conf.
(defn local-conf-with-keys
  [session local-conf]
  (let [{:keys [edb-s3-keys]} session]
    (deep-merge-with #(identity %2)
                     {:hdfs-conf 
                      {"fs.s3n.awsAccessKeyId" (:identity edb-s3-keys)
                       "fs.s3n.awsSecretAccessKey" (:credential edb-s3-keys)}}
                     local-conf)))

(defn remote-file-local-conf!
  [session dst-path]
  (let [conf-with-keys (->> (-> session :environment :ring)
                            (read-local-conf!)
                            (local-conf-with-keys session)
                            (prn-str))]
    (remote-file session
                 dst-path
                 :content conf-with-keys)))

