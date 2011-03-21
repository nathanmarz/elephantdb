(ns elephantdb.deploy.config
  (:use
   [pallet.compute]
   [org.jclouds.blobstore :only [upload-blob]]
   [pallet.request-map :only [nodes-in-group]]
   [pallet.configure :only [pallet-config]]
   [pallet.resource.remote-file :only [remote-file]]
   [clojure.contrib.map-utils :only [deep-merge-with]]))

(defn read-global-conf! [ring]
  (let [path (format "conf/%s/global-conf.clj" ring)]
    (read-string (slurp path))))

(defn read-local-conf! [ring]
  (let [path (format "conf/%s/local-conf.clj" ring)]
    (read-string (slurp path))))

(defn- global-conf-with-hosts [req local-config]
  (let [hosts (map private-ip (nodes-in-group req))]
    (prn-str (assoc local-config :hosts hosts))))

(defn upload-global-conf! [req]
  (let [local-conf (read-global-conf! (:ring req))
        s3-conf (global-conf-with-hosts req local-conf)
        s3-key (format "configs/elephantdb/%s/global-conf.clj" (:ring req))]
    (upload-blob "hdfs2" s3-key s3-conf (:blobstore req))
    req))

(defn local-conf-with-keys [req local-conf]
  (let [{:keys [edb-s3-keys]} req]
    (deep-merge-with #(identity %2)
     {:hdfs-conf 
      {"fs.s3n.awsAccessKeyId" (:identity edb-s3-keys)
       "fs.s3n.awsSecretAccessKey" (:credential edb-s3-keys)}}
     local-conf)))

(defn remote-file-local-conf! [req dst-path]
  (let [conf (read-local-conf! (:ring req))
        conf-with-keys (local-conf-with-keys req conf)]
    (-> req
        (remote-file dst-path :content (prn-str conf-with-keys)))))

