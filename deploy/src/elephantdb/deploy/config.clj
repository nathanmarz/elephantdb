(ns elephantdb.deploy.config
  (:use
   [pallet.compute]
   [org.jclouds.blobstore :only [upload-blob]]
   [pallet.request-map :only [nodes-in-group]]))

(defn read-global-conf! [ring]
  (let [path (format "conf/%s/global-conf.clj" ring)]
    (read-string (slurp path))))

(defn read-local-conf! [ring])

(defn- global-conf-with-hosts [req local-config]
  (let [hosts (map private-ip (nodes-in-group req))]
    (prn-str (assoc local-config :hosts hosts))))

(defn upload-global-conf! [req]
  (let [local-conf (read-global-conf! (:ring req))
        s3-conf (global-conf-with-hosts req local-conf)
        s3-key (format "configs/elephantdb/%s/global-conf.clj" (:ring req))]
    (upload-blob "hdfs2" s3-key s3-conf (:blobstore req))))


#_   (use '[pallet.blobstore :only [blobstore-from-config]])
#_    (use '[ pallet.configure :only [pallet-config]])

#_ (def bs (blobstore-from-config (pallet-config) ["backtype"]))

#_    (def w1 (upload-blob "hdfs2" "tmp/foo.txt"
                          (apply str (take (* 1024 1024 1) (repeat \b)))
                          bs))
#_    (def w2 (upload-blob "hdfs2" "tmp/foo.txt"
                          (apply str (take (* 1024 1024 4) (repeat \a)))
                          bs))
3

#_ (map private-ip (rm/nodes-in-group w "mygroup"))



#_ (blobstore-from-config (pallet-config) ["backtype"])

#_ (require '[pallet.configure :as configure])
#_ (def bs
        (configure/compute-service-properties (pallet-config) ["backtype"]))



#_ (upload-s3-config! "dev2" ["foo"] bs)

#_ (def )


#_ (b/blobstore-from-cnfig "~/.pallet/config.clj" "backtype")

