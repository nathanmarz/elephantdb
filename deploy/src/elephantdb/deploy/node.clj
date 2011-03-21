(ns elephantdb.deploy.node
  (:require
   [elephantdb.deploy
    [daemontools :as daemontools]
    [elephantdb :as elephantdb]
    [config :as config]]
   [pallet
    [request-map :as request-map]])
  (:use
   [pallet compute core thread-expr]
   [pallet.crate
    [automated-admin-user :only [automated-admin-user]]]
   [pallet.blobstore :only [blobstore-from-config]]
   [pallet.resource :only [phase]]
   [pallet.configure :only [pallet-config compute-service-properties]]))

(defn my-node-spec [ring]
  (let [{:keys [port]} (config/read-global-conf! ring)]
    (node-spec
     :image {:image-id "us-east-1/ami-08f40561"
             :hardware-id "m1.large"
             :inbound-ports [22 port]})))

(defn gen-edb-spec [ring]
  (group-spec
   (str "edb-" ring)
   :node-spec (my-node-spec ring)
   :phases
   {:bootstrap (phase
                (automated-admin-user)
                (daemontools/daemontools))
    :configure (phase
                (elephantdb/setup)
                (elephantdb/deploy)
                (config/upload-global-conf!))}))

(def edb-spec (mytest "dev5"))



#_ (do  (defn mk-aws []
          (compute-service-from-config-file "backtype"))
        (def aws (mk-aws)))

#_ (converge {edb-spec 1}
          :compute (compute-service-from-config-file "backtype")
          :environment
          {:blobstore (blobstore-from-config (pallet-config) ["backtype"])
           :ring "dev5"
           :edb-s3-keys (compute-service-properties (pallet-config)
                                                    ["backtype"])})




