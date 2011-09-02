(ns elephantdb.deploy.node
  (:use pallet.thread-expr
        pallet.compute
        pallet.core
        [clojure.contrib.def :only (defnk)]
        [pallet.blobstore :only (blobstore-from-config)]
        [pallet.phase :only (phase-fn)]
        [pallet.configure :only (pallet-config compute-service-properties)])
  (:require [pallet.request-map :as request-map]
            [pallet.crate.automated-admin-user :as automated-admin-user]
            [elephantdb.deploy.crate.daemontools :as daemontools]
            [elephantdb.deploy.crate.edb :as edb]
            [elephantdb.deploy.crate.edb-configs :as edb-configs]))

(defn- edb-node-spec [ring local?]
  (let [{port :port} (edb-configs/read-global-conf! ring)]
    (node-spec
     :image (if local?
              {:os-family :ubuntu
               :os-64-bit true}
              {:image-id "us-east-1/ami-08f40561"
               :hardware-id "m1.large"
               :inbound-ports [22 port]}))))

(def edb-server-spec
  (let [fd-limit "500000"
        users ["root" "elephantdb"]]
    (server-spec
     :phases {:bootstrap (phase-fn
                          (automated-admin-user/automated-admin-user)            
                          (edb/filelimits fd-limit users))
              :configure (phase-fn
                          (daemontools/daemontools)
                          (edb/setup)
                          (edb/deploy))})))

(defnk edb-group-spec [ring :local? false]
  (group-spec (str "edb-" ring)
              :node-spec (edb-node-spec ring local?)
              :extends [edb-server-spec]))



