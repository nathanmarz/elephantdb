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
   [pallet.configure :only [pallet-config]]))

(defn my-node-spec [ring]
  (let [{:keys [port]} (config/read-global-config! ring)]
    (node-spec
     :image {:image-id "us-east-1/ami-08f40561"
             :hardware-id "m1.large"
             :inbound-ports [22 port]})))

(defn mytest [ring]
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
                (config/update-global-conf!))}))

(def mytest-ring (mytest "dev4"))


#_ (require '[pallet.parameter :as parameter])
#_ (lift mytest-ring
         :compute aws
         :phase (fn [r] (parameter/assoc-for r [:foo] 3)))
#_ (lift mytest-ring
         :compute aws
         :phase (fn [r] (print (parameter/get-for r [:foo])) r))

#_ (do  (defn mk-aws []
          (compute-service-from-config-file "backtype"))
        (def aws (mk-aws)))

#_ (def mytest2
     (group-spec "myword" :extends mytest))
#_ (env/request-with-environment {:server {:b 3}} {:a 3})


#_ (def w (lift mytest-ring
                :compute aws
                :phase (fn [r] r)
                :prefix "dev3"))
#_ (:prefix  (keys w))
#_ (:groups w)
#_ (lift mytest
         :compute aws
         :phase elephantdb/setup)

#_ (def bs (blobstore-from-config (pallet-config) ["backtype"]))
#_ (require '[pallet.environment :as env])


#_ (require '[pallet.parameter :as param])
#_ (def w (lift mytest-ring :compute aws
                :phase (fn [req]
                         (let [req$ (param/assoc-for-target req [:a] 3)]
                           (print (param/get-for-target req$ [:a]))))))

#_ (param/get-for w [:a])
#_ (:parameters w)
#_ (keys w)

#_ (converge {mytest-ring 1}
          :compute aws
          :environment {:blobstore bs
                        :ring "dev4"})


#_ (def aws (mk-aws))
#_ (-> h
       (assoc :blobstore bs)
       (remote-config! "dev2"))

#_ (def h (converge {mytest 1} :compute aws))
#_ (def h (converge {mytest2 0} :compute aws))


