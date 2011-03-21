(ns elephantdb.deploy.daemontools
  (:use [pallet.core]
        [pallet.resource
         [package :only [package]]
         [directory :only [directory]]
         [exec-script :only [exec-script]]]))

(defn daemontools [req]
  (-> req
      (package "daemontools-run")
      (directory "/service" :action :create :mode 755)
      (exec-script
       ("svscan /service &"))))
