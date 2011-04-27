(ns elephantdb.deploy.crate.daemontools
  (:use [pallet.core]
        [pallet.action
         [exec-script :only [exec-script]]]
        [pallet.resource
         [package :only [package]]
         [directory :only [directory]]]))

(defn daemontools [req]
  (-> req
      (package "daemontools-run")
      (directory "/service" :action :create :mode 755)
      (exec-script
       ("svscan /service &"))))
