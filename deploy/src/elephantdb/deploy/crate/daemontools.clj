(ns elephantdb.deploy.crate.daemontools
  (:use pallet.core
        [pallet.phase :only (phase-fn)]
        [pallet.action.exec-script :only (exec-script)]
        [pallet.resource.package :only (package)]
        [pallet.resource.directory :only (directory)]))

(def daemontools
  (phase-fn
   (package "daemontools-run")
   (directory "/service" :action :create :mode 755)
   (exec-script ("svscan /service &"))))
