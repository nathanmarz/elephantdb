(defproject elephantdb-deploy "1.0.0-SNAPSHOT"
  :description "FIXME: write"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]

                 [org.cloudhoist/pallet "0.5.0-SNAPSHOT"]
                 [org.cloudhoist/java "0.5.0-SNAPSHOT"]
                 [org.cloudhoist/git "0.5.0-SNAPSHOT"]
                 [org.cloudhoist/ssh-key "0.5.0-SNAPSHOT"]
                 [org.cloudhoist/automated-admin-user "0.5.0-SNAPSHOT"]

                 [org.jclouds.provider/aws-ec2 "1.0-beta-9b"]
                 [org.jclouds.provider/aws-s3 "1.0-beta-9b"]
                 [org.jclouds.driver/jclouds-jsch "1.0-beta-9b"]
                 [org.jclouds.driver/jclouds-log4j "1.0-beta-9b"]
                 [org.jclouds.driver/jclouds-enterprise "1.0-beta-9b"]

                 [org.antlr/stringtemplate "3.2"]]

  :dev-dependencies [[swank-clojure "1.2.1"]
                     [org.cloudhoist/pallet-lein "0.2.0"]])
