(defproject elephantdb-deploy "1.0.0-SNAPSHOT"
  :main elephantdb.deploy.provision
  :repositories {"sonatype" "https://oss.sonatype.org/content/repositories/releases"
                 "jclouds-snapshot" "https://oss.sonatype.org/content/repositories/snapshots"}
  :dependencies [[org.clojure/clojure "1.2.1"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [org.antlr/stringtemplate "3.2"]]
  :dev-dependencies [[swank-clojure "1.4.0-SNAPSHOT"]
                     [clojure-source "1.2.0"]
                     [org.cloudhoist/pallet "0.6.2"]
                     ;; [vmfest "0.2.4-SNAPSHOT"]
                     ;; [org.cloudhoist/pallet "0.7.1-SNAPSHOT"]
                     [org.cloudhoist/java "0.5.0"]
                     [org.cloudhoist/git "0.5.0"]
                     [org.cloudhoist/ssh-key "0.5.0"]
                     [org.cloudhoist/automated-admin-user "0.5.0"]
                     [org.jclouds.provider/aws-ec2 "1.0.0"]
                     [org.jclouds.provider/aws-s3 "1.0.0"]
                     [org.jclouds.driver/jclouds-jsch "1.0.0"]
                     [com.jcraft/jsch "0.1.44-1"]])
