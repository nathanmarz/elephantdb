(defproject elephantdb-deploy "1.0.0-SNAPSHOT"
  :main elephantdb.deploy.provision
  :repositories {"sonatype" "https://oss.sonatype.org/content/repositories/releases"
                 "jclouds-snapshot" "https://oss.sonatype.org/content/repositories/snapshots"}
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]

                 [org.cloudhoist/pallet "0.5.0-SNAPSHOT"
                  :exclusions [org.jclouds/jclouds-jsch
                               org.jclouds/jclouds-log4j
                               org.jclouds.driver/jclouds-jsch
                               org.jclouds.driver/jclouds-log4j
                               org.jclouds.driver/jclouds-enterprise
                               org.jclouds/jclouds-blobstore
                               org.jclouds/jclouds-bouncycastle
                               org.jclouds.driver/jclouds-bouncycastle
                               org.jclouds/jclouds-compute
                               org.jclouds/jclouds-scriptbuilder
                               ]]
                 [org.cloudhoist/java "0.5.0-SNAPSHOT"]
                 [org.cloudhoist/git "0.5.0-SNAPSHOT"]
                 [org.cloudhoist/ssh-key "0.5.0-SNAPSHOT"]
                 [org.cloudhoist/automated-admin-user "0.5.0-SNAPSHOT"]

                 [org.jclouds.provider/aws-ec2 "1.0-SNAPSHOT"]
                 [org.jclouds.provider/aws-s3 "1.0-SNAPSHOT"]

                 [org.jclouds.driver/jclouds-jsch "1.0-SNAPSHOT"]
                 [com.jcraft/jsch "0.1.44-1"]

                 [org.antlr/stringtemplate "3.2"]]

  :dev-dependencies [[swank-clojure "1.2.1"]
                     [org.cloudhoist/pallet-lein "0.2.0"]])
