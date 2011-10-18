(defproject elephantdb/elephantdb "0.1.0-SNAPSHOT"
  :source-path "src/clj"
  :test-path "test/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :repositories {"private"
                 {:url "http://ec2-72-44-56-146.compute-1.amazonaws.com/repo/"
                  :username "backtype"
                  :password "btpreview"}
                 "oracle" "http://download.oracle.com/maven"}
  :main elephantdb.main
  :dependencies [[org.clojure/clojure "1.2.1"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [jvyaml "1.0.0"]
                 [backtype/thriftjava "1.0.0"]
                 [log4j/log4j "1.2.16"]
                 [com.sleepycat/je "4.1.10"]
                 [hadoop-util "0.2.1"]]
  :dev-dependencies [[swank-clojure "1.4.0-SNAPSHOT"]
                     [clojure-source "1.2.0"]
                     [lein-marginalia "0.6.0"]
                     [lein-app "1.0.0"]
                     [org.apache.hadoop/hadoop-core "0.20.2-dev"]]
  :aot [elephantdb.client elephantdb.main]
  :app-app {:bastion "nest1.corp.twitter.com"
            :jar-name "elephantdb-server.jar"
            :app-user  "backtype"
            :app-group "backtype"
            :layout-size "10G"
            :include-dev true})
