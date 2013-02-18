(defproject elephantdb/elephantdb-thrift "0.4.0-SNAPSHOT"
  :min-lein-version "2.0.0"
  :java-source-paths ["src/jvm"]
  :dependencies [[org.apache.thrift/libthrift "0.8.0"]]
  :profiles {:dev
             {:dependencies
              [[org.clojure/clojure "1.4.0"]
               [midje "1.5-alpha9"]]
              :plugins [[lein-midje "3.0-alpha4"]]}})