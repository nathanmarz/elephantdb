(defproject elephantdb/elephantdb-thrift "0.4.5-SNAPSHOT"
  :min-lein-version "2.0.0"
  :java-source-paths ["src/jvm"]
  :javac-options ["-source" "1.6" "-target" "1.6"]
  :dependencies [[org.apache.thrift/libthrift "0.8.0"]]
  :profiles {:dev
             {:dependencies
              [[org.clojure/clojure "1.5.1"]
               [midje "1.5.0"]]
              :plugins [[lein-midje "3.0.0"]]}})
