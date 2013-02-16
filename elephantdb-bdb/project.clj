(defproject elephantdb/elephantdb-bdb "0.4.0-SNAPSHOT"
  :min-lein-version "2.0.0"
  :java-source-paths ["src/jvm"]
  :javac-options ["-source" "1.6" "-target" "1.6"]
  :repositories {"oracle" "http://download.oracle.com/maven"}
  :dependencies [[elephantdb/elephantdb-core "0.4.0-SNAPSHOT"]
                 [com.sleepycat/je "5.0.58"]]
  :profiles {:dev
             {:dependencies
              [[org.clojure/clojure "1.4.0"]
               [midje "1.5-alpha9"]]
              :plugins [[lein-midje "3.0-alpha4"]]}})
