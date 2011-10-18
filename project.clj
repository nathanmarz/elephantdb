(defproject elephantdb/elephantdb "0.1.1-SNAPSHOT"
  :source-path "src/clj"
  :test-path "test/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :repositories {"oracle" "http://download.oracle.com/maven"}
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/tools.logging "0.2.3"]
                 [jvyaml "1.0.0"]
                 [backtype/thriftjava "1.0.0"]
                 [log4j/log4j "1.2.16"]
                 [com.sleepycat/je "4.1.10"]
                 [hadoop-util "0.2.3"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [swank-clojure "1.4.0-SNAPSHOT"]
                     [lein-marginalia "0.6.1"]]
  :aot [elephantdb.client elephantdb.main]
  :main elephantdb.main)
