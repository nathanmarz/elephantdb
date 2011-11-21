(defproject elephantdb/elephantdb "0.2.0"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :repositories {"oracle" "http://download.oracle.com/maven"}
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/tools.logging "0.2.3"]
                 [jvyaml "1.0.0"]
                 [backtype/thriftjava "1.0.0"]
                 [log4j/log4j "1.2.16"]
                 [hadoop-util "0.2.3"]
                 [com.sleepycat/je "4.1.10"]
                 [org.apache.lucene/lucene-core "3.0.3"]
                 [org.apache.lucene/lucene-queries "3.0.3"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [swank-clojure "1.4.0-SNAPSHOT"]
                     [lein-marginalia "0.6.1"]
                     [midje "1.3-alpha4"]
                     [lein-midje "1.0.4"]]
  :aot [elephantdb.keyval.main]
  :main elephantdb.keyval.main)
