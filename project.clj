(defproject elephantdb/elephantdb "0.2.0"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :jvm-opts ["-Xmx768m" "-server"]
  :repositories {"oracle" "http://download.oracle.com/maven"}
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [jvyaml "1.0.0"]
                 [org.yaml/snakeyaml "1.9"]
                 [commons-io "1.4"]
                 [storm/libthrift7 "0.7.0"]
                 [edb/kryonet "1.0.4"]
                 [jackknife "0.1.2"]
                 [hadoop-util "0.2.7"]
                 [cascading.kryo "0.1.5"]
                 [cascalog/carbonite "1.1.0"]
                 [com.sleepycat/je "5.0.34"]
                 [org.apache.lucene/lucene-core "3.0.3"]
                 [org.apache.lucene/lucene-queries "3.0.3"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [midje "1.3.0"]]
  :main elephantdb.keyval.core)

