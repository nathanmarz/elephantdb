(defproject elephantdb/elephantdb "0.2.0-wip1"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :jvm-opts ["-Xmx768m" "-server"]
  :repositories {"oracle" "http://download.oracle.com/maven"
                 "releases" "http://artifactory.local.twitter.com/libs-releases-local"}
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [jvyaml "1.0.0"]
                 [org.yaml/snakeyaml "1.9"]
                 [commons-io "1.4"]
                 [storm/libthrift7 "0.7.0"]

                 ;; do we need this?
                 ;; [org.slf4j/slf4j-api "1.6.1"]
                 [edb/kryonet "1.0.4"]
                 [jackknife "0.1.2"]
                 [hadoop-util "0.2.7"]
                 [cascading.kryo "0.2.1"]
                 [cascalog/carbonite "1.1.0"]
                 [com.sleepycat/je "5.0.34"]
                 [org.apache.lucene/lucene-core "3.0.3"]
                 [org.apache.lucene/lucene-queries "3.0.3"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2.cdh3u2-t1"]
                     [midje "1.3.1" :exclusions [org.clojure/clojure]]
                     [lein-midje "1.0.8"]]
  :main elephantdb.keyval.core)

