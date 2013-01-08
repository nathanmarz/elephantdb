(defproject elephantdb "0.2.0-SNAPSHOT"
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [jvyaml "1.0.0"]
                 [org.yaml/snakeyaml "1.9"]
                 [commons-io "1.4"]
                 [org.apache.thrift/libthrift "0.8.0"
                  :exclusions [org.slf4j/slf4j-api]]
                 [org.slf4j/slf4j-log4j12 "1.6.4"]
                 [jackknife "0.1.2"]
                 [yieldbot/hadoop-util "0.2.9-SNAPSHOT"]
                 [cascading.kryo "0.4.6"]
                 [com.twitter/carbonite "1.3.1"]
                 [com.sleepycat/je "5.0.58"]
                 [org.apache.lucene/lucene-core "3.0.3"]
                 [org.apache.lucene/lucene-queries "3.0.3"]
                 [org.fusesource.leveldbjni/leveldbjni-all "1.4"]]
  :source-paths ["src/clj"]
  :profiles {:dev
             {:dependencies
              [[midje "1.4.0"
                :exclusions [org.clojure/clojure]]]
              :plugins [[lein-midje "2.0.3"]
                        [lein-pedantic "0.0.5"]]}
             :provided
             {:dependencies
              [[org.apache.hadoop/hadoop-core "1.0.3"]]}}
  :repositories {"oracle" "http://download.oracle.com/maven"
                 "conjars.org" "http://conjars.org/repo"
                 "fusesource.nexus.snapshot" "http://repo.fusesource.com/nexus/content/groups/public-snapshots"}
  :java-source-paths ["src/jvm"]
  :javac-options ["-source" "1.6" "-target" "1.6"]
  :jvm-opts ["-Xmx768m" "-server" "-Djava.net.preferIPv4Stack=true" "-XX:+UseCompressedOops"]
  :pedantic :warn
  :main elephantdb.keyval.core)
