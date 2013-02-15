(defproject elephantdb/elephantdb-core "0.4.0-SNAPSHOT"
  :min-lein-version "2.0.0"
  :repositories {"oracle" "http://download.oracle.com/maven"
                 "conjars.org" "http://conjars.org/repo"
                 "fusesource.nexus.snapshot" "http://repo.fusesource.com/nexus/content/groups/public-snapshots"}
  :java-source-paths ["src/jvm"]
  :javac-options ["-source" "1.6" "-target" "1.6"]
  :jvm-opts ["-Xmx768m" "-server"]
  :dependencies [[jvyaml "1.0.0"]
                 [org.yaml/snakeyaml "1.9"]
                 [commons-io "1.4"]
                 [org.apache.thrift/libthrift "0.8.0"
                  :exclusions [org.slf4j/slf4j-api]]
                 [jackknife "0.1.2"]
                 [hadoop-util "0.2.9"]
                 [com.sleepycat/je "5.0.58"]
                 [org.fusesource.leveldbjni/leveldbjni-all "1.4"]]
  :profiles {:dev
             {:dependencies
              [[org.clojure/clojure "1.4.0"]
               [midje "1.5-alpha9"]]
              :plugins [[lein-midje "3.0-alpha4"]]}
             :provided
             {:dependencies
              [[org.apache.hadoop/hadoop-core "0.20.2-dev"]]}})
