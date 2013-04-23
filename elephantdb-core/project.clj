(defproject elephantdb/elephantdb-core "0.4.3-SNAPSHOT"
  :min-lein-version "2.0.0"
  :repositories {"conjars.org" "http://conjars.org/repo"}
  :java-source-paths ["src/jvm"]
  :javac-options ["-source" "1.6" "-target" "1.6"]
  :jvm-opts ["-Xmx768m" "-server"]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [jvyaml "1.0.0"]
                 [org.yaml/snakeyaml "1.9"]
                 [commons-io "1.4"]
                 [jackknife "0.1.2"]
                 [hadoop-util "0.3.0"]
                 [metrics-clojure "1.0.1"]
                 [elephantdb/elephantdb-thrift "0.4.3-SNAPSHOT"
                  :exclusions [org.slf4j/slf4j-api]]]
  :profiles {:provided
             {:dependencies [[org.apache.hadoop/hadoop-core "0.20.2"]]}
             :dev
             {:dependencies
              [[midje "1.5.0"]]
              :plugins [[lein-midje "3.0.0"]]}})
