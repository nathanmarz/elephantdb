(defproject elephantdb/elephantdb-core "0.4.0-RC1"
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
                 [hadoop-util "0.2.9"]
                 [com.yammer.metrics/metrics-core "2.2.0"]
                 [metrics-clojure "0.9.2"
                  :exclusions [org.clojure/clojure
                               com.yammer.metrics/metrics-core]]
                 [elephantdb/elephantdb-thrift "0.4.0-RC1"
                  :exclusions [org.slf4j/slf4j-api]]]
  :profiles {:dev
             {:dependencies
              [[midje "1.5.0"]
               [org.apache.hadoop/hadoop-core "0.20.2"]]
              :plugins [[lein-midje "3.0.0"]]}})
