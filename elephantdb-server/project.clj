(defproject elephantdb/elephantdb-server "0.4.0-SNAPSHOT"
  :min-lein-version "2.0.0"
  :source-paths ["src/clj"]
  :jvm-opts ["-Xmx768m" "-server" "-Djava.net.preferIPv4Stack=true" "-XX:+UseCompressedOops"]
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.slf4j/slf4j-log4j12 "1.6.4"]
                 [elephantdb/elephantdb-core "0.4.0-SNAPSHOT"]]
  :profiles {:dev
             {:dependencies
              [[elephantdb/elephantdb-leveldb "0.4.0-SNAPSHOT"]
               [midje "1.5-alpha9"]]
              :plugins [[lein-midje "3.0-alpha4"]]}
             :provided
             {:dependencies
              [[org.apache.hadoop/hadoop-core "0.20.2-dev"]]}}
  :main elephantdb.keyval.core)
