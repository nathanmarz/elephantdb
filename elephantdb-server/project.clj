(defproject elephantdb/elephantdb-server "0.4.1"
  :min-lein-version "2.0.0"
  :source-paths ["src/clj"]
  :jvm-opts ["-Xmx768m" "-server" "-Djava.net.preferIPv4Stack=true" "-XX:+UseCompressedOops"]
  :dependencies [[org.slf4j/slf4j-log4j12 "1.6.4"]
                 [com.yammer.metrics/metrics-graphite "2.2.0"]
                 [elephantdb/elephantdb-core "0.4.1"]
                 [elephantdb/elephantdb-bdb "0.4.1"]
                 [elephantdb/elephantdb-leveldb "0.4.1"]]
  :profiles {:provided
             {:dependencies
              [[org.apache.hadoop/hadoop-core "0.20.2"]]}
             :dev
             {:dependencies
              [[midje "1.5.0"]]
              :plugins [[lein-midje "3.0.0"]]}}
  :main elephantdb.keyval.core)
