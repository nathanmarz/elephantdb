(defproject elephantdb/elephantdb-server "0.4.3-SNAPSHOT"
  :min-lein-version "2.0.0"
  :source-paths ["src/clj"]
  :jvm-opts ["-Xmx768m" "-server" "-Djava.net.preferIPv4Stack=true" "-XX:+UseCompressedOops"]
  :dependencies [[ch.qos.logback/logback-classic "1.0.11"]
                 [org.slf4j/jul-to-slf4j "1.7.4"]
                 [org.slf4j/jcl-over-slf4j "1.7.4"]
                 [org.slf4j/log4j-over-slf4j "1.7.4"]
                 [com.yammer.metrics/metrics-graphite "2.2.0"]
                 [com.yammer.metrics/metrics-ganglia "2.2.0"]
                 [elephantdb/elephantdb-core "0.4.3-SNAPSHOT"]
                 [elephantdb/elephantdb-bdb "0.4.3-SNAPSHOT"]
                 [elephantdb/elephantdb-leveldb "0.4.3-SNAPSHOT"]]
  :profiles {:provided
             {:dependencies
              [[org.apache.hadoop/hadoop-core "0.20.2"]]}
             :dev
             {:dependencies
              [[midje "1.5.0"]]
              :plugins [[lein-midje "3.0.0"]]}}
  :main elephantdb.keyval.core)
