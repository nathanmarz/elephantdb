(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp))

(defproject elephantdb/elephantdb-server VERSION
  :min-lein-version "2.0.0"
  :source-paths ["src/clj"]
  :jvm-opts ["-Xmx768m" "-server" "-Djava.net.preferIPv4Stack=true" "-XX:+UseCompressedOops"]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [ch.qos.logback/logback-classic "1.0.11"]
                 [org.slf4j/jul-to-slf4j "1.7.4"]
                 [org.slf4j/jcl-over-slf4j "1.7.4"]
                 [org.slf4j/log4j-over-slf4j "1.7.4"]
                 [com.yammer.metrics/metrics-graphite "2.2.0"]
                 [com.yammer.metrics/metrics-ganglia "2.2.0"]
                 [compojure "1.1.5"]
                 [hiccup-bootstrap "0.1.2"]
                 [ring/ring-core "1.1.8"]
                 [ring/ring-jetty-adapter "1.1.8"]
                 [elephantdb/elephantdb-core ~VERSION]
                 [elephantdb/elephantdb-client ~VERSION]
                 [elephantdb/elephantdb-bdb ~VERSION]
                 [elephantdb/elephantdb-leveldb ~VERSION]]
  :ring {:handler elephantdb.ui.handler/app}
  :plugins [[lein-ring "0.8.5"]]
  :profiles {:provided
             {:dependencies
              [[org.apache.hadoop/hadoop-core "1.2.1"]]}
             :dev
             {:dependencies
              [[ring-mock "0.1.3"]
               [midje "1.6.3"]]
              :plugins [[lein-midje "3.1.3"]]}}
  :aot :all
  :main elephantdb.keyval.core)
