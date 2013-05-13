(defproject elephantdb/elephantdb-client "0.4.4"
  :description "A client interface to ElephantDB"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [ch.qos.logback/logback-classic "1.0.11"]
                 [elephantdb/elephantdb-thrift "0.4.4"
                  :exclusions [org.slf4j/slf4j-api]]]
  :profiles {:dev
             {:dependencies [[midje "1.5.0"]]
              :plugins [[lein-midje "3.0.0"]]}}
  :source-paths ["src/clj"]
  :warn-on-reflection true)
