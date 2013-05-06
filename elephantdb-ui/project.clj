(defproject elephantdb/elephantdb-ui "0.4.4-SNAPSHOT"
  :description "ElephantDB UI"
  :url "https://githib.com/nathanmarz/elephantdb"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [compojure "1.1.5"]
                 [hiccup-bootstrap "0.1.2"]
                 [elephantdb/elephantdb-client "0.4.4-SNAPSHOT"]
                 [sonian/carica "1.0.2"]
                 [clj-time "0.4.5"]
                 [ring/ring-core "1.1.8"]
                 [ring/ring-jetty-adapter "1.1.8"]
                 [ch.qos.logback/logback-classic "1.0.11"]]
  :plugins [[lein-ring "0.8.5"]]
  :source-paths ["src/clj"]
  :ring {:handler elephantdb.ui.handler/app}
  :main elephantdb.ui.core
  :profiles
  {:dev {:dependencies [[ring-mock "0.1.3"]]}})
