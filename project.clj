(defproject elephantdb "0.2.1-SNAPSHOT"
  :min-lein-version "2.0.0"
  :dependencies [[elephantdb/elephantdb-core "0.2.1-SNAPSHOT"]
                 [elephantdb/elephantdb-server "0.2.1-SNAPSHOT"]
                 [elephantdb/elephantdb-cascading "0.3.6-SNAPSHOT"]]
  :plugins [[lein-sub "0.2.1"]]
  :sub ["elephantdb-core"
        "elephantdb-server"
        "elephantdb-cascading"])
