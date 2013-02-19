(defproject elephantdb "0.4.0-SNAPSHOT"
  :min-lein-version "2.0.0"
  :dependencies [[elephantdb/elephantdb-thrift "0.4.0-SNAPSHOT"]
                 [elephantdb/elephantdb-core "0.4.0-SNAPSHOT"]
                 [elephantdb/elephantdb-bdb "0.4.0-SNAPSHOT"]
                 [elephantdb/elephantdb-leveldb "0.4.0-SNAPSHOT"]
                 [elephantdb/elephantdb-server "0.4.0-SNAPSHOT"]
                 [elephantdb/elephantdb-cascading "0.4.0-SNAPSHOT"]]
  :plugins [[lein-sub "0.2.1"]]
  :profiles {:dev
             {:dependencies [[midje "1.5-alpha9"]]
              :plugins [[lein-midje "3.0-alpha4"]]}}
  :sub ["elephantdb-thrift"
        "elephantdb-core"
        "elephantdb-bdb"
        "elephantdb-leveldb"
        "elephantdb-server"
        "elephantdb-cascading"])
