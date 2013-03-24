(defproject elephantdb "0.4.0-RC1"
  :min-lein-version "2.0.0"
  :dependencies [[elephantdb/elephantdb-thrift "0.4.0-RC1"]
                 [elephantdb/elephantdb-core "0.4.0-RC1"]
                 [elephantdb/elephantdb-bdb "0.4.0-RC1"]
                 [elephantdb/elephantdb-leveldb "0.4.0-RC1"]
                 [elephantdb/elephantdb-server "0.4.0-RC1"]
                 [elephantdb/elephantdb-cascading "0.4.0-RC1"]]
  :plugins [[lein-sub "0.2.1"]]
  :sub ["elephantdb-thrift"
        "elephantdb-core"
        "elephantdb-bdb"
        "elephantdb-leveldb"
        "elephantdb-server"
        "elephantdb-cascading"])
