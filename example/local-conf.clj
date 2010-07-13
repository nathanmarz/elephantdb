;; This configuration is stored on a per machine basis

{ :local-dir "/data1/elephantdb"
  :max-online-download-rate-kb-s 1024
  :local-db-conf {"elephantdb.JavaBerkDB" {}
                  "elephantdb.TokyoCabinet" {}
                  }
  :hdfs-conf {"fs.default.name" "s3n://hdfs"}
}
