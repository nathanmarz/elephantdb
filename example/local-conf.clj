;; This configuration is stored on a per machine basis

{ :local-dir "/data1/elephantdb"
  :max-online-download-rate-kb-s 1024
  :update-interval-s 60  ;; check for domain updates every minute
  :local-db-conf {"elephantdb.persistence.JavaBerkDB" {}
                  "elephantdb.persistence.TokyoCabinet" {}
                  }
  :hdfs-conf {"fs.default.name" "s3n://hdfs"}
}
