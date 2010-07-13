;; This configuration is stored globally on HDFS

{ :replication 2
  :hosts ["elephant1.server" "elephant2.server" "elephant3.server"]
  :port 3578
  :domains {"graph" "s3n://mybucket/elephantdb/graph"
            "docs"  "/data/docdb"
            }
}