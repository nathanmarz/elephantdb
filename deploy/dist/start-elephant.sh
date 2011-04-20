#!/bin/sh

java -server -Xmx5120m -cp `sh classpath.sh` elephantdb.main $1 local-conf.clj $2
