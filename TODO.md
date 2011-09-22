## Steps to boot ##

1. Take in local and global configs
2. Launch updater process, start the thrift server serving.

* client  -  
* config  - DONE
* domain  - 
* hadoop  - 
* loader  - 
* log     - DONE
* main    - DONE
* service - 
* shard   - 
* testing - 
* thrift  - 
* types   - 
* utils   - 


Notes on conf files. hdfs-conf is fed into hadoop's filesystem maker:
(FileSystem/get (configuration conf-map))

* Note that right now, we have to have hdfs conf like:

:hdfs-conf {"fs.default.name" "hdfs://ec2-184-72-201-146.compute-1.amazonaws.com:9000"
           "fs.s3n.awsAccessKeyId" keyid
           "fs.s3n.awsSecretAccessKey" accesskey}

Make it so this works fine if it fails.
