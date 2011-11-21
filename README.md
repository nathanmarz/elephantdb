# About

ElephantDB is a database that specializes in exporting key/value data from Hadoop. ElephantDB is composed of two components. The first is a library that is used in MapReduce jobs for creating an indexed key/value dataset that is stored on a distributed filesystem. The second component is a daemon that can download a subset of a dataset and serve it in a read-only, random-access fashion. A group of machines working together to serve a full dataset is called a ring.

Since ElephantDB server doesn't support random writes, it is almost laughingly simple. Once the server loads up its subset of the data, it does very little. This leads to ElephantDB being rock-solid in production, since there's almost no moving parts.

ElephantDB server has a Thrift interface, so any language can make reads from it. The database itself is implemented in Clojure.

An ElephantDB datastore contains a fixed number of shards of a "Local Persistence". ElephantDB's local persistence engine is pluggable, and ElephantDB comes bundled with a local persistence implementation for Berkeley DB Java Edition. On the MapReduce side, each reducer creates or updates a single shard into the DFS, and on the server side, each server serves a subset of the shards.

We are currently working on adding hot-swapping to ElephantDB server so that a live server can be updated with a new set of shards. Right now, to update a domain of data you either have to take downtime on the ring or switch between two rings serving the data and update them one at a time.

ElephantDB comes with two companion projects, [elephantdb-cascading](https://github.com/nathanmarz/elephantdb-cascading) and [elephantdb-cascalog](https://github.com/nathanmarz/elephantdb-cascalog), that make it seemless to create ElephantDB datastores from Cascading or Cascalog respectively. 

BackType uses ElephantDB to export views from TBs of data and serve them in the analytics applications and APIs of backtype.com and backtweets.com. As of February 2011, the BackType ElephantDB cluster serves more than 100GBs of data computed from the batch workflow.

# Questions

Google group: [elephantdb-user](http://groups.google.com/group/elephantdb-user)

# Tutorials

[Introducing ElephantDB](http://tech.backtype.com/introducing-elephantdb-a-distributed-database)

# Using ElephantDB in MapReduce Jobs

ElephantDB is hosted at [Clojars](http://clojars.org/elephantdb). Clojars is a maven repo that is trivially easy to use with maven or leiningen. You should use this dependency when using ElephantDB within your MapReduce jobs to create ElephantDB datastores.

# Deploying ElephantDB server

## Setting Up

ElephantDB uses [Pallet](https://github.com/pallet/pallet) for deploys; the main Pallet config is stored in `config.clj`, located in the `.pallet` directory. Go ahead and create this directory and file, and open up `config.clj` in your favorite text editor.

Add the following form to `config.clj`, replacing appropriate fields with your information:

    (defpallet
      :services
      {:elephantdb
       {:configs {:example
                  {:global {:replication 1
                            :node-count 1
                            :port 3578
                            :domains {"example-domain" "s3n://yourbucket/example-shards"}}
                   :local {:local-dir "/mnt/elephantdb"
                           :max-online-download-rate-kb-s 1024
                           :local-db-conf {"elephantdb.persistence.JavaBerkDB" {}
                                           "elephantdb.persistence.TokyoCabinet" {}}
                           :hdfs-conf {"fs.default.name" "s3n://yourbucket"
                                       "fs.s3n.awsAccessKeyId" "youraccesskey"
                                       "fs.s3n.awsSecretAccessKey" "yoursecret"}}}}
        :data-creds {:blobstore-provider "aws-s3"
                     :provider "aws-ec2"
                     :identity "youraccesskey"
                     :credential "yoursecret"}
        :deploy-creds {:blobstore-provider "aws-s3"
                       :provider "aws-ec2"
                       :identity "youraccesskey"
                       :credential "yoursecret"}}})

(If you have an existing `.pallet/config.clj`, add the `:elephantdb` kv pair under `:services`.)

Add new domains to the EDB ring by adding kv pairs under the `:domains` key of global config.

## Deploying

To deploy ElephantDB, you'll need to install [Leiningen](https://github.com/technomancy/leiningen). Once that's complete, run the following commands to prep your system for deploy:

    $ git clone git://github.com/sritchie/elephantdb-deploy.git
    $ cd elephantdb-deploy
    $ lein deps

These commands will pull in all dependencies required to deploy ElephantDB. Finally, run:

    $ lein run --start --ring <your-ring-name>

When the task completes, you'll be presented with the public and private IPs of your running EDB server. That's it!

TODO: Tutorial on connecting to EDB with a thrift service.

# Running the EDB Jar

To launch an ElephantDB server, you must run the class elephantdb.main with three command line arguments, described below. ElephantDB also requires the Hadoop jars in its classpath.

ring-configuration-path: A path on the distributed filesystem where the ring configuration is. The ring configuration indicates what domains of data to serve, where to find that data, what port to run on, what replication factor to use, and a list of all the servers participating in serving the data. An example is given in example/global-conf.clj

local-config-path: A local path containing the local configuration. The local configuration contains the local dir, where ElephantDB should cache shards it downloads from the DFS, as well as configuration so that ElephantDB knows which distributed filesystem to talk to. An example is given in example/local-conf.clj.

token: The token can be any string. The token is used to indicate to ElephantDB whether it should refresh its data cache with what exists on the DFS, or whether ElephantDB should just start up using its local cache. As long as the token given is different than what ElephantDB was given the last time it successfully loaded, ElephantDB will refresh its local cache. Typically you update the token with the current timestamp when you want to update the data it serves.

# Developing on EDB

*The following documentation is in progress!*

* Building Thrift
* Generating schema files

## Next Steps

Lucene integration:
 
    [org.apache.lucene/lucene-core "3.4.0"]

## Program Flow

When shards get written to a domain, a `domain-spec.yaml` file shows up too. Here's an example:

     --- 
     local_persistence: elephantdb.persistence.JavaBerkDB
     num_shards: 32

The `local_persistence` is a string version of LocalPersistenceFactory. This LocalPersistenceFactory subclass will define the nature of the key-value store being worked with.

## Interface notes

* Tap needs to take as an option some object that will allow for proper serialization of keys and values. Right 
* NOTE CURRENTLY that if you don't pass in null, you'll get a replace updater. If the passed-in updater is set to nil, the updateDirHdfs never gets set and you wont' load remote shards for updating! This magic happens in ElephantDBTap; the real sauce is in the elephantdb output format.
** Instead, should we have a `:do-update` flag?

// Pushed into interfaces:
int shardIndex(key, val)       // both unserialized
int shardIndex(key, val, opts) // both unserialized, again

byte[] serializeKey(Object key);
byte[] deserializeKey(Object key);

byte[] serializeVal(Object val);
byte[] deserializeVal(Object val);

void updateElephant(LocalPersistence lp, byte[] newKey, byte[] newVal);

We need to extend the INPUT FORMAT and override:

    createKey
    createVal
   next(Object k, Object v)

So that we can interact properly with a custom iterator. That or farm out the work inside of "next" to an interfaced method:

    processNext(k, v, nextVal);

On the output format side, we have:

    _args.updater.updateElephant(lp, record.key, record.val);


## Local persistence formatt
