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
* NOTE CURRENTLY that if you don't pass in null, you'll get a replace indexer. If the passed-in indexer is set to nil, the updateDirHdfs never gets set and you wont' load remote shards for updating! This magic happens in ElephantDBTap; the real sauce is in the elephantdb output format.
** Instead, should we have a `:do-update` flag?

interface KeySharder {
    // Useful in Shardize class!
    int shardIndex(Object key, Object val); // both unserialized

    byte[] serializeKey(Object key);
    byte[] deserializeKey(Object key);

    byte[] serializeVal(Object val);
    byte[] deserializeVal(Object val);
}

Really, though, this can happen on the client side OR at the shard itself.

// OR should we just put out writable objects? We might not need straight byte arrays;

// Pushed into interfaces:

called on the Indexer.
    void updateElephant(Persistence lp, byte[] newKey, byte[] newVal);

We need to extend the INPUT FORMAT and override:

    createKey
    createVal
   next(Object k, Object v)

HMM, or, right now we create BytesWritable objects and do everything that way.
So the lucene index would be responsible for gt 

So that we can interact properly with a custom iterator. That or farm out the work inside of "next" to an interfaced method:

    processNext(k, v, nextVal);

On the output format side, we have:
   
    _args.indexer.updateElephant(lp, record.key, record.val);

## LocalPersistenceFactory

* Returns a LocalPersistence
** Split the get method out of here! That should 

## InputFormat and OutputFormat

initializeArgs

create local persistence -> LocalPersistenceFactory.create, openForAppend

indexer -> Indexer.index(LocalPersistence, Record)

## API Ideas

    (with-elephant edb-instance
        (get "face"))

modeled after clutch.

* Pseudocode for new Interfaces: https://gist.github.com/76f6ea67a874bc7c0f45



### NOTES:

* Serialize keys with Kryo, for the various sharding schemes
  * Tap accepts [shard-key, Document] pairs and a ShardScheme for config

a sharding scheme includes: 1) a mapreduce workflow to compute the state for the shards 2) state that's kept in the domain spec 3) a function to compute the shard given a shard key and the state


### Cascading -> Persistence

ElephantTap:
* Sink (key, value) pairs

Behind the scenes, ElephantTap serializes your key with Kryo to get a shard:
    
    int shardIndex(int numShards, Object k, Object shardState) // from DomainSpec

And generates a KeyValDocument (implements Document):

    KeyValueDocument makeDoc(k, v)

This will have an accompanying Kryo serialization.

The HashMod implementation will group on the resulting shard AND do a secondary sort by the sharding key.

EDB then sinks the shard and KeyValueDocument:

    BytesWritable b = new BytesWritable(..kryo-serialized KeyValueDocument..);
    outputCollector.collect(new IntWritable(shard), b);

The OutputFormat deserializes the shard and KeyValueDocument using an identical Kryo serialization instance. Next step:

    keyValueIndexer.index(LocalPersistence lp, KeyValueDocument d );

The key value indexer will unpack the key and value and call

    bdbPersistence.add(k, v);

Then from here, we've got the same implementation as before.

#### QUESTIONS: 

For this particular implementation, how do we handle key and value serialization? Right now we automatically handle byte, string, int and long keys. Do we automatically serialize these with a bare instance of Kryo now? And what do with non-byte-array values? It's beautiful except for the long-term persistence characteristics of Kryo.

### Persistence -> Cascading

Iterator returns KeyValueDocument objects; these are serialized over the wire with Kryo and appear at the tap. We should deserialize keys and values with Kryo, I think, since byte arrays stay the same.

ISSUE:
For our particular key-value database, I think we should just serialize everything that comes in using Kryo. This would require the Indexer to have access to the DomainSpec, so that it could have access to the serialization information. The idea is sound because byte arrays can pass in and out, so thrift objects won't be affected.

### Persistence -> Thrift

When a key comes in, we serialize it with Kryo (instance booted up from information inside of the DomainSpec) and look up the corresponding value (deserialized with kryo).

Single point of contact for various DomainStore questions. ICoordinator returns a


### More Notes on Client interface

This is a good interface:

```clojure
(defprotocol IDomainStore
  (remote-versions [_] "Returns a sequence of available remote versions.")
  (local-versions [_] "Returns a sequence of available local version strings.")
  (pull-version [_ other-store other-version] "Transfers other version on other-store to this domain store.")
  (push-version [_ other-store this-version] "Transfers the supplied version to the remote domain store.")
  (sync [_ other-store & opts] "Sync versions; Opts can specify whether to sync two-ways or one-way (which way?)")
  (compact [_ keep-fn] keep-fn receives a sequence of versions and returns a sequence of versions-to-keep.")
  (cleanup [_ keep-n] "destroy all versions but the last keep-n.")
  (domain-spec [_] "Returns instanced of DomainSpec."))
```

Beyond that, we need some way for a DomainStore to give us access to its ICoordinator. the database really IS an ICoordinator; or really a set of the local persistences on the machine.

Does the user need to know about current replication?

### Notes

getString, etc, all of that is for the particular USER to figure out.

The actual "Domain Data" is a collection of LocalPersistence objects.


### TODO

* Implement Ostrich's [service interface](https://github.com/twitter/ostrich/blob/master/src/main/scala/com/twitter/ostrich/admin/Service.scala)
* [Register with Ostrich](https://github.com/twitter/ostrich/blob/master/src/main/scala/com/twitter/ostrich/admin/config/ServerConfig.scala#L31)
