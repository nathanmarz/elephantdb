# About

ElephantDB is a database that specializes in exporting key/value data from Hadoop. ElephantDB is composed of two components. The first is a library that is used in MapReduce jobs for creating an indexed key/value dataset that is stored on a distributed filesystem. The second component is a daemon that can download a subset of a dataset and serve it in a read-only, random-access fashion. A group of machines working together to serve a full dataset is called a ring.

Since ElephantDB server doesn't support random writes, it is almost laughingly simple. Once the server loads up its subset of the data, it does very little. This leads to ElephantDB being rock-solid in production, since there's almost no moving parts.

ElephantDB server has a Thrift interface, so any language can make reads from it. The database itself is implemented in Clojure.

An ElephantDB datastore contains a fixed number of shards of a "Local Persistence". ElephantDB's local persistence engine is pluggable, and ElephantDB comes bundled with a local persistence implementation for Berkeley DB Java Edition. On the MapReduce side, each reducer creates or updates a single shard into the DFS, and on the server side, each server serves a subset of the shards.

We are currently working on adding hot-swapping to ElephantDB server so that a live server can be updated with a new set of shards. Right now, to update a domain of data you either have to take downtime on the ring or switch between two rings serving the data and update them one at a time.

ElephantDB comes with two companion projects, [elephantdb-cascading](https://github.com/nathanmarz/elephantdb-cascading) and [elephantdb-cascalog](https://github.com/nathanmarz/elephantdb-cascalog), that make it seemless to create ElephantDB datastores from Cascading or Cascalog respectively. 

# Questions

Google group: [elephantdb-user](http://groups.google.com/group/elephantdb-user)


# Using ElephantDB in MapReduce Jobs

ElephantDB is hosted at [Clojars](http://clojars.org/elephantdb). Clojars is a maven repo that is trivially easy to use with maven or leiningen. You should use this dependency when using ElephantDB within your MapReduce jobs to create ElephantDB datastores.

# Deploying ElephantDB server

To build ElephantDB, you will need to install [Leiningen](https://github.com/technomancy/leiningen). Run "lein deps && lein uberjar" to create a jar containing ElephantDB and all it's dependencies. 

To launch an ElephantDB server, you must run the class elephantdb.main with three command line arguments, described below. ElephantDB also requires the Hadoop jars in its classpath.

ring-configuration-path: A path on the distributed filesystem where the ring configuration is. The ring configuration indicates what domains of data to serve, where to find that data, what port to run on, what replication factor to use, and a list of all the servers participating in serving the data. An example is given in example/global-conf.clj

local-config-path: A local path containing the local configuration. The local configuration contains the local dir, where ElephantDB should cache shards it downloads from the DFS, as well as configuration so that ElephantDB knows which distributed filesystem to talk to. An example is given in example/local-conf.clj.

token: The token can be any string. The token is used to indicate to ElephantDB whether it should refresh its data cache with what exists on the DFS, or whether ElephantDB should just start up using its local cache. As long as the token given is different than what ElephantDB was given the last time it successfully loaded, ElephantDB will refresh its local cache. Typically you update the token with the current timestamp when you want to update the data it serves.


