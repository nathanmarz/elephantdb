[![Build Status](https://travis-ci.org/nathanmarz/elephantdb.png?branch=develop)](https://travis-ci.org/nathanmarz/elephantdb)

# ElephantDB 0.5.1 (cascalog 2.x)

## ElephantDB 0.4.5 (cascalog 1.x)

# About

ElephantDB is a database that specializes in exporting key/value data
from Hadoop. ElephantDB is composed of two components. The first is a
library that is used in MapReduce jobs for creating an indexed
key/value dataset that is stored on a distributed filesystem. The
second component is a daemon that can download a subset of a dataset
and serve it in a read-only, random-access fashion. A group of
machines working together to serve a full dataset is called a ring.

Since ElephantDB server doesn't support random writes, it is almost
laughingly simple. Once the server loads up its subset of the data, it
does very little. This leads to ElephantDB being rock-solid in
production, since there's almost no moving parts.

ElephantDB server has a Thrift interface, so any language can make
reads from it. The database itself is implemented in Clojure.

An ElephantDB datastore contains a fixed number of shards of a "Local
Persistence". ElephantDB's local persistence engine is pluggable, and
ElephantDB comes bundled with local persistence implementations for
Berkeley DB Java Edition and LevelDB. On the MapReduce side, each
reducer creates or updates a single shard into the DFS, and on the
server side, each server serves a subset of the shards.

ElephantDB support hot-swapping so that a live server can be updated
with a new set of shards without downtime.

# Questions

Google group: [elephantdb-user](http://groups.google.com/group/elephantdb-user)

# Introduction

[Introduction to ElephantDB](https://speakerdeck.com/sorenmacbeth/introduction-to-elephantdb)

# Tutorials

TODO: Write an updated tutorial for ElephantDB 0.4.x

# Using ElephantDB in MapReduce Jobs

ElephantDB is hosted at [Clojars](http://clojars.org/elephantdb).
Clojars is a maven repo that is trivially easy to use with maven or
leiningen. You should use this dependency when using ElephantDB within
your MapReduce jobs to create ElephantDB datastores. ElephantDB
contains a module elephantdb-cascading which allows you to easily create
datastores from your Cascading workflows. elephantdb-cascalog is available
for use with [Cascalog](http://github.com/nathanmarz/cascalog) >= 1.10.1.

# Deploying ElephantDB server

TODO: Documentation on how to deploy ElephantDB.

# Running the EDB Jar

TODO: Documentation on how to run ElephantDB
