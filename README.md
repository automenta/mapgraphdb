
![base](https://pbs.twimg.com/profile_images/2992585317/75aaa8e1895eb1ae54acfeb31cd47dea_400x400.png)
![base](https://raw.githubusercontent.com/tinkerpop/blueprints/master/doc/images/blueprints-logo.png)

MapDB Database Implementation for Tinkerpop Blueprints
======================================================

Blueprints
==========
https://github.com/tinkerpop/blueprints
http://tinkerpop.com

Blueprints is a property graph model interface. It provides implementations, test suites, and supporting extensions. Graph databases and frameworks that implement the Blueprints interfaces automatically support Blueprints-enabled applications. Likewise, Blueprints-enabled applications can plug-and-play different Blueprints-enabled graph backends.

MapDB
=====
http://mapdb.org
https://github.com/jankotek/MapDB

Concurrent - MapDB has record level locking and state-of-art concurrent engine. Its performance scales nearly linearly with number of cores. Data can be written by multiple parallel threads.

Fast - MapDB has outstanding performance rivaled only by native DBs. It is result of more than a decade of optimizations and rewrites.

ACID - MapDB optionally supports ACID transactions with full MVCC isolation. MapDB uses write-ahead-log or append-only store for great write durability.

Flexible - MapDB can be used everywhere from in-memory cache to multi-terabyte database. It also has number of options to trade durability for write performance. This makes it very easy to configure MapDB to exactly fit your needs.

Hackable - MapDB is component based, most features (instance cache, async writes, compression) are just class wrappers. It is very easy to introduce new functionality or component into MapDB.

SQL Like - MapDB was developed as faster alternative to SQL engine. It has number of features which makes transition from relational database easier: secondary indexes/collections, autoincremental sequential ID, joins, triggers, composite keys…

Low disk-space usage - MapDB has number of features (serialization, delta key packing…) to minimize disk used by its store. It also has very fast compression and custom serializers. We take disk-usage seriously and do not waste single byte.
