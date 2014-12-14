Kafka/Cassandra Real Time Counters
==================================

This is a simple project that will allow you to easily set up a counting system having:

- Streaming layer: Currently a super concrete implementation to kafka that can not be abstracted (If time will allow me, it will be :P)
- Counting Layer: Currently a super concrete implementation to cassandra.

Cassandra Data Schema
=====================

Before using it, please execute the script that resides in /src/main/resources/schema.cql

```
CREATE KEYSPACE counters WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };

use counters;

CREATE TABLE time_series_minute (
    lookup_key text,
    date text,
    slot int,
    value counter,
PRIMARY KEY((lookup_key,date),slot),
) WITH CLUSTERING ORDER BY (slot DESC);

```

It creates a table in the following shape:

```

 lookup_key | date | slot | value
 ------------+------+------+-------

 ```

This schema will guarantee that the data will reside in one node for the lookup_key and the date specified and that you will be
able to create range queries against the slot column. All the date for a specific lookup_key will be stored in the same node.

Get the source and Build it
===========================

Requirements:

- Git: Dont know what git is? Visit http://git-scm.com/
- Maven: Dont know what maven is? Visit http://maven.apache.org/

```
$ git clone https://github.com/alanlavintman/rt-counters.git
$ mvn package
```

This will create an Uber jar that will be able to execute with a classic java jar command.
You may need to pass the property file and the logger file as a command line argument.





