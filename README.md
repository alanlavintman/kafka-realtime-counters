Kafka/Cassandra Real Time Counters
==================================

This is a simple project that will allow you to easily set up a counting system having:

- Streaming layer: Currently a super concrete implementation to kafka that can not be abstracted (If time will allow me, it will be :P)
- Counting Layer: Currently a super concrete implementation to cassandra.
- Only Json Support !

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

Property list:

```

# All the kafka consumer properties should start with the "streaming.kafka" prefix, they will all be passed to the consumer.
streaming.kafka.zookeeper.connect=localhost:2181
streaming.kafka.group.id=counters3
streaming.kafka.zookeeper.session.timeout.ms=10000
streaming.kafka.zookeeper.sync.time.ms=2000
streaming.kafka.auto.commit.interval.ms=1000
streaming.kafka.rebalance.max.retries=30
streaming.kafka.auto.offset.reset=largest
#streaming.kafka.consumer.timeout.ms=1000

# The topics we would like to listen to and the parallelism for each topic.
# In this case we will open 3 threads to read from each partition of each topic. 2 for inject and one for addon topic.
streaming.kafka.topics=inject:2,addon

# the field that will be used in order to understand the time of the event. (Currently only in seconds.
counters.topic.timefield.inject=now
counters.topic.timefield.addon=now

# Count total aggregations. Tell whether to count the total per topic or not.
counters.counter.total.inject=true
counters.counter.total.addon=true

#Here we set up all the counter definitions. IT should start with the prefix "counters.counter"+topicname+countername.
# In the following case our countername is "aff.subaff" and the fields that we will consider to count are affid and subaffid.
counters.counter.inject.aff.subaff=affid,subaffid
counters.counter.inject.country=country_code


# The flush policy to use in order to flush the events to the counters store.
counter.cleaner.recicle.period.ms=1000
# Amount of times that a flush will try to retry if it fails before it drops the data.
counter.cleaner.recicle.retry.count=5


# Configuration of the cassandra repository
counters.repository.cassandra.nodes=127.0.0.1
counters.repository.cassandra.port=9142
counters.repository.cassandra.reconnection.policy=100
counters.repository.cassandra.flush.size=10
```

Looking forward:
- Abstract the streaming layer.
- Abstract the counting layer.
- Being able to define different slot period types. Such as counters per hour/day,etc. Currently its all on a minute based.
- Being able to define the time type of the event, currently you can define the field that holds the time, but its only epoch (seconds).
- Being able to define different types of data format streams and not only json.