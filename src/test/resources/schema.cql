CREATE KEYSPACE counters WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };

CREATE TABLE counters.time_series_minute (
    lookup_key text,
    date text,
    slot int,
    value counter,
PRIMARY KEY((lookup_key,date),slot),
) WITH CLUSTERING ORDER BY (slot DESC);