CREATE KEYSPACE counters WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };

use counters;

CREATE TABLE time_series_minute (
    lookup_key text,
    date text,
    slot int,
    value counter,
PRIMARY KEY((lookup_key,date),slot),
) WITH CLUSTERING ORDER BY (slot DESC);

update counters.time_series_minute set value = value+1 where lookup_key='1010-2020' and date='2013-04-03' and slot=1000 USING TTL 1209600;

BEGIN UNLOGGED BATCH
  UPDATE time_series_minute SET value = value+1 WHERE lookup_key='1010-2020' and date='2013-04-03' and slot=1002 ;
APPLY BATCH;