package com.revizer.counters.v2.flusher;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.revizer.counters.v2.counters.metadata.AggregationCounterKey;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by alanl on 11/13/14.
 */
public class CassandraCounterRepository implements CounterRepository {

    private static Logger logger = LoggerFactory.getLogger(CassandraCounterRepository.class);
    private String[] cassandraNodes;
    private long reconnectionPolicy;
    private Cluster cluster;
    private Session session;
    private int flushSize;
    private int cassandraPorts;

    public Cluster getCluster() {
        return cluster;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public String[] getCassandraNodes() {
        return cassandraNodes;
    }

    public void setCassandraNodes(String[] cassandraNodes) {
        this.cassandraNodes = cassandraNodes;
    }

    public long getReconnectionPolicy() {
        return reconnectionPolicy;
    }

    public void setReconnectionPolicy(long reconnectionPolicy) {
        this.reconnectionPolicy = reconnectionPolicy;
    }

    public int getCassandraPorts() {
        return cassandraPorts;
    }

    public void setCassandraPorts(int cassandraPorts) {
        this.cassandraPorts = cassandraPorts;
    }

    public void initializeSchema(){

        this.getSession().execute("CREATE KEYSPACE counters WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };");

        this.getSession().execute("CREATE TABLE counters.time_series_minute (\n" +
                "    lookup_key text,\n" +
                "    date text,\n" +
                "    slot int,\n" +
                "    value counter,\n" +
                "PRIMARY KEY((lookup_key,date),slot),\n" +
                ") WITH CLUSTERING ORDER BY (slot DESC);");

    }

    @Override
    public void initialize(Configuration configuration) {
        cassandraNodes = configuration.getStringArray("counters.repository.cassandra.nodes");
        cassandraPorts = configuration.getInt("counters.repository.cassandra.port");
        reconnectionPolicy = configuration.getLong("counters.repository.cassandra.reconnection.policy");
        flushSize = configuration.getInt("counters.repository.cassandra.flush.size");
        this.cluster = Cluster.builder()
                .addContactPoints(this.cassandraNodes)
                .withPort(this.cassandraPorts)
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(this.reconnectionPolicy))
                .withCompression(ProtocolOptions.Compression.SNAPPY)
                .withLoadBalancingPolicy(new RoundRobinPolicy())
                .build();
        this.session = cluster.connect();
        Metadata metadata = cluster.getMetadata();
        logger.info("Connected to cluster: {} ", metadata.getClusterName());
        for ( Host host : metadata.getAllHosts() ) {
            logger.info("Datacenter: {}; Host: {}; Rack: {} ", host.getDatacenter(), host.getAddress(), host.getRack());
        }
    }

    public long count(){
        Row row =  getSession().execute("SELECT COUNT(*) FROM counters.time_series_minute;").one();
        return row.getLong("count");
    }

    @Override
    public void persist(String topic, Integer slotKeyInMinute, ConcurrentHashMap<AggregationCounterKey, AtomicLong> olderSlot) throws CounterRepositoryException {

        try{

            PreparedStatement preparedStatement = getSession().prepare("UPDATE counters.time_series_minute SET value = value+? WHERE lookup_key=? and date=? and slot=?;");
            BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
            for (Map.Entry<AggregationCounterKey, AtomicLong> aggregationCounterKeyAtomicLongEntry : olderSlot.entrySet()) {

                /* Get the key values and append it to the batch */
                AggregationCounterKey key = aggregationCounterKeyAtomicLongEntry.getKey();
                AtomicLong counter = aggregationCounterKeyAtomicLongEntry.getValue();
                batch.add(preparedStatement.bind(counter.get(), key.getCounterKey(), key.getDate(), slotKeyInMinute));

                /* Check out if its time to commit. */
                if (batch.size() == this.flushSize){
                    /* Persist the information */
                    getSession().execute(batch);

                    /* Reset the Batch. */
                    batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
                }

            }

            // Clean the reminders:
            if (batch.size() > 0){
                getSession().execute(batch);
            }

        } catch (Exception ex){
            throw new CounterRepositoryException(ex);
        }

    }

}