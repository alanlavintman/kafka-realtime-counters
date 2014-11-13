package com.revizer.counters.services.counting.cleaner;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.revizer.counters.services.counting.exceptions.CounterRepositoryException;
import com.revizer.counters.services.counting.model.AggregationCounterKey;
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

    public void startupScript(){

    }

    @Override
    public void initialize(Configuration configuration) {
        cassandraNodes = configuration.getStringArray("counters.repository.cassandra.nodes");
        cassandraPorts = configuration.getInt("counters.repository.cassandra.port");
        reconnectionPolicy = configuration.getLong("counters.repository.cassandra.reconnection.policy");
        flushSize = configuration.getInt("counters.repository.cassandra.flush.size");
        /*
        Since Version of cassandra 2.1 use BEGIN UNLOGGED BATCH.
        BEGIN UNLOGGED BATCH
            INSERT INTO purchases (user, balance) VALUES ('user1', -8) IF NOT EXISTS;
            UPDATE UserActionCounts SET total = total + 1 WHERE keyalias = 523;
            UPDATE AdminActionCounts SET total = total + 1 WHERE keyalias = 701;
        APPLY BATCH;
        *
        * */
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

    @Override
    public void persist(String topic, Integer slotKeyInMinute, ConcurrentHashMap<AggregationCounterKey, AtomicLong> olderSlot) throws CounterRepositoryException {

        try{

            PreparedStatement preparedStatement = getSession().prepare("UPDATE time_series_minute SET value = value+? WHERE lookup_key=? and date=? and slot=?;");
            BatchStatement batch = new BatchStatement();
            for (Map.Entry<AggregationCounterKey, AtomicLong> aggregationCounterKeyAtomicLongEntry : olderSlot.entrySet()) {

                /* Get the key values and append it to the batch */
                AggregationCounterKey key = aggregationCounterKeyAtomicLongEntry.getKey();
                AtomicLong counter = aggregationCounterKeyAtomicLongEntry.getValue();
                batch.add(preparedStatement.bind(counter, key.getCounterKey(), key.getDate(), slotKeyInMinute));

                /* Check out if its time to commit. */
                if (batch.size() == this.flushSize){
                    /* Persist the information */
                    getSession().execute(batch);

                    /* Reset the Batch. */
                    batch = new BatchStatement();
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