package com.revizer.counters;

import com.revizer.counters.services.counting.cleaner.CassandraCounterRepository;
import com.revizer.counters.services.counting.exceptions.CounterRepositoryException;
import com.revizer.counters.services.counting.model.AggregationCounterKey;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by alanl on 11/13/14.
 */
public class CassandraCounterRepositoryTest {

    @Test
    public void testInitialScript(){

        try {

            EmbeddedCassandraServerHelper.startEmbeddedCassandra();

            Configuration configuration = prepareConfiguration();

            CassandraCounterRepository repository = new CassandraCounterRepository();
            repository.initialize(configuration);
            repository.initializeSchema(); // creates the keyspace and the table.

            String topic = "inject";
            Integer minuteSlot = 1;
            ConcurrentHashMap<AggregationCounterKey, AtomicLong> timeseries = new ConcurrentHashMap<>();
            String date = "20141112";
            int maxAff = 10;
            int maxSubAff = 10;

            for (int affid=0;affid<10;affid++){
                for (int subaffid=0;subaffid<10;subaffid++){
                    int rnd = new Random().nextInt();
                    String counterKey = String.valueOf(affid).concat(String.valueOf(subaffid));
                    AggregationCounterKey key = new AggregationCounterKey(counterKey, date);
                    timeseries.put(key,new AtomicLong(rnd));
                }
            }

            repository.persist(topic, minuteSlot, timeseries);

            long count = repository.count();

            // We test that the amount of rows are the same.
            Assert.assertEquals(count,maxAff*maxSubAff);

        } catch (CounterRepositoryException e){
            Assert.fail(e.toString());
        } catch (TTransportException e) {
            Assert.fail(e.toString());
        } catch (IOException e) {
            Assert.fail(e.toString());
        } catch (InterruptedException e) {
            Assert.fail(e.toString());
        } catch (ConfigurationException e) {
            Assert.fail(e.toString());
        } finally {
            EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        }

    }

    private Configuration prepareConfiguration() {
        Configuration configuration = new BaseConfiguration();
        configuration.addProperty("counters.repository.cassandra.nodes","127.0.0.1");
        configuration.addProperty("counters.repository.cassandra.port",9142);
        configuration.addProperty("counters.repository.cassandra.reconnection.policy",1000);
        configuration.addProperty("counters.repository.cassandra.flush.size",10);

        configuration.addProperty("counters.repository.cassandra.flush.size",10);
        configuration.addProperty("counters.repository.cassandra.flush.size",10);
        configuration.addProperty("counters.repository.cassandra.flush.size",10);
        return configuration;
    }

}
