package com.revizer.counters;

import com.revizer.counters.services.counting.cleaner.CassandraCounterRepository;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by alanl on 11/13/14.
 */
public class CassandraCounterRepositoryTest {

    @Test
    public void testInitialScript(){
//        System.setProperty("cassandra.config", "file:../../test/conf/cassandra.yaml");
//        System.setProperty("log4j.configuration", "file:../../test/conf/log4j-junit.properties");

        try {
            EmbeddedCassandraServerHelper.startEmbeddedCassandra();

            Configuration configuration = prepareConfiguration();

            CassandraCounterRepository repository = new CassandraCounterRepository();
            repository.initialize(configuration);


        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }

    }

    private Configuration prepareConfiguration() {
        Configuration configuration = new BaseConfiguration();
        configuration.addProperty("counters.repository.cassandra.nodes","127.0.0.1");
        configuration.addProperty("counters.repository.cassandra.port",9142);
        configuration.addProperty("counters.repository.cassandra.reconnection.policy",100);
        configuration.addProperty("counters.repository.cassandra.flush.size",10);
        return configuration;
    }

}
