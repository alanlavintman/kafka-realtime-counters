//package com.revizer.counters;
//
//import com.revizer.counters.embedded.kafka.EmbeddedKafkaClusterHelper;
//import com.revizer.counters.services.ServiceFactory;
//import com.revizer.counters.services.counting.cleaner.CassandraCounterRepository;
//import com.revizer.counters.services.counting.exceptions.CounterRepositoryException;
//import com.revizer.counters.services.counting.model.AggregationCounterKey;
//import com.revizer.counters.services.metrics.MetricsService;
//import kafka.javaapi.producer.Producer;
//import kafka.producer.KeyedMessage;
//import kafka.producer.ProducerConfig;
//import org.apache.cassandra.exceptions.ConfigurationException;
//import org.apache.commons.configuration.BaseConfiguration;
//import org.apache.commons.configuration.Configuration;
//import org.apache.thrift.transport.TTransportException;
//import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
//import org.junit.After;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.io.IOException;
//import java.util.Properties;
//import java.util.Random;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.atomic.AtomicLong;
//
///**
// * Created by alanl on 11/13/14.
// */
//public class SystemTest extends BaseCounterTest {
//
//    @Before
//    public void beforeTest(){
//
//        try {
//            //1)  Start Embedded Kafka
//            EmbeddedKafkaClusterHelper.startEmbeddedKafkaCluster();
//
//            //2) Start Embedded Cassandra.
//            EmbeddedCassandraServerHelper.startEmbeddedCassandra();
//
//            ProducerConfig config = new ProducerConfig(createProducerProperties());
//            Producer<String, String> producer = new Producer<String, String>(config);
//
//            //3) Create a new topic according to the configuration but with no replication.
//            Configuration configuration = prepareConfiguration();
//            String[] topics = configuration.getStringArray("streaming.kafka.topics");
//            for (String topic : topics) {
//                String[] topicInfo = topic.split(":");
//                int paralellism = Integer.valueOf(topicInfo[1]);
////                this.createTopic(topicInfo[0],paralellism,1, new Properties());
//                producer.send(new KeyedMessage<String, String>("inject","ASDFASDFASDF"));
//                int a =123;
//            }
//
//        } catch (TTransportException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        } catch (ConfigurationException e) {
//            e.printStackTrace();
//        }
//    }
//
//    @After
//    public void afterTest(){
//        //1)  Start Embedded Kafka
//        EmbeddedKafkaClusterHelper.shutDownEmbeddedKafkaCluster();
//
//
//        //2) Start Embedded Cassandra.
//        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
//    }
//
//    @Test
//    public void SimpleTest(){
//
//        // Build a countingSystem and start it, for that we need a configuration, metrics and a service factory.
//        Configuration configuration = prepareConfiguration();
//        MetricsService metrics = new MetricsService(configuration);
//        ServiceFactory serviceFactory = new ServiceFactory(configuration, metrics);
//        CountingSystem system = new CountingSystem(serviceFactory);
//        system.start();
//
//        ProducerConfig config = new ProducerConfig(createProducerProperties());
//        Producer<String, String> producer = new Producer<String, String>(config);
//
//
//        /* Create 10 events each minute, */
//        long now = System.currentTimeMillis()/1000;
//        String topic = configuration.getString("streaming.kafka.topics").split(":")[0];
//        String affid = "1234";
//        String subaffid = "5678";
//        String country = "US";
//        String browser = "ie";
//        String[] revmods = {"a","b"};
//        for (int i=0;i<10;i++){
//            now = ((now/60)+i*60);
//            String event = this.createEvent(now, affid, subaffid, country, browser,revmods);
//            producer.send(new KeyedMessage<String, String>(topic, event));
//        }
//        // Wait 1 second and shut down the system.
//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//        // Shut down the system and collect statistics.
//        system.stop();
//
//        // Collect the counters and see that everything is in cassandra.
//        CassandraCounterRepository repository = new CassandraCounterRepository();
//        repository.initialize(configuration);
//        repository.initializeSchema(); // creates the keyspace and the table.
//
//
//    }
//
//    private Properties createProducerProperties(){
//        Properties props = new Properties();
//        props.put("metadata.broker.list", "localhost:9092");
//        props.put("serializer.class", "kafka.serializer.StringEncoder");
////        props.put("request.required.acks", "1");
//        return props;
//    }
//
//
//    private Configuration prepareConfiguration() {
//        Configuration configuration = new BaseConfiguration();
//        configuration.addProperty("counters.repository.cassandra.nodes","127.0.0.1");
//        configuration.addProperty("counters.repository.cassandra.port",9142);
//        configuration.addProperty("counters.repository.cassandra.reconnection.policy",1000);
//        configuration.addProperty("counters.repository.cassandra.flush.size",10);
//
//        configuration.addProperty("streaming.kafka.zookeeper.connect","localhost:2181");
//        configuration.addProperty("streaming.kafka.group.id","counters");
//        configuration.addProperty("streaming.kafka.zookeeper.session.timeout.ms","1500");
//        configuration.addProperty("streaming.kafka.zookeeper.sync.time.ms","100");
//        configuration.addProperty("streaming.kafka.auto.commit.interval.ms","100");
//
//
//        configuration.addProperty("streaming.kafka.topics","inject:1");
//        configuration.addProperty("counters.topic.timefield.inject","now");
//        configuration.addProperty("counters.counter.inject.aff.subaff.country","affid,subaffid,country_code");
//        configuration.addProperty("counters.counter.inject.aff.subaff.browser","affid,subaffid,browser");
//        configuration.addProperty("counters.counter.inject.country","country_code");
////        configuration.addProperty("counters.counter.inject.revmod","revmods");
//
//        configuration.addProperty("counter.cleaner.recicle.period.ms",1000);
//
//        return configuration;
//    }
//
//}
