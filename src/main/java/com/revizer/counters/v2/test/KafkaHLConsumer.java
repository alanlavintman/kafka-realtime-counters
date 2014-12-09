package com.revizer.counters.v2.test;



import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaHLConsumer {
    private final ConsumerConnector connector;
    private final String topic;
    private  ExecutorService executor;

    public KafkaHLConsumer(String a_zookeeper, String a_groupId, String a_topic) {
        connector = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(a_zookeeper, a_groupId));
        this.topic = a_topic;
    }

    public void shutdown() {
        if (connector != null) connector.shutdown();
        if (executor != null) executor.shutdown();
    }

    public void run(int a_numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(a_numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = connector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        KafkaStream m_stream = streams.get(0);
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext())
            System.out.println(new String(it.next().message()));

//        // now launch all the threads
//        //
//        executor = Executors.newFixedThreadPool(a_numThreads);
//
//        // now create an object to consume the messages
//        //
//        int threadNumber = 0;
//        for (final KafkaStream stream : streams) {
//
//        }
    }

    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "10000");
        props.put("zookeeper.sync.time.ms", "2000");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        return new ConsumerConfig(props);
    }

    public static void main(String[] args) {
        String zooKeeper = "localhost:2181";
        String groupId = "something";
        String topic = "inject";
        int threads = 1;

        KafkaHLConsumer example = new KafkaHLConsumer(zooKeeper, groupId, topic);
        example.run(threads);

//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException ie) {
//
//        }
        //example.shutdown();
    }
}
