package com.revizer.counters.v2;

import kafka.cluster.Broker;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.configuration.Configuration;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by alanl on 12/8/14.
 */
public class CounterContextConfiguration {

    private static Logger logger = LoggerFactory.getLogger(CounterContextConfiguration.class);

    public static CounterContext build(Configuration configuration){

        CounterContext context = new CounterContext(configuration);

        /* Build the topic list and discard the ones that are not configured in the kafka server */
        context = buildKafkaConfiguration(configuration, context);

        return null;

    }

    private static ZkClient buildZkClient(String zookeeperConnect){
        return new ZkClient(zookeeperConnect, 30000, 30000, new ZkSerializer() {
            @Override
            public byte[] serialize(Object o)
                    throws ZkMarshallingError
            {
                return ZKStringSerializer.serialize(o);
            }

            @Override
            public Object deserialize(byte[] bytes)
                    throws ZkMarshallingError
            {
                return ZKStringSerializer.deserialize(bytes);
            }
        });
    }

    private static CounterContext buildKafkaConfiguration(Configuration configuration, CounterContext context) {
        String zookeeperConnect = configuration.getString("streaming.kafka.zookeeper.connect");
        ZkClient zkClient = buildZkClient(zookeeperConnect);

        /* Build the broker information. */
        Seq<Broker> brokers = ZkUtils.getAllBrokersInCluster(zkClient);
        if (brokers.size() == 0){
            throw new RuntimeException("There are no kafka brokers under configuration parameter streaming.kafka.zookeeper.connect=" + zookeeperConnect);
        }

        Iterator<Broker> brokerIterator = brokers.iterator();
        while (brokerIterator.hasNext()){
            Broker broker = brokerIterator.next();
            logger.info("Registering Kafka Broker: {}", broker.getConnectionString());
            context.getBrokers().add(broker.getConnectionString());
        }

        /* Build the topics configuration. */
        String[] configuredTopics = configuration.getStringArray("streaming.kafka.topics");
        List<String> topicSkipList = new ArrayList<>();
        Seq<String> allKafkaTopics = ZkUtils.getAllTopics(zkClient);
        String[] allKafkaTopicsArray = new String[allKafkaTopics.size()];
        allKafkaTopics.copyToArray(allKafkaTopicsArray);
        for (String configuredTopic : configuredTopics) {
            String[] topicNameAndPartitions = configuredTopic.split(":");
            String topicName = topicNameAndPartitions[0];
            if (topicNameAndPartitions.length > 1){
                topicName = topicNameAndPartitions[1];
            }
            for (String kafkaTopic : allKafkaTopicsArray) {
                if (topicName.equals(kafkaTopic)){
                    // Get the amount of partitions.
                    context.getTopicAndPartition().add(kafkaTopic);
                }
            }
            topicSkipList.add(topicName);
        }

        if (context.getTopicAndPartition().size() == 0){
            throw new RuntimeException("There are no kafka topics that can match the configuration streaming.kafka.topics=" + StringUtils.join(configuredTopics,","));
        }

        for (String topicSkip : topicSkipList) {
            logger.info("Does not exists in kafka, skipping topic: {}", topicSkip);
        }

        return context;
    }
}
