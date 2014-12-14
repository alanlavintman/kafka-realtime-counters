package com.revizer.counters.v1;

import com.google.common.base.Preconditions;
import com.revizer.counters.utils.ConfigurationParser;
import com.revizer.counters.v1.counters.metadata.AggregationCounter;
import com.revizer.counters.v1.counters.metadata.TopicAggregationsMetadata;
import com.revizer.counters.v1.metrics.MetricsService;
import com.revizer.counters.v1.streaming.decoder.KafkaJsonMessageDecoder;
import com.revizer.counters.v1.streaming.listeners.CounterKafkaListener;
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

        ConfigurationParser.printLine(logger);
        logger.info("Starting the counting system configuration");
        ConfigurationParser.printLine(logger);
        /* Build the metrics service and append it to the counter context */
        MetricsService metricsService = new MetricsService(configuration);
        CounterContext context = new CounterContext(configuration, metricsService);

        /* Build the topic list and discard the ones that are not configured in the kafka server. */
        context = buildKafkaConfiguration(configuration, context);

        /* Build the aggregations object graph, filter the ones that do not belong to the topics that do not exists. */
        context = buildAggregationsGraph(configuration, context);

        /* Configure listeners */
        context = buildListenerHandlers(configuration, context);

        /* Configure decoders */
        context = buildMessageDecoder(configuration, context);

        ConfigurationParser.printLine(logger);
        logger.info("Counting system configuration finished successfully");
        ConfigurationParser.printLine(logger);

        return context;

    }

    private static CounterContext buildMessageDecoder(Configuration configuration, CounterContext context) {
        logger.info("   Starting to build the Message decoder aggregations configuration");
        context.setDecoder(new KafkaJsonMessageDecoder());
        logger.info("   Message decoder configuration finished successfully.");
        return context;
    }

    private static CounterContext buildListenerHandlers(Configuration configuration, CounterContext context) {
        logger.info("   Starting to build the message listeners");
        context.getListeners().add(new CounterKafkaListener("counter-listener",context));
        logger.info("       CounterKafkaListener class registered");
        logger.info("   Message listeners configuration finished successfully");
        return context;
    }

    private static CounterContext buildAggregationsGraph(Configuration configuration, CounterContext context) {
        logger.info("   Starting to build the aggregations configuration");
        TopicAggregationsMetadata topicAggregationsMetadata = new TopicAggregationsMetadata();
        Preconditions.checkArgument(context.getTopicAndPartition() != null, "There are no kafka topics that can match the configuration streaming.kafka.topics");
        Preconditions.checkArgument(context.getTopicAndPartition().size() > 0, "There are no kafka topics that can match the configuration streaming.kafka.topics");

        List<String> topicAndPartitions = context.getTopicAndPartition();
        for (String topicAndPartition : topicAndPartitions) {

            String[] topicNameArray = topicAndPartition.split(":");
            String topicName = topicNameArray[0];
            logger.info("       Starting to configure aggregations for topic {}.", topicName);


            /* Set up the topic time field and the total counter if needed */
            String timeField = configuration.getString("counters.topic.timefield." + topicName, "now");
            Boolean countTotal = configuration.getBoolean("counters.counter.total." + topicName, false);
            topicAggregationsMetadata.addNowField(topicName,timeField);
            if (countTotal == true){
                topicAggregationsMetadata.addTopicCountersTotal(topicName);
            }

            /* Create a counter slot for the topic so we will be able to hold aggregations */
            topicAggregationsMetadata.addTopicCounterSlotHolder(topicName);

            /* Set up all the counters registered for the topic */
            String counterKey = "counters.counter." + topicName;
            List<String> aggregationKeys = ConfigurationParser.getKeysThatStartsWith(configuration, counterKey);
            List<AggregationCounter> aggregationCounters = new ArrayList<AggregationCounter>();
            for (String aggregationKey : aggregationKeys) {
                logger.info("           Registering aggregations {}.", aggregationKey);
                String aggregationName = aggregationKey.substring(counterKey.length()+1);
                String[] aggregationValue = configuration.getStringArray(aggregationKey);
                AggregationCounter aggregation = new AggregationCounter(topicName, aggregationName, aggregationValue);
                aggregationCounters.add(aggregation);
            }
            topicAggregationsMetadata.getTopicAggregations().put(topicName, aggregationCounters);

        }
        context.setTopicAggregationsMetadata(topicAggregationsMetadata);
        logger.info("   Aggregations configuration finished successfully.");
        return context;
    }


    /**
     * Method in charge of decorating a CounterContext
     * @param configuration
     * @param context
     * @return
     */
    private static CounterContext buildKafkaConfiguration(Configuration configuration, CounterContext context) {
        logger.info("   Starting to build the kafka configuration");
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
            logger.info("       Registering Kafka Broker: {}", broker.getConnectionString());
            context.getBrokers().add(broker.getConnectionString());
        }

        /* Build the topics configuration. */
        String[] configuredTopics = configuration.getStringArray("streaming.kafka.topics");
        Seq<String> allKafkaTopics = ZkUtils.getAllTopics(zkClient);
        String[] allKafkaTopicsArray = new String[allKafkaTopics.size()];
        allKafkaTopics.copyToArray(allKafkaTopicsArray);
        for (String configuredTopic : configuredTopics) {
            String[] topicNameAndPartitions = configuredTopic.split(":");
            String topicName = topicNameAndPartitions[0];
            boolean found = false;
            for (String kafkaTopic : allKafkaTopicsArray) {
                if (topicName.equals(kafkaTopic)){
                    // Get the amount of partitions.
                    context.getTopicAndPartition().add(kafkaTopic);
                    found = true;
                    logger.info("       Registering topic {}.", topicName);
                }
            }
            if (!found){
                logger.info("       Skipping topic {} because it was not found in kafka.", topicName);
            }
        }

        if (context.getTopicAndPartition().size() == 0){
            throw new RuntimeException("There are no kafka topics that can match the configuration streaming.kafka.topics=" + StringUtils.join(configuredTopics,","));
        }

        logger.info("   Kafka Configuration has finished successfully.");

        return context;
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
}
