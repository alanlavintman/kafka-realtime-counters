package com.revizer.counters;

import com.revizer.counters.utils.ConfigurationParser;
import com.revizer.counters.v1.CounterContext;
import com.revizer.counters.v1.counters.CounterSlotHolder;
import com.revizer.counters.v1.flusher.CounterSlotHolderCleaner;
import com.revizer.counters.v1.streaming.KafkaConsumerHandler;
import com.revizer.counters.v1.streaming.decoder.KafkaJsonMessageDecoder;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by alanl on 12/8/14.
 */
public class CountingSystem {

    private CounterContext context;
    private ConsumerConfig consumerConfig;
    private Map<String, Integer> topicStreamMap;
    private ExecutorService countersExecutor = null;
    private ExecutorService cleanerExecutor = null;
    private ConsumerConnector consumer;
    private KafkaJsonMessageDecoder decoder;
    private int parallelism;
    private static Logger logger = LoggerFactory.getLogger(CountingSystem.class);

    public CountingSystem(CounterContext context) {
        this.context = context;
        this.consumerConfig = new ConsumerConfig(configure(this.context.getConfiguration()));
        this.topicStreamMap = createTopicStreamMapAndChangeParalellism(this.context.getConfiguration());
        this.decoder = new KafkaJsonMessageDecoder();
    }

    public void start(){

        ConfigurationParser.printLine(logger);
        logger.info("   Starting the Counting System.");
        ConfigurationParser.printLine(logger);

        /* Start the jmx metrics*/
        this.context.getMetricsService().start();

        /* Start the cleaner process */
        startCleanerThreads();

        /* Start the streamer process */
        startStreamingThreads();

        logger.info("   Counting System started successfully.");
        ConfigurationParser.printLine(logger);
    }

    private void startCleanerThreads() {
        logger.info("       Starting the counters cleaner handlers.");
        /* Holds the topics to a counter holder instance. */
        Map<String, CounterSlotHolder> countersSlotHolderPerTopic = context.getTopicAggregationsMetadata().getCountersSlotHolderPerTopic();

        this.cleanerExecutor = Executors.newFixedThreadPool(countersSlotHolderPerTopic.size());

        /* for each topic and counter holder, create a new thread. */
        for (Map.Entry<String, CounterSlotHolder> stringCounterSlotHolderEntry : countersSlotHolderPerTopic.entrySet()) {
            String topic = stringCounterSlotHolderEntry.getKey();
            CounterSlotHolderCleaner cleaner = new CounterSlotHolderCleaner(topic, stringCounterSlotHolderEntry.getValue(), this.context);
            this.cleanerExecutor.submit(cleaner);
            logger.info("           Counter cleaner registered for topic {}.",topic);
        }

        logger.info("       Counter cleaner handlers registered successfully.");
    }

    private void startStreamingThreads() {
        logger.info("       Starting the Consumer Handlers.");
        /* Fire all the threads according to the paralellism configured. */
        this.consumer = Consumer.createJavaConsumerConnector(consumerConfig);
        this.countersExecutor = Executors.newFixedThreadPool(this.parallelism);
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicStreamMap);
        int threadNumber = 0;
        for (String topic : messageStreams.keySet()) {
            List<KafkaStream<byte[], byte[]>> streamList = messageStreams.get(topic);
            for (KafkaStream<byte[], byte[]> stream : streamList) {
                logger.info("       Registering handler for topic {} and thread number {}", topic, threadNumber);
                countersExecutor.submit(new KafkaConsumerHandler(this.context, topic, stream, threadNumber));
                threadNumber++;
            }
        }
        logger.info("       Consumer Handlers started successfully with a paralellism of {}.", this.parallelism);
        ConfigurationParser.printLine(logger);
    }

    public void stop(){
        ConfigurationParser.printLine(logger);
        logger.info("Starting to shut down the counting system");
        ConfigurationParser.printLine(logger);
        try{
            this.context.getMetricsService().stop();
        } catch (Exception ex){
            logger.error("There was an error while trying to stop the metrics service", ex);
        }
        logger.info("Starting to shut down the kafka consumer connection.");
        try{
            this.consumer.shutdown();
            logger.info("Kafka consumer connection Shut down successfully");
        } catch (Exception ex){
            logger.error("There was an error while trying to stop the kafka consumer connection", ex);
        }
        ConfigurationParser.printLine(logger);
        logger.info("The Counting System has been shut down");
        ConfigurationParser.printLine(logger);
    }

    public Map<String, Integer> createTopicStreamMapAndChangeParalellism(Configuration configuration) {
        Map<String, Integer> topicAndNumOfStreams = ConfigurationParser.getTopicAndNumOfStreams(configuration);
        for (Integer numOfStreams : topicAndNumOfStreams.values()) {
            this.parallelism += numOfStreams;
        }
        return topicAndNumOfStreams;
    }

    /**
     * Method in charge of building a {@link java.util.Properties} instance from a {@link org.apache.commons.configuration.Configuration} one.
     * @param configuration The configuration instance passed by parameter.
     * @return A {@link java.util.Properties} instance empty or full of results depending on the configuration passed by parameter.
     */
    public Properties configure(Configuration configuration){
        Properties props = new Properties();
        Iterator<String> keys = configuration.getKeys();
        while(keys.hasNext()){
            String key = keys.next();
            String kafkaKey = "streaming.kafka.";
            if (key.startsWith(kafkaKey)){
                String value = configuration.getString(key);
                String propsNewKey  = key.substring(kafkaKey.length());
                props.put(propsNewKey, value);
            }
        }
        return props;
    }
}
