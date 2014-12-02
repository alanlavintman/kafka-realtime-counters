package com.revizer.counters.services.streaming.kafka;

import com.revizer.counters.services.metrics.MetricsService;
import com.revizer.counters.services.streaming.StreamingService;
import com.revizer.counters.utils.ConfigurationParser;
import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by alanl on 11/9/14.
 */
public class KafkaStreamingService extends StreamingService {

    private static Logger logger = LoggerFactory.getLogger(KafkaStreamingService.class);
    private ConsumerConfig consumerConfig = null;
    private Map<String,Integer> topicStreamMap = null;
    private KafkaJsonMessageDecoder messageDecoder = new KafkaJsonMessageDecoder();
    private List<KafkaStreamingHandler> handlers = new ArrayList<KafkaStreamingHandler>();
    private int parallelism = 0;
    private ExecutorService executor = null;

    public KafkaStreamingService(Configuration configuration, MetricsService metricsService) {
        super(configuration, metricsService);
        /* Get the configuration properties */
        consumerConfig = new ConsumerConfig(configure(configuration));
        topicStreamMap = createTopicStreamMap(configuration);
        messageDecoder.initialize(configuration, metricsService);
        printConfiguration();
    }

    private void printConfiguration() {
        logger.info("Streaming=> Starting the kafka streaming service with ");
        for (Map.Entry<String, Integer> stringIntegerEntry : topicStreamMap.entrySet()) {
            logger.info("Topic: {} and Number of streams: {}", stringIntegerEntry.getKey(), stringIntegerEntry.getValue());
        }
    }

    public Map<String, Integer> createTopicStreamMap(Configuration configuration) {
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

    @Override
    public void start() {

        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicStreamMap);
        executor = Executors.newFixedThreadPool(this.parallelism);

        // For each topic
        int threadNumber = 0;
        for (Map.Entry<String, List<KafkaStream<byte[], byte[]>>> topicStreamMap : messageStreams.entrySet()) {
            List<KafkaStream<byte[], byte[]>> streamList = topicStreamMap.getValue();
            for (KafkaStream<byte[], byte[]> stream : streamList) {
                String topicName = topicStreamMap.getKey();
                KafkaStreamingHandler kafkaStreamingHandler = new KafkaStreamingHandler(this.getConfiguration(), this.getMetricsService(), topicName, stream, threadNumber, this.messageDecoder, this.getListeners());
                handlers.add(kafkaStreamingHandler);
                executor.submit(kafkaStreamingHandler);
                threadNumber++;
            }
        }

    }

    @Override
    public void stop() {
        logger.info("Starting to stop the streaming service handlers.");
        for (KafkaStreamingHandler handler : handlers) {
            logger.info("Closing topic: {} and thread number: {}", handler.getTopic(), handler.getThreadNumber());
            handler.shutdown();
        }
        logger.info("Finished to stop the streaming service handlers.");
        logger.info("Starting to shutdown the executor service.");
        // Shut down the executor service instance.
        shutdownAndAwaitTermination(executor);
        logger.info("Finished Starting to shutdown the executor service.");
    }

    void shutdownAndAwaitTermination(ExecutorService pool) {
        // Disable new tasks from being submitted
        pool.shutdown();
        try {
            // Wait a while for existing tasks to terminate
            logger.info("Waiting for executor service to terminate with timeout of: {} seconds.", 60);
            if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                logger.info("forcing the shutdown.");
                pool.shutdownNow(); // Cancel currently executing tasks
                logger.info("Waiting for executor service to terminate after forced shutdown.");
                if (!pool.awaitTermination(60, TimeUnit.SECONDS)){
                    logger.error("Executor service pool could not terminate correctly.");
                }
             }
        } catch (InterruptedException ie) {
            logger.info("Recancelling the executor service pool.");
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            logger.info("Preserving the interrupt status.");
            Thread.currentThread().interrupt();
        }
    }


}
