package com.revizer.counters.v2;

import com.revizer.counters.services.streaming.kafka.KafkaStreamingHandler;
import com.revizer.counters.utils.ConfigurationParser;
import com.revizer.counters.v2.streaming.KafkaJsonMessageDecoder;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.commons.configuration.Configuration;

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
    private ExecutorService executor = null;
    private ConsumerConnector consumer;
    private KafkaJsonMessageDecoder decoder;
    private int parallelism;

    public CountingSystem(CounterContext context) {
        this.context = context;
        this.consumerConfig = new ConsumerConfig(configure(this.context.getConfiguration()));
        this.topicStreamMap = createTopicStreamMapAndChangeParalellism(this.context.getConfiguration());
        this.decoder = new KafkaJsonMessageDecoder();
    }

    public void start(){
        this.consumer = Consumer.createJavaConsumerConnector(consumerConfig);
        this.executor = Executors.newFixedThreadPool(this.parallelism);
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicStreamMap);
        int threadNumber = 0;
        for (String topic : messageStreams.keySet()) {
            List<KafkaStream<byte[], byte[]>> streamList = messageStreams.get(topic);
            for (KafkaStream<byte[], byte[]> stream : streamList) {
                executor.submit(new KafkaStreamingHandler(context, topic, stream, threadNumber, this.decoder));
                threadNumber++;
            }
        }
    }

    public void stop(){
        this.consumer.shutdown();
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
