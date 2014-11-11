package com.revizer.counters.services.streaming.kafka;

import com.codahale.metrics.Meter;
import com.revizer.counters.services.metrics.MetricsService;
import com.revizer.counters.services.streaming.exceptions.MessageDecoderException;
import com.revizer.counters.services.streaming.StreamServiceListener;
import com.revizer.counters.services.streaming.exceptions.StreamServiceListenerException;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.commons.configuration.Configuration;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by alanl on 11/10/14.
 */
public class KafkaStreamingHandler<T> implements Runnable {

    private Configuration configuration;
    private MetricsService metricsService;
    private Meter rps;
    private KafkaStream stream;
    private int threadNumber;
    private String topic;
    private KafkaJsonMessageDecoder decoder;
    private List<StreamServiceListener> listeners;
    private static Logger logger = LoggerFactory.getLogger(KafkaStreamingHandler.class);

    public KafkaStreamingHandler(Configuration configuration, MetricsService metricsService, String topic, KafkaStream stream, int threadNumber, KafkaJsonMessageDecoder decoder, List<StreamServiceListener> listeners) {
        this.topic = topic;
        this.threadNumber = threadNumber;
        this.stream = stream;
        this.decoder = decoder;
        this.listeners = listeners;
        this.rps = metricsService.createMeter(KafkaStreamingHandler.class,topic + "-rps");
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> consumerIterator = stream.iterator();
        while (consumerIterator.hasNext()){
            JsonNode event = null;
            try {
                this.rps.mark();
                event = decoder.decode(consumerIterator.next().message());
                for (StreamServiceListener listener : listeners) {
                    try {
                        listener.process(topic, event);
                    } catch (StreamServiceListenerException e) {
                        logger.error("There was an error while trying to process the stream service listener: {}", listener.getName(), e);
                    }
                }
            } catch (MessageDecoderException e) {
                logger.error("There was an error while trying to decode the message: {}", new String(consumerIterator.next().message()), e);
            }
        }
    }
}