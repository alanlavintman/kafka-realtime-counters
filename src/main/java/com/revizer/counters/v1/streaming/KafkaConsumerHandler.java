package com.revizer.counters.v1.streaming;

import com.codahale.metrics.Meter;
import com.revizer.counters.services.metrics.MetricsService;
import com.revizer.counters.services.streaming.StreamServiceListener;
import com.revizer.counters.services.streaming.exceptions.MessageDecoderException;
import com.revizer.counters.services.streaming.exceptions.StreamServiceListenerException;
import com.revizer.counters.v1.CounterContext;
import com.revizer.counters.v1.streaming.listeners.KafkaStreamListener;
import com.revizer.counters.v1.streaming.listeners.KafkaStreamListenerException;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.commons.configuration.Configuration;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by alanl on 11/10/14.
 */
public class KafkaConsumerHandler implements Runnable {

    private Configuration configuration;
    private KafkaStream<byte[], byte[]> stream;
    private int threadNumber;
    private String topic;
    private CounterContext context;
    private Meter consumedMessage;
    private Meter failedDecodedMessage;

    private static Logger logger = LoggerFactory.getLogger(KafkaConsumerHandler.class);

    public CounterContext getContext() {
        return context;
    }

    public void setContext(CounterContext context) {
        this.context = context;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public KafkaStream getStream() {
        return stream;
    }

    public void setStream(KafkaStream stream) {
        this.stream = stream;
    }

    public int getThreadNumber() {
        return threadNumber;
    }

    public void setThreadNumber(int threadNumber) {
        this.threadNumber = threadNumber;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public KafkaConsumerHandler(CounterContext context, String topic, KafkaStream<byte[], byte[]> stream, int threadNumber) {
        this.context = context;
        this.topic = topic;
        this.threadNumber = threadNumber;
        this.stream = stream;
        this.consumedMessage = context.getMetricsService().createMeter(KafkaConsumerHandler.class,"consumed-event-" + this.getTopic() + "-" + String.valueOf(this.threadNumber));
        this.failedDecodedMessage = context.getMetricsService().createMeter(KafkaConsumerHandler.class,"failed-decoded-event-" + this.getTopic() + "-" + String.valueOf(this.threadNumber));
    }

    public void run() {
        logger.info("Starting to run a new consumer on topic: {} and thread: {}", this.topic, this.threadNumber);
        ConsumerIterator<byte[],byte[]> consumerIterator = stream.iterator();
        while (consumerIterator.hasNext()){
            JsonNode event = null;
            try {
                event = this.getContext().getDecoder().decode(consumerIterator.next().message());
                this.consumedMessage.mark();
                List<KafkaStreamListener> listeners = this.getContext().getListeners();
                for (KafkaStreamListener listener : listeners) {
                    try {
                        logger.debug("Processing event on topic: {}, thread: {} and listener {}", this.topic, this.threadNumber, listener.getName());
                        listener.process(topic, event);
                    } catch (KafkaStreamListenerException e) {
                        logger.error("There was an error while trying to process the stream service listener: {}", listener.getName(), e);
                    }
                }
            } catch (MessageDecoderException e) {
                logger.error("There was an error while trying to decode the message",e);
                this.failedDecodedMessage.mark();
            }
        }
    }

    public void shutdown() {
        logger.info("Consumer handler on topic: {} thread: {} closed successfully.", this.getTopic(), this.getThreadNumber());
    }
}