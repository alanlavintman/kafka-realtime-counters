package com.revizer.counters.services.streaming.kafka;

import com.revizer.counters.services.streaming.StreamServiceListener;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.codehaus.jackson.JsonNode;

import java.util.List;

/**
 * Created by alanl on 11/10/14.
 */
public class KafkaStreamingHandler<T> implements Runnable {

    private KafkaStream stream;
    private int threadNumber;
    private String topic;
    private KafkaJsonMessageDecoder decoder;
    private List<StreamServiceListener> listeners;

    public KafkaStreamingHandler(String topic, KafkaStream stream, int threadNumber, KafkaJsonMessageDecoder decoder, List<StreamServiceListener> listeners) {
        this.topic = topic;
        this.threadNumber = threadNumber;
        this.stream = stream;
        this.decoder = decoder;
        this.listeners = listeners;
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> consumerIterator = stream.iterator();
        while (consumerIterator.hasNext()){
            JsonNode event = decoder.decode(consumerIterator.next().message());
            for (StreamServiceListener listener : listeners) {
                listener.process(event);
            }
        }
    }
}
