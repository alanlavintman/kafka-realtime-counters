package com.revizer.counters.v1.streaming.listeners;

import com.revizer.counters.v1.CounterContext;
import org.codehaus.jackson.JsonNode;

/**
 * Created by cloudera on 12/9/14.
 */
public abstract class KafkaStreamListener {

    private CounterContext context;
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public CounterContext getContext() {
        return context;
    }

    public void setContext(CounterContext context) {
        this.context = context;
    }

    public KafkaStreamListener(String name, CounterContext context) {
        this.name = name;
        this.context = context;
    }

    public abstract void process(String topic, JsonNode event) throws KafkaStreamListenerException;

}
