package com.revizer.counters.v2.streaming;

import com.revizer.counters.v2.CounterContext;

/**
 * Created by cloudera on 12/9/14.
 */
public abstract class KafkaStreamListener {

    private CounterContext context;

    protected KafkaStreamListener(CounterContext context) {
        this.context = context;
    }

    private boolean process(String topic, String event){
        return false;
    }

}
