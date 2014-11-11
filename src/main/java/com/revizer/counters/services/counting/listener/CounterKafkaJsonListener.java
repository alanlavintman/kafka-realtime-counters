package com.revizer.counters.services.counting.listener;

import com.codahale.metrics.Meter;
import com.google.common.base.Preconditions;
import com.revizer.counters.services.counting.JsonCounterService;
import com.revizer.counters.services.metrics.MetricsService;
import com.revizer.counters.services.streaming.StreamServiceListener;
import com.revizer.counters.services.streaming.exceptions.StreamServiceListenerException;
import org.apache.commons.configuration.Configuration;
import org.codehaus.jackson.JsonNode;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by alanl on 11/10/14.
 */
public class CounterKafkaJsonListener implements StreamServiceListener {

    private Configuration configuration;
    private MetricsService metricsService;
    private JsonCounterService counterService;
    private Meter errorCount;

    @Override
    public String getName() {
        return "Generic Kafka Counter Listener";
    }

    public CounterKafkaJsonListener(JsonCounterService counterService) {
        Preconditions.checkNotNull(counterService, "The counterService parameter can not be null");
        this.counterService = counterService;
    }

    @Override
    public void initialize(Configuration configuration, MetricsService metricsService) {
        Preconditions.checkNotNull(configuration, "The configuration parameter can not be null");
        Preconditions.checkNotNull(metricsService, "The metricsService parameter can not be null");
        this.configuration = configuration;
        this.metricsService = metricsService;
        this.errorCount = metricsService.createMeter(CounterKafkaJsonListener.class,"error-processing");
    }

    @Override
    public void process(String topic, JsonNode payload) throws StreamServiceListenerException {
        try{
            counterService.process(topic, payload);
        } catch(Exception ex){
            this.errorCount.mark();
            throw new StreamServiceListenerException(ex);
        }
    }

}
