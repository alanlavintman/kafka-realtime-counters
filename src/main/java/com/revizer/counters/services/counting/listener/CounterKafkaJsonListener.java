package com.revizer.counters.services.counting.listener;

import com.google.common.base.Preconditions;
import com.revizer.counters.services.counting.JsonCounterService;
import com.revizer.counters.services.metrics.MetricsService;
import com.revizer.counters.services.streaming.StreamServiceListener;
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
    }

    @Override
    public void process(JsonNode payload) {

    }

    @Override
    public void process(String payload) {

        throw new NotImplementedException();
    }

    @Override
    public void process(byte[] payload) {
        throw new NotImplementedException();
    }

}
