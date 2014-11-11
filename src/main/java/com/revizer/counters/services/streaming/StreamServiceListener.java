package com.revizer.counters.services.streaming;

import com.revizer.counters.services.metrics.MetricsService;
import com.revizer.counters.services.streaming.exceptions.StreamServiceListenerException;
import org.apache.commons.configuration.Configuration;
import org.codehaus.jackson.JsonNode;

/**
 * Created by alanl on 11/9/14.
 */
public interface StreamServiceListener {

    String getName();

    void initialize(Configuration configuration, MetricsService metricsService);

    void process(String topic, JsonNode payload) throws StreamServiceListenerException;

}
