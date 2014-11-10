package com.revizer.counters.services.streaming;

import com.revizer.counters.services.metrics.MetricsService;
import org.apache.commons.configuration.Configuration;
import org.codehaus.jackson.JsonNode;

/**
 * Created by alanl on 11/9/14.
 */
public interface StreamServiceListener {

    String getName();

    void initialize(Configuration configuration, MetricsService metricsService);

    void process(JsonNode payload);

    void process(String payload);

    void process(byte[] payload);

}
