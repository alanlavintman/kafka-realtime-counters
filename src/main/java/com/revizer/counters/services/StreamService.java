package com.revizer.counters.services;

import org.apache.commons.configuration.Configuration;

/**
 * Created by alanl on 08/11/14.
 */
public class StreamService {

    private Configuration configuration;
    private MetricsService metricsService;
    private CounterService counterService;

    public Configuration getConfiguration() {
        return configuration;
    }

    public StreamService(Configuration configuration, MetricsService metricsService, CounterService counterService) {
        this.configuration = configuration;
        this.metricsService = metricsService;
        this.counterService = counterService;
    }

    public void start() {

    }

    public void stop() {

    }
}
