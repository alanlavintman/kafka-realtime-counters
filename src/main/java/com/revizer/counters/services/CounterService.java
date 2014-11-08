package com.revizer.counters.services;

import org.apache.commons.configuration.Configuration;

/**
 * Created by alanl on 08/11/14.
 */
public class CounterService {

    private Configuration configuration;
    private MetricsService metricsService;

    public Configuration getConfiguration() {
        return configuration;
    }

    public CounterService(Configuration configuration, MetricsService metricsService) {
        this.configuration = configuration;
        this.metricsService = metricsService;
    }
}
