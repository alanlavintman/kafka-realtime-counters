package com.revizer.counters.services;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import org.apache.commons.configuration.Configuration;

/**
 * Created by alanl on 08/11/14.
 */
public class MetricsService {

    private MetricRegistry registry = null;
    private JmxReporter reporter = null;

    private Configuration configuration;

    public Configuration getConfiguration() {
        return configuration;
    }

    public MetricsService(Configuration configuration) {
        this.configuration = configuration;
        registry = new MetricRegistry();
        reporter = JmxReporter.forRegistry(registry).build();
        reporter.start();
    }

}
