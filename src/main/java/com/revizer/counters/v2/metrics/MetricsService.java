package com.revizer.counters.v2.metrics;

import com.codahale.metrics.*;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import static com.codahale.metrics.MetricRegistry.name;

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
    private Logger logger = LoggerFactory.getLogger(MetricsService.class);

    public MetricsService(Configuration configuration) {
        Preconditions.checkNotNull(configuration, "The configuration parameter can not be null");
        this.configuration = configuration;
        registry = new MetricRegistry();

    }

    public void start(){
        logger.info("Starting the metrics service.");
        reporter = JmxReporter.forRegistry(registry).build();
        reporter.start();
        logger.info("Metrics service started correctly.");
    }

    public void stop(){
        logger.info("Starting to shut down the metrics service");
        reporter.stop();
        logger.info("Metrics service has been shut down successfully");
    }

    public Counter createCounter(Class clz, String counterName){
        return registry.counter(name(clz, counterName));
    }

    public Timer createTimer(Class clz,String timerName){
        return registry.timer(name(clz, timerName));
    }

    public Meter createMeter(Class clz,String meterName){
        return registry.meter(name(clz, meterName));
    }

}
