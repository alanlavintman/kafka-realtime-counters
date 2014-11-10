package com.revizer.counters.services.metrics;

import com.codahale.metrics.*;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration.Configuration;
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

    public MetricsService(Configuration configuration) {
        Preconditions.checkNotNull(configuration, "The configuration parameter can not be null");
        this.configuration = configuration;
        registry = new MetricRegistry();
        reporter = JmxReporter.forRegistry(registry).build();
    }

    public void start(){
        reporter.start();
    }

    public void stop(){
        throw new NotImplementedException();
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
