package com.revizer.counters.services.streaming;

import com.google.common.base.Preconditions;
import com.revizer.counters.services.metrics.MetricsService;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by alanl on 08/11/14.
 */
public abstract class StreamingService {

    private static Logger logger = LoggerFactory.getLogger(StreamingService.class);

    private Configuration configuration;
    private MetricsService metricsService;
    private List<StreamServiceListener> listeners = new ArrayList<StreamServiceListener>();

    public Configuration getConfiguration() {
        return configuration;
    }

    public MetricsService getMetricsService() {
        return metricsService;
    }

    public List<StreamServiceListener> getListeners() {
        return new ArrayList<StreamServiceListener>(listeners);
    }

    public StreamingService(Configuration configuration, MetricsService metricsService) {
        Preconditions.checkNotNull(configuration, "The configuration parameter can not be null");
        Preconditions.checkNotNull(metricsService, "The metricsService parameter can not be null");
        this.configuration = configuration;
        this.metricsService = metricsService;
    }

    public void addListener(StreamServiceListener listener){
        listeners.add(listener);
        logger.info("Streaming=> Registering a listener: {}", listener.getName());
    }
    public abstract void start();

    public abstract void stop();

}
