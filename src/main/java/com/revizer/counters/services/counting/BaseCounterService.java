package com.revizer.counters.services.counting;

import com.revizer.counters.services.counting.model.*;
import com.revizer.counters.services.metrics.MetricsService;
import com.revizer.counters.utils.ConfigurationParser;
import org.apache.commons.configuration.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by alanl on 11/10/14.
 */
public abstract class BaseCounterService implements CounterService {


    private Configuration configuration;
    private MetricsService metricsService;
    private CounterTopicAggregator counterTopicAggregator;
    private Map<String, CounterSlotHolder> countersSlotHolder;

    public Configuration getConfiguration() {
        return configuration;
    }

    public MetricsService getMetricsService() {
        return metricsService;
    }

    public CounterTopicAggregator getCounterTopicAggregator() {
        return counterTopicAggregator;
    }

    public Map<String, CounterSlotHolder> getCountersSlotHolder() {
        return countersSlotHolder;
    }

    protected BaseCounterService(Configuration configuration, MetricsService metricsService) {
        this.configuration = configuration;
        this.metricsService = metricsService;
        this.counterTopicAggregator = this.buildCountersMetadata(configuration);
        this.countersSlotHolder = new HashMap<String, CounterSlotHolder>();
        /* Creating counter slots for each topic. */
        for (String topic : this.counterTopicAggregator.getTopicStreamsMap().keySet()) {
            this.countersSlotHolder.put(topic, new CounterSlotHolder());
        }
    }

    public CounterTopicAggregator buildCountersMetadata(Configuration configuration){
        /* Filling in the metadata */
        CounterTopicAggregator counterTopic = new CounterTopicAggregator();
        counterTopic.setTopicStreamsMap(ConfigurationParser.getTopicAndNumOfStreams(configuration));
        counterTopic.setTopicNowField(ConfigurationParser.getTopicNowField(configuration));
        counterTopic.setTopicCounters(ConfigurationParser.getCountersByTopic(configuration));
        return counterTopic;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }
}

