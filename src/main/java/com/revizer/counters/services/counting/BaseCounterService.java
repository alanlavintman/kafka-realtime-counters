package com.revizer.counters.services.counting;

import com.revizer.counters.services.counting.cleaner.CounterSlotHolderCleaner;
import com.revizer.counters.services.counting.model.*;
import com.revizer.counters.services.metrics.MetricsService;
import com.revizer.counters.utils.ConfigurationParser;
import org.apache.commons.configuration.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by alanl on 11/10/14.
 */
public abstract class BaseCounterService implements CounterService {


    private Configuration configuration;
    private MetricsService metricsService;
    // This object contains all the information of the property file in MAPS of type "Topic" -> "something"
    // Currenty it has three maps: topic->num of streams(Partitions in kafka), topic->now field, topic->counters
    private CounterTopicAggregator counterTopicAggregator;

    /* Field that holds all the counter slots per topic. This holds the timelines per topic! */
    private Map<String, CounterSlotHolder> countersSlotHolder;

    /* Field that holds the cleaner per topic */
    private Map<String, CounterSlotHolderCleaner> dataCleaner;

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
        dataCleaner = new HashMap<String, CounterSlotHolderCleaner>();
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

        int amountOfTopics = this.counterTopicAggregator.getTopicStreamsMap().keySet().size();
        ExecutorService executor = Executors.newFixedThreadPool(amountOfTopics);
        for (String topic : this.counterTopicAggregator.getTopicStreamsMap().keySet()) {
            // Get the counter slot holder by the given topic.
            CounterSlotHolder counterSlotHolder = this.countersSlotHolder.get(topic);
            // Create a new cleaner belonging to the topic and the counter slot holder.
            CounterSlotHolderCleaner cleaner = new CounterSlotHolderCleaner(topic, counterSlotHolder, this.getConfiguration());
            dataCleaner.put(topic, cleaner);
            executor.execute(cleaner);
        }

    }

    @Override
    public void stop() {

        for (CounterSlotHolderCleaner counterSlotHolderCleaner : dataCleaner.values()) {
//            counterSlotHolderCleaner.kill~!!!
        }

    }
}

