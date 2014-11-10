package com.revizer.counters.services.counting;

import com.revizer.counters.services.counting.model.*;
import com.revizer.counters.services.counting.model.Counter;
import com.revizer.counters.services.metrics.MetricsService;
import com.revizer.counters.services.utils.ConfigurationParser;
import org.apache.commons.configuration.Configuration;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import org.codehaus.jackson.JsonNode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by alanl on 11/10/14.
 */
public abstract class BaseCounterService implements CounterService {


    private Configuration configuration;
    private MetricsService metricsService;
    private CounterTopicMetadata counterTopicMetadata;

    protected BaseCounterService(Configuration configuration, MetricsService metricsService) {
        this.configuration = configuration;
        this.metricsService = metricsService;
        this.counterTopicMetadata = this.buildCountersMetadata(configuration);
    }

    public CounterTopicMetadata buildCountersMetadata(Configuration configuration){
        CounterTopicMetadata counterTopic = new CounterTopicMetadata();
        counterTopic.setTopicStreamsMap(ConfigurationParser.getTopicAndNumOfStreams(configuration));
        counterTopic.setTopicNowField(ConfigurationParser.getTopicNowField(configuration));
        counterTopic.setTopicCounters(ConfigurationParser.getCountersByTopic(configuration));
        return counterTopic;
    }

    public void configureCounters(){
        // for each topic, collect the counters:

        Map<String, Counter> stringCountersMap = buildCounters(configuration, ConfigurationParser.getKeysThatStartsWith(configuration, COUNTERS_CONFIGURATION_KEYS_STARTS_WITH));
    }

    public Map<String, Counter> buildCounters(Configuration configuration, List<String> keys){
        Map<String, Counter> counters = new HashMap<String, Counter>();
        for (String key : keys) {
            String counterName = key.substring(COUNTERS_CONFIGURATION_KEYS_STARTS_WITH.length());
            String[] fields = configuration.getStringArray(key);


        }
        return null;
    }

    @Override
    public void process(JsonNode payload) {
        throw new NotImplementedException();
    }

    @Override
    public void process(String payload) {
        throw new NotImplementedException();
    }

    @Override
    public void process(byte[] payload) {
        throw new NotImplementedException();
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }
}

