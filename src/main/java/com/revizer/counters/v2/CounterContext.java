package com.revizer.counters.v2;

import com.revizer.counters.v2.counters.TopicAggregationsHolder;
import org.apache.commons.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by alanl on 12/8/14.
 */
public class CounterContext {

    private Configuration configuration;
    private List<String> brokers;
    private List<String> topicAndPartition;
    private TopicAggregationsHolder topicAggregationsHolder;

    public CounterContext(Configuration configuration) {
        this.configuration = configuration;
        this.brokers = new ArrayList<>();
        this.topicAndPartition = new ArrayList<>();
    }

    public TopicAggregationsHolder getTopicAggregationsHolder() {
        return topicAggregationsHolder;
    }

    public void setTopicAggregationsHolder(TopicAggregationsHolder topicAggregationsHolder) {
        this.topicAggregationsHolder = topicAggregationsHolder;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public List<String> getBrokers() {
        return brokers;
    }

    public void setBrokers(List<String> brokers) {
        this.brokers = brokers;
    }

    public List<String> getTopicAndPartition() {
        return topicAndPartition;
    }

    public void setTopicAndPartition(List<String> topicAndPartition) {
        this.topicAndPartition = topicAndPartition;
    }

}
