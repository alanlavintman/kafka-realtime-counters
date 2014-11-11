package com.revizer.counters.services.counting.model;

import java.util.List;
import java.util.Map;

/**
 * Created by alanl on 11/10/14.
 */
public class CounterTopicAggregator {

    private Map<String, Integer> topicStreamsMap = null;
    private Map<String, String> topicNowField = null;
    private Map<String, List<AggregationCounter>> topicCounters = null; // for each topic you have a list of counters.

    public Map<String, Integer> getTopicStreamsMap() {
        return topicStreamsMap;
    }

    public void setTopicStreamsMap(Map<String, Integer> topicStreamsMap) {
        this.topicStreamsMap = topicStreamsMap;
    }

    public Map<String, String> getTopicNowField() {
        return topicNowField;
    }

    public void setTopicNowField(Map<String, String> topicNowField) {
        this.topicNowField = topicNowField;
    }

    public Map<String, List<AggregationCounter>> getTopicCounters() {
        return topicCounters;
    }

    public void setTopicCounters(Map<String, List<AggregationCounter>> topicCounters) {
        this.topicCounters = topicCounters;
    }
}
