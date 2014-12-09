package com.revizer.counters.v2.counters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by cloudera on 12/9/14.
 */
public class TopicAggregationsHolder {

    // Give me information per topic.
    private Map<String, Integer> topicStreamsMap = null;
    private Map<String, String> topicNowField = null;
    private Map<String, List<AggregationCounter>> topicAggregations = null; // for each topic you have a list of counters or aggregations.
    private Map<String, Boolean> topicCountersTotal = null; // for each topic you have a list of counters or aggregations.

    public TopicAggregationsHolder() {
        this.topicNowField = new HashMap<>();
        this.topicAggregations = new HashMap<>();
        this.topicCountersTotal = new HashMap<>();
    }

    public void addNowField(String topic, String field){
        this.topicNowField.put(topic, field);
    }

    public void addTopicCountersTotal(String topic){
        this.topicCountersTotal.put(topic, true);
    }

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

    public Map<String, List<AggregationCounter>> getTopicAggregations() {
        return topicAggregations;
    }

    public void setTopicAggregations(Map<String, List<AggregationCounter>> topicAggregations) {
        this.topicAggregations = topicAggregations;
    }

}
