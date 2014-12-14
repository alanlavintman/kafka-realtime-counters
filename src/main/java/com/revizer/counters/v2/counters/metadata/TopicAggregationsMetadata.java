package com.revizer.counters.v2.counters.metadata;

import com.revizer.counters.v2.counters.CounterSlotHolder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by cloudera on 12/9/14.
 */
public class TopicAggregationsMetadata {

    // Give me information per topic.
    private Map<String, Integer> topicStreamsMap = null;
    private Map<String, String> topicNowField = null;
    private Map<String, List<AggregationCounter>> topicAggregations = null; // for each topic you have a list of counters or aggregations.
    private Map<String, Boolean> topicCountersTotal = null; // for each topic you have a list of counters or aggregations.
    private Map<String, CounterSlotHolder> countersSlotHolderPerTopic = null; // for each topic you have a CounterSlotHolder which is the timeline of counters.

    public TopicAggregationsMetadata() {
        this.topicNowField = new HashMap<>();
        this.topicAggregations = new HashMap<>();
        this.topicCountersTotal = new HashMap<>();
        this.countersSlotHolderPerTopic = new HashMap<String, CounterSlotHolder>();
    }


    public void addNowField(String topic, String field){
        this.topicNowField.put(topic, field);
    }

    public void addTopicCountersTotal(String topic){
        this.topicCountersTotal.put(topic, true);
    }

    public void addTopicCounterSlotHolder(String topic){
        this.countersSlotHolderPerTopic.put(topic, new CounterSlotHolder());
    }

    public Map<String, CounterSlotHolder> getCountersSlotHolderPerTopic() {
        return countersSlotHolderPerTopic;
    }

    public void setCountersSlotHolderPerTopic(Map<String, CounterSlotHolder> countersSlotHolderPerTopic) {
        this.countersSlotHolderPerTopic = countersSlotHolderPerTopic;
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
