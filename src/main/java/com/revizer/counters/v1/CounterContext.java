package com.revizer.counters.v1;

import com.revizer.counters.v1.counters.metadata.TopicAggregationsMetadata;
import com.revizer.counters.v1.metrics.MetricsService;
import com.revizer.counters.v1.streaming.KafkaJsonMessageDecoder;
import com.revizer.counters.v1.streaming.listeners.KafkaStreamListener;
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
    private TopicAggregationsMetadata topicAggregationsMetadata;

    private MetricsService metricsService;
    private KafkaJsonMessageDecoder decoder;
    private List<KafkaStreamListener> listeners;

    public CounterContext(Configuration configuration, MetricsService metricsService) {
        this.configuration = configuration;
        this.brokers = new ArrayList<>();
        this.topicAndPartition = new ArrayList<>();
        this.metricsService = metricsService;
        this.listeners = new ArrayList<>();
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public KafkaJsonMessageDecoder getDecoder() {
        return decoder;
    }

    public void setDecoder(KafkaJsonMessageDecoder decoder) {
        this.decoder = decoder;
    }

    public List<KafkaStreamListener> getListeners() {
        return listeners;
    }

    public void setListeners(List<KafkaStreamListener> listeners) {
        this.listeners = listeners;
    }

    public MetricsService getMetricsService() {
        return metricsService;
    }

    public void setMetricsService(MetricsService metricsService) {
        this.metricsService = metricsService;
    }

    public TopicAggregationsMetadata getTopicAggregationsMetadata() {
        return topicAggregationsMetadata;
    }

    public void setTopicAggregationsMetadata(TopicAggregationsMetadata topicAggregationsMetadata) {
        this.topicAggregationsMetadata = topicAggregationsMetadata;
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
