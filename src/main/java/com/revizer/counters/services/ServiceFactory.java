package com.revizer.counters.services;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import com.revizer.counters.services.counting.listener.CounterKafkaJsonListener;
import com.revizer.counters.services.counting.JsonCounterService;
import com.revizer.counters.services.metrics.MetricsService;
import com.revizer.counters.services.streaming.StreamServiceListener;
import com.revizer.counters.services.streaming.StreamingService;
import com.revizer.counters.services.streaming.kafka.KafkaStreamingService;
import org.apache.commons.configuration.Configuration;

/**
 * Created by alanl on 11/9/14.
 */
public class ServiceFactory {

    private Configuration configuration;
    private MetricsService metricsService;
    private Counter streamingServiceCounter;
    private Counter counterServiceCounter;
    private Counter streamServiceListenerCounter;

    public ServiceFactory(Configuration configuration, MetricsService metricsService) {
        Preconditions.checkNotNull(configuration, "The configuration parameter can not be null");
        Preconditions.checkNotNull(metricsService, "The metricsService parameter can not be null");
        this.configuration = configuration;
        this.metricsService = metricsService;

        this.streamingServiceCounter = metricsService.createCounter(ServiceFactory.class,"create-streaming-service");
        this.counterServiceCounter = metricsService.createCounter(ServiceFactory.class,"create-counter-service");
        this.streamServiceListenerCounter = metricsService.createCounter(ServiceFactory.class,"create-streaming-listener");
    }

    public StreamingService createStreamingServiceInstance() {
        StreamingService streamingService = new KafkaStreamingService(this.configuration, this.metricsService);
        streamingServiceCounter.inc();
        return streamingService;
    }

    public StreamingService createStreamingServiceInstance(StreamServiceListener listener) {
        StreamingService streamingService = new KafkaStreamingService(this.configuration, this.metricsService);
        streamingService.addListener(listener);
        streamingServiceCounter.inc();
        return streamingService;
    }

    public JsonCounterService createCounterServiceInstance() {
        JsonCounterService counterService = new JsonCounterService(this.configuration, this.metricsService);
        counterServiceCounter.inc();
        return counterService;
    }

    public StreamServiceListener createCounterStreamServiceListenerInstance() {
        CounterKafkaJsonListener counterKafkaJsonListener = new CounterKafkaJsonListener(this.createCounterServiceInstance());
        counterKafkaJsonListener.initialize(this.configuration, this.metricsService);
        streamServiceListenerCounter.inc();
        return counterKafkaJsonListener;
    }

    public StreamServiceListener createCounterStreamServiceListenerInstance(JsonCounterService service) {
        CounterKafkaJsonListener counterKafkaJsonListener = new CounterKafkaJsonListener(service);
        counterKafkaJsonListener.initialize(this.configuration, this.metricsService);
        streamServiceListenerCounter.inc();
        return counterKafkaJsonListener;
    }

}