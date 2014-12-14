package com.revizer.counters.v1.streaming.listeners;

import com.codahale.metrics.Meter;
import com.revizer.counters.v1.CounterContext;
import com.revizer.counters.v1.counters.CounterSlotHolder;
import com.revizer.counters.v1.counters.metadata.AggregationCounter;
import com.revizer.counters.v1.counters.metadata.AggregationCounterKey;
import com.revizer.counters.v1.counters.metadata.TopicAggregationsMetadata;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by cloudera on 12/11/14.
 */
public class CounterKafkaListener extends KafkaStreamListener {

    private static Logger logger = LoggerFactory.getLogger(CounterKafkaListener.class);
    private Map<String, Meter> meters;

    public CounterKafkaListener(String name, CounterContext context) {
        super(name, context);

        this.meters = new HashMap<>();

        /* Build Meter's according to each topic */
        List<String> topicAndPartitions = context.getTopicAndPartition();
        for (String topicAndPartition : topicAndPartitions) {
            String topicName = topicAndPartition.split(":")[0];
            if (!this.meters.containsKey(topicName)){
                String meterName = "counter-process-" + topicName;
                Meter meter = context.getMetricsService().createMeter(CounterKafkaListener.class,meterName);
                this.meters.put(topicName, meter);
                logger.info("   Registering meter {}", meterName);
            }
        }
    }

    @Override
    public void process(String topic, JsonNode event) {

        /* Get the now parameter */
        Long eventNow = getNowField(topic, event);

        /* Gets the counter slot holder for the topic of the event. */
        TopicAggregationsMetadata topicAggregationsMetadata = this.getContext().getTopicAggregationsMetadata();
        CounterSlotHolder counterSlotHolder = topicAggregationsMetadata.getCountersSlotHolderPerTopic().get(topic);

        /* Get all the aggregations for this topic from the metadata object that holds per topic configuration (getCounterTopicAggregator)*/
        List<AggregationCounter> aggregationCounters = topicAggregationsMetadata.getTopicAggregations().get(topic);
        for (AggregationCounter aggregationCounter : aggregationCounters) {
            try {
                /* For each aggregation get the key and increment in the counter slot holder. */
                List<AggregationCounterKey> key = aggregationCounter.getCounterKey(eventNow, event);
                for (AggregationCounterKey aggregationCounterKey : key) {
                    counterSlotHolder.inc(eventNow, aggregationCounterKey);
                }
            } catch(Exception ex){
                logger.error("There was an error while trying to increment the values for the event: {}", event.toString(), ex);
            }
        }

        meters.get(topic).mark();
    }

    public Long getNowField(String topic, JsonNode payload){
        String nowField = this.getContext().getTopicAggregationsMetadata().getTopicNowField().get(topic);
        Long now = 0L;
        try {
            now = payload.get(nowField).getLongValue();
        } catch (Exception ex){
            /* If the event does not bring the field, we set up the now field by our own. */
            now = System.currentTimeMillis()/1000;
        }
        return now;
    }

}

