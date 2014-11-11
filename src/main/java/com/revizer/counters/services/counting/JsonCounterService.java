package com.revizer.counters.services.counting;

import com.revizer.counters.services.counting.model.AggregationCounter;
import com.revizer.counters.services.counting.model.AggregationCounterKey;
import com.revizer.counters.services.counting.model.CounterSlotHolder;
import com.revizer.counters.services.metrics.MetricsService;
import com.revizer.counters.services.streaming.StreamServiceListener;
import org.apache.commons.configuration.Configuration;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by alanl on 08/11/14.
 */
public class JsonCounterService extends BaseCounterService {

    private static Logger logger = LoggerFactory.getLogger(JsonCounterService.class);

    public JsonCounterService(Configuration configuration, MetricsService metricsService) {
        super(configuration, metricsService);
    }

    @Override
    public void process(String topic, JsonNode payload) {
        /* Get the now parameter */
        Long now = getNowField(topic, payload);

        /* Gets the counter slot holder for the topic of the event. */
        CounterSlotHolder counterSlotHolder = this.getCountersSlotHolder().get(topic);

        /* Get all the aggregations for this topic from the metadata object that holds per topic configuration (getCounterTopicAggregator)*/
        List<AggregationCounter> aggregationCounters = this.getCounterTopicAggregator().getTopicCounters().get(topic);
        for (AggregationCounter aggregationCounter : aggregationCounters) {
            try {
                /* For each aggregation get the key and increment in the counter slot holder. */
                AggregationCounterKey key = aggregationCounter.getCounterKey(now, payload);
                counterSlotHolder.inc(now, key);
            } catch(Exception ex){
                logger.error("There was an error while trying to increment the values for the event: {}", payload.toString(), ex);
            }
        }
    }

    /**
     * Gets the now field in seconds.
     * @param topic
     * @param payload
     * @return
     */
    public Long getNowField(String topic, JsonNode payload){
        String nowField = this.getCounterTopicAggregator().getTopicNowField().get(topic);
        Long now = 0L;
        try {
            now = payload.get(nowField).getLongValue();
        } catch (Exception ex){
            now = System.currentTimeMillis()/1000;
        }
        return now;
    }

}
