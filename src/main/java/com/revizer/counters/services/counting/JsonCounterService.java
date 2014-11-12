package com.revizer.counters.services.counting;

import com.codahale.metrics.Timer;
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

    private Timer processTimer;
    private boolean processCounterService = true;

    private static Logger logger = LoggerFactory.getLogger(JsonCounterService.class);

    public JsonCounterService(Configuration configuration, MetricsService metricsService) {
        super(configuration, metricsService);
        this.processCounterService = this.getConfiguration().getBoolean("counters.configuration.process.counter.service",false);
        if (this.processCounterService){
            this.processTimer = this.getMetricsService().createTimer(BaseCounterService.class,"aggregation-key-generation");
        }
    }

    /**
     * The process method is in charge of incrementing a value for a certain topic and payload.
     * It will run all the aggregations
     * @param topic
     * @param payload
     */
    @Override
    public void process(String topic, JsonNode payload) {
        Timer.Context time = null;
        if (this.processTimer != null){
            time = this.processTimer.time();
        }
        /* Get the now parameter */
        Long eventNow = getNowField(topic, payload);

        /* Gets the counter slot holder for the topic of the event. */
        CounterSlotHolder counterSlotHolder = this.getCountersSlotHolder().get(topic);

        /* Get all the aggregations for this topic from the metadata object that holds per topic configuration (getCounterTopicAggregator)*/
        List<AggregationCounter> aggregationCounters = this.getCounterTopicAggregator().getTopicCounters().get(topic);
        for (AggregationCounter aggregationCounter : aggregationCounters) {
            try {
                /* For each aggregation get the key and increment in the counter slot holder. */
                List<AggregationCounterKey> key = aggregationCounter.getCounterKey(eventNow, payload);
                for (AggregationCounterKey aggregationCounterKey : key) {
                    counterSlotHolder.inc(eventNow, aggregationCounterKey);
                }
            } catch(Exception ex){
                logger.error("There was an error while trying to increment the values for the event: {}", payload.toString(), ex);
            }
        }
        if (time != null){
            time.stop();
        }
    }

    /**
     * Gets the now field in seconds.
     * @param topic The topic we are currently about to evaluate.
     * @param payload The payload that contains the fields we need to search for.
     * @return A Long value containing the value of the topic.
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
