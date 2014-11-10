package com.revizer.counters.services.counting;

import com.revizer.counters.services.metrics.MetricsService;
import com.revizer.counters.services.streaming.StreamServiceListener;
import org.apache.commons.configuration.Configuration;
import org.codehaus.jackson.JsonNode;

/**
 * Created by alanl on 08/11/14.
 */
public class JsonCounterService extends BaseCounterService {

    public JsonCounterService(Configuration configuration, MetricsService metricsService) {
        super(configuration, metricsService);
    }

    @Override
    public void process(JsonNode payload) {

    }


}
