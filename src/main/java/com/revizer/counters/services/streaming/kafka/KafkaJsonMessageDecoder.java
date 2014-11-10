package com.revizer.counters.services.streaming.kafka;

import com.revizer.counters.services.metrics.MetricsService;
import org.apache.commons.configuration.Configuration;
import org.codehaus.jackson.JsonNode;

/**
 * Created by alanl on 11/10/14.
 */
public class KafkaJsonMessageDecoder implements MessageDecoder<JsonNode> {

    @Override
    public JsonNode decode(byte[] message) {
        return null;
    }

    @Override
    public void initialize(Configuration configuration, MetricsService service) throws InitializeDecoderException {

    }
}
