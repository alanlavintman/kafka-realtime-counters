package com.revizer.counters.v2.streaming;

import com.codahale.metrics.Meter;
import com.revizer.counters.services.metrics.MetricsService;
import com.revizer.counters.services.streaming.exceptions.InitializeDecoderException;
import com.revizer.counters.services.streaming.exceptions.MessageDecoderException;
import com.revizer.counters.services.streaming.kafka.MessageDecoder;
import com.revizer.counters.v2.CounterContext;
import org.apache.commons.configuration.Configuration;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Created by alanl on 11/10/14.
 */
public class KafkaJsonMessageDecoder {

    private ObjectMapper mapper = null;

    public KafkaJsonMessageDecoder() {
        mapper = new ObjectMapper();
    }

    public JsonNode decode(byte[] message) throws MessageDecoderException {
        try {
            String stringMessage = new String(message, "UTF-8");
            JsonNode node = mapper.readTree(stringMessage);
            return node;
        } catch (Exception e) {
            throw new MessageDecoderException(e);
        }
    }

}
