package com.revizer.counters.v1.streaming;

import com.revizer.counters.services.metrics.MetricsService;
import com.revizer.counters.services.streaming.exceptions.InitializeDecoderException;
import com.revizer.counters.services.streaming.exceptions.MessageDecoderException;
import com.revizer.counters.services.streaming.kafka.MessageDecoder;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

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
