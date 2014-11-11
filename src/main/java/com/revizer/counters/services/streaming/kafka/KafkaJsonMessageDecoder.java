package com.revizer.counters.services.streaming.kafka;

import com.codahale.metrics.Meter;
import com.revizer.counters.services.metrics.MetricsService;
import com.revizer.counters.services.streaming.exceptions.InitializeDecoderException;
import com.revizer.counters.services.streaming.exceptions.MessageDecoderException;
import org.apache.commons.configuration.Configuration;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Created by alanl on 11/10/14.
 */
public class KafkaJsonMessageDecoder implements MessageDecoder<JsonNode> {

    private ObjectMapper mapper = null;
    private Meter failedDecoderMeter = null;
    private Meter failedDecoderStringMeter = null;

    @Override
    public JsonNode decode(byte[] message) throws MessageDecoderException {
        try {
            String stringMessage = new String(message, "UTF-8");
            JsonNode node = mapper.readTree(stringMessage);
            return null;
        } catch (UnsupportedEncodingException e) {
            failedDecoderStringMeter.mark();
            throw new MessageDecoderException(e);
        } catch (JsonProcessingException e) {
            failedDecoderMeter.mark();
            throw new MessageDecoderException(e);
        } catch (IOException e) {
            failedDecoderMeter.mark();
            throw new MessageDecoderException(e);
        }
    }

    @Override
    public void initialize(Configuration configuration, MetricsService service) throws InitializeDecoderException {
        mapper = new ObjectMapper();
        failedDecoderMeter = service.createMeter(MessageDecoder.class, "failed-to-decode-json");
        failedDecoderStringMeter = service.createMeter(MessageDecoder.class, "failed-to-decode-string");
    }

}
