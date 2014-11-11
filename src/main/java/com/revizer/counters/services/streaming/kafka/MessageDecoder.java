package com.revizer.counters.services.streaming.kafka;

import com.revizer.counters.services.metrics.MetricsService;
import com.revizer.counters.services.streaming.exceptions.InitializeDecoderException;
import com.revizer.counters.services.streaming.exceptions.MessageDecoderException;
import org.apache.commons.configuration.Configuration;

/**
 * Created by alanl on 11/10/14.
 */
public interface MessageDecoder<T> {

    T decode(byte[] message) throws MessageDecoderException;

    void initialize(Configuration configuration, MetricsService service) throws InitializeDecoderException;

}
