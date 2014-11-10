package com.revizer.counters.services.counting;

import org.codehaus.jackson.JsonNode;

/**
 * Created by alanl on 11/10/14.
 */
public interface CounterService {

    void process(JsonNode payload);

    void process(String payload);

    void process(byte[] payload);

    void start();

    void stop();

}
