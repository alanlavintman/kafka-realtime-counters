package com.revizer.counters.services.counting;

import org.codehaus.jackson.JsonNode;

/**
 * Created by alanl on 11/10/14.
 */
public interface CounterService {

    void process(String payload, JsonNode jsonNode);

    void start();

    void stop();

}
