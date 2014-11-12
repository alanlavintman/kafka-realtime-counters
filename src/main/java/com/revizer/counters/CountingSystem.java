package com.revizer.counters;

import com.revizer.counters.services.ServiceFactory;
import com.revizer.counters.services.counting.JsonCounterService;
import com.revizer.counters.services.streaming.StreamServiceListener;
import com.revizer.counters.services.streaming.StreamingService;

/**
 * Created by alanl on 11/9/14.
 */
public class CountingSystem {

    private StreamingService streamingService;
    private JsonCounterService counterService;

    public CountingSystem(ServiceFactory factory) {
        counterService = factory.createCounterServiceInstance();
        StreamServiceListener counterListener = factory.createCounterStreamServiceListenerInstance(counterService);
        streamingService = factory.createStreamingServiceInstance();
        streamingService.addListener(counterListener);
    }

    public void start(){
        // Start the counting service before which starts the flushing threads.
        counterService.start();

        // Later on start the streaming service so it can start counting.
        streamingService.start();
    }

    public void stop(){

        // First of all, stop the streaming service to close
        streamingService.stop();

        // First of all, stop the streaming service.
        counterService.stop();

    }


}
