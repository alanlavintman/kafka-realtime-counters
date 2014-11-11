package com.revizer.counters.services.counting;

import com.revizer.counters.services.counting.model.CounterSlotHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by alanl on 11/11/14.
 */
public class CounterSlotHolderCleaner implements Runnable{

    private Map<String, CounterSlotHolder> counterHolder = new HashMap<String, CounterSlotHolder>();
    private static Logger logger = LoggerFactory.getLogger(CounterSlotHolderCleaner.class);

    @Override
    public void run() {

        logger.info("Starting to clean the data");

        logger.info("Cleaned the data successfully.");
    }

}
