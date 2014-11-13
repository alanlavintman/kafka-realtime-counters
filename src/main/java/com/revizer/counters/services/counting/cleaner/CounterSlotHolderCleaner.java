package com.revizer.counters.services.counting.cleaner;

import com.revizer.counters.services.counting.exceptions.CounterRepositoryException;
import com.revizer.counters.services.counting.model.AggregationCounterKey;
import com.revizer.counters.services.counting.model.CounterSlotHolder;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by alanl on 11/11/14.
 */
public class CounterSlotHolderCleaner implements Runnable {

    private String topic;
    private CounterSlotHolder holder;
    private Configuration configuration;
    private Map<String, CounterSlotHolder> counterHolder = new HashMap<String, CounterSlotHolder>();
    private static Logger logger = LoggerFactory.getLogger(CounterSlotHolderCleaner.class);
    private int reciclePeriod;
    private int amountOfRetries;
    private CounterRepository repository;

    public CounterSlotHolderCleaner(String topic, CounterSlotHolder holder, Configuration configuration) {
        this.topic = topic;
        this.holder = holder;
        this.configuration = configuration;
        this.reciclePeriod = configuration.getInt("counter.cleaner.recicle.period.ms");
    }

    @Override
    public void run() {

        logger.info("CounterSlotHolderCleaner => Initializing the counter slot holder cleaner for topic: {} and recicle preiod: {}", topic, reciclePeriod);
        ConcurrentHashMap<AggregationCounterKey, AtomicLong> olderSlot = null;
        while(true){
            try {

                Thread.sleep(reciclePeriod);

                // If the olderSlot is null, it means we need to pick up a new slot to persist.
                Integer slotKeyInMinute = holder.getOlderSlotKey();
                olderSlot = holder.removeSlot(slotKeyInMinute);
                try {
                    // Persist the data in cassandra.
                    repository.persist(topic, slotKeyInMinute, olderSlot);

                } catch (CounterRepositoryException e) {
                    logger.error("Error while trying to store slot: {} and topic : {} in cassandra.", topic, e);
                }

            } catch (InterruptedException e) {
                logger.error("There has been an interrupted exception for the counter slot holder cleaner in topic: {}", topic, e);
            }
        }
    }

}
