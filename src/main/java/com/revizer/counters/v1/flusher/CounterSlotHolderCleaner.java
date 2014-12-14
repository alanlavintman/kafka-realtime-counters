package com.revizer.counters.v1.flusher;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.revizer.counters.v1.CounterContext;
import com.revizer.counters.v1.counters.CounterSlotHolder;
import com.revizer.counters.v1.counters.metadata.AggregationCounterKey;
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

    private volatile boolean keepRunning = true;
    private String topic;
    private CounterSlotHolder holder;
    private Map<String, CounterSlotHolder> counterHolder = new HashMap<String, CounterSlotHolder>();
    private static Logger logger = LoggerFactory.getLogger(CounterSlotHolderCleaner.class);
    private int reciclePeriod;
    private int amountOfRetries;
    private int recicleRetryCount;
    private CounterRepository repository;
    private Meter errorMeter;
    private Timer processingTime;


    public boolean isKeepRunning() {
        return keepRunning;
    }

    public void setKeepRunning(boolean keepRunning) {
        this.keepRunning = keepRunning;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public CounterSlotHolder getHolder() {
        return holder;
    }

    public void setHolder(CounterSlotHolder holder) {
        this.holder = holder;
    }

    public Map<String, CounterSlotHolder> getCounterHolder() {
        return counterHolder;
    }

    public void setCounterHolder(Map<String, CounterSlotHolder> counterHolder) {
        this.counterHolder = counterHolder;
    }

    public int getReciclePeriod() {
        return reciclePeriod;
    }

    public void setReciclePeriod(int reciclePeriod) {
        this.reciclePeriod = reciclePeriod;
    }

    public int getAmountOfRetries() {
        return amountOfRetries;
    }

    public void setAmountOfRetries(int amountOfRetries) {
        this.amountOfRetries = amountOfRetries;
    }

    public CounterRepository getRepository() {
        return repository;
    }

    public void setRepository(CounterRepository repository) {
        this.repository = repository;
    }

    public CounterSlotHolderCleaner(String topic, CounterSlotHolder holder, CounterContext context) {
        this.topic = topic;
        this.holder = holder;
        this.reciclePeriod = context.getConfiguration().getInt("counter.cleaner.recicle.period.ms");
        this.recicleRetryCount= context.getConfiguration().getInt("counter.cleaner.recicle.retry.count");
        this.processingTime = context.getMetricsService().createTimer(CounterSlotHolderCleaner.class,"counters-flusher-processing-time");
        this.errorMeter = context.getMetricsService().createMeter(CounterSlotHolderCleaner.class, "counters-flusher-error-meter");
    }

    @Override
    public void run() {

        logger.info("CounterSlotHolderCleaner => Initializing the counter slot holder cleaner for topic: {} and recicle preiod: {}", topic, reciclePeriod);
        ConcurrentHashMap<AggregationCounterKey, AtomicLong> olderSlot = null;
        while(keepRunning){
            try {
                this.flushWithRetry();
                Thread.sleep(reciclePeriod);
            } catch (InterruptedException e) {
                logger.error("There has been an interrupted exception for the counter slot holder cleaner in topic: {}", topic, e);
            }
        }
    }



    public void shutdown(){
        logger.info("Starting to shut down the cleaner for topic {}.", topic);
        keepRunning=false;
        /* Clean all the holder size */
        while(holder.size() > 0){
            logger.info("Counter holder size for {} is {}.", topic, holder.size());
            flushWithRetry();
        }
    }

    public void flushWithRetry(){
        if (!this.flush()){
            // try to reflush it 3 times.
            logger.error("Flusing failed, waiting 1 second and trying again for {} times.", this.recicleRetryCount);
            boolean succeedRetry = false;
            for (int i=0;i<this.recicleRetryCount;i++){
                try {
                    logger.info("Flusing failed for {} times, waiting 1 second and trying again.", i+1);
                    if (!this.flush()){
                        Thread.sleep(1000);
                    } else {
                        succeedRetry = true;
                        break;
                    }
                } catch (InterruptedException e) {
                    logger.error("There was an interrupted exception", e);
                }
            }
            if (!succeedRetry){
                logger.error("The flush with retry for topic {} failed. Dropping the data.", this.topic);
            }
        }
    }

    public boolean flush(){
        // If the olderSlot is null, it means we need to pick up a new slot to persist.
        boolean persisted = false;
        Integer slotKeyInMinute = holder.getOlderSlotKey();
        /* If there is at least one slot, we try to persist it */
        if (slotKeyInMinute != null && slotKeyInMinute.intValue() != -1){
            ConcurrentHashMap<AggregationCounterKey, AtomicLong> olderSlot = holder.removeSlot(slotKeyInMinute);
            Timer.Context time = null;
            try {
                // Persist the data in cassandra.
                time = this.processingTime.time();
                logger.debug("Starting to persist topic {} slotkey in minutes {}", topic, slotKeyInMinute);
                repository.persist(topic, slotKeyInMinute, olderSlot);
                logger.debug("Topic {} and slotkey in minutes {} persisted successfully", topic, slotKeyInMinute);
                persisted = true;
            } catch (CounterRepositoryException e) {
                logger.error("Error while trying to store slot: {} and topic : {} in cassandra.", topic, e);
                errorMeter.mark();
            } finally{
                if (time != null){
                    time.stop();
                }
            }
        } else {
            logger.debug("The counter holder is empty for topic {}", topic);
            persisted = true;
        }
        return persisted;
    }

}
