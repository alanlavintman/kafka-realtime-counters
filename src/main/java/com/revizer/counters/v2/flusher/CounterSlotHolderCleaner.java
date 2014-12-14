package com.revizer.counters.v2.flusher;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.revizer.counters.v2.CounterContext;
import com.revizer.counters.v2.counters.CounterSlotHolder;
import com.revizer.counters.v2.counters.metadata.AggregationCounterKey;
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
    private Configuration configuration;
    private Map<String, CounterSlotHolder> counterHolder = new HashMap<String, CounterSlotHolder>();
    private static Logger logger = LoggerFactory.getLogger(CounterSlotHolderCleaner.class);
    private int reciclePeriod;
    private int amountOfRetries;
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

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
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

    public CounterSlotHolderCleaner(String topic, CounterSlotHolder holder, Configuration configuration, CounterContext context) {
        this.topic = topic;
        this.holder = holder;
        this.configuration = configuration;
        this.reciclePeriod = configuration.getInt("counter.cleaner.recicle.period.ms");
        this.processingTime = context.getMetricsService().createTimer(CounterSlotHolderCleaner.class,"counters-flusher-processing-time");
        this.errorMeter = context.getMetricsService().createMeter(CounterSlotHolderCleaner.class, "counters-flusher-error-meter");
    }

    @Override
    public void run() {

        logger.info("CounterSlotHolderCleaner => Initializing the counter slot holder cleaner for topic: {} and recicle preiod: {}", topic, reciclePeriod);
        ConcurrentHashMap<AggregationCounterKey, AtomicLong> olderSlot = null;
        while(keepRunning){
            try {
                Thread.sleep(reciclePeriod);
                this.flush();
            } catch (InterruptedException e) {
                logger.error("There has been an interrupted exception for the counter slot holder cleaner in topic: {}", topic, e);
            }
        }
    }

    public void shutdown(){
        keepRunning=false;
        /* Clean all the holder size */
        while(holder.size() > 0){
            if (!this.flush()){
                // try to reflush it 3 times.
                logger.info("Flusing failed, waiting 1 second and trying again for 3 times.");
                for (int i=0;i<3;i++){
                    try {
                        logger.info("Flusing failed for {} time, waiting 1 second and trying again.", i);
                        if (!this.flush()){
                            Thread.sleep(1000);
                        }
                    } catch (InterruptedException e) {
                        logger.error("There was an interrupted exception", e);
                    }
                }
            }
        }
    }

    public boolean flush(){
        // If the olderSlot is null, it means we need to pick up a new slot to persist.
        boolean persisted = false;
        Integer slotKeyInMinute = holder.getOlderSlotKey();
        /* If there is at least one slot, we try to persist it */
        if (slotKeyInMinute != null){
            ConcurrentHashMap<AggregationCounterKey, AtomicLong> olderSlot = holder.removeSlot(slotKeyInMinute);
            Timer.Context time = null;
            try {
                // Persist the data in cassandra.
                time = this.processingTime.time();
                repository.persist(topic, slotKeyInMinute, olderSlot);
                persisted = true;
            } catch (CounterRepositoryException e) {
                logger.error("Error while trying to store slot: {} and topic : {} in cassandra.", topic, e);
                errorMeter.mark();
            } finally{
                if (time != null){
                    time.stop();
                }
            }
        }
        return persisted;
    }

}
