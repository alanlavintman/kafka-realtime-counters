package com.revizer.counters.v2.counters;

import com.revizer.counters.v2.counters.metadata.AggregationCounterKey;
import org.joda.time.DateTime;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is the timeline !
 * Created by alanl on 11/11/14.
 */
public class CounterSlotHolder {

    private ConcurrentSkipListMap<Integer, ConcurrentHashMap<AggregationCounterKey, AtomicLong>> slotHolder = new ConcurrentSkipListMap<Integer, ConcurrentHashMap<AggregationCounterKey, AtomicLong>>();

    public ConcurrentSkipListMap<Integer, ConcurrentHashMap<AggregationCounterKey, AtomicLong>> getSlotHolder() {
        return slotHolder;
    }

    /**
     * Default constructor to build {@link com.revizer.counters.v2.counters.CounterSlotHolder} instances.
     */
    public CounterSlotHolder() {

    }

    /**
     *
     * @param eventNow
     * @param key
     */
    public void inc(Long eventNow, AggregationCounterKey key) {
        DateTime jodaTime = new DateTime(eventNow*1000);
        int minuteOfDay = jodaTime.getMinuteOfDay();
        slotHolder.putIfAbsent(minuteOfDay, new ConcurrentHashMap<AggregationCounterKey, AtomicLong>());
        ConcurrentHashMap<AggregationCounterKey, AtomicLong> aggregationCounterKeyAtomicLongConcurrentHashMap = slotHolder.get(minuteOfDay);
        aggregationCounterKeyAtomicLongConcurrentHashMap.putIfAbsent(key, new AtomicLong());
        AtomicLong counter = aggregationCounterKeyAtomicLongConcurrentHashMap.get(key);
        counter.incrementAndGet();
    }

    public Integer getOlderSlotKey(){
        if (slotHolder.size() > 0){
            return slotHolder.firstKey();
        } else {
            return -1;
        }
    }

    public int size(){
        return slotHolder.size();
    }

    public ConcurrentHashMap<AggregationCounterKey, AtomicLong> removeSlot(Integer key){
        /* Start by removing the older item. */
        ConcurrentHashMap<AggregationCounterKey, AtomicLong> remove = slotHolder.remove(key);
        return remove;
    }

}