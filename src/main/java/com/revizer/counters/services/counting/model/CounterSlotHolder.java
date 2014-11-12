package com.revizer.counters.services.counting.model;

import com.codahale.metrics.Meter;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by alanl on 11/11/14.
 */
public class CounterSlotHolder {

    private ConcurrentSkipListMap<Integer, ConcurrentHashMap<AggregationCounterKey, AtomicLong>> slotHolder = new ConcurrentSkipListMap<Integer, ConcurrentHashMap<AggregationCounterKey, AtomicLong>>();

    public ConcurrentSkipListMap<Integer, ConcurrentHashMap<AggregationCounterKey, AtomicLong>> getSlotHolder() {
        return slotHolder;
    }

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

    public ConcurrentHashMap<AggregationCounterKey, AtomicLong> cleanOlderSlot(){
        /* Start by removing the older 10 items. */
        Integer firstKey = slotHolder.firstKey();
        ConcurrentHashMap<AggregationCounterKey, AtomicLong> remove = slotHolder.remove(firstKey);
        return remove;
    }

}