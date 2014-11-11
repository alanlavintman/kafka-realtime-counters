package com.revizer.counters.services.counting.model;

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

    private ConcurrentSkipListMap<Long, ConcurrentHashMap<AggregationCounterKey, AtomicLong>> slotHolder = new ConcurrentSkipListMap<Long, ConcurrentHashMap<AggregationCounterKey, AtomicLong>>();

    public CounterSlotHolder() {

    }

    public void inc(Long now, AggregationCounterKey key) {
        Long minuteOfDay = now/60;
        slotHolder.putIfAbsent(minuteOfDay, new ConcurrentHashMap<AggregationCounterKey, AtomicLong>());
        ConcurrentHashMap<AggregationCounterKey, AtomicLong> aggregationCounterKeyAtomicLongConcurrentHashMap = slotHolder.get(minuteOfDay);
        aggregationCounterKeyAtomicLongConcurrentHashMap.putIfAbsent(key, new AtomicLong());
        AtomicLong counter = aggregationCounterKeyAtomicLongConcurrentHashMap.get(key);
        counter.incrementAndGet();
    }

    public List<ConcurrentHashMap<AggregationCounterKey, AtomicLong>> pruneLastSlot(Long since){
        List<ConcurrentHashMap<AggregationCounterKey, AtomicLong>> listOfSlots = new ArrayList<ConcurrentHashMap<AggregationCounterKey, AtomicLong>>();
        /* Start by removing the */
        for (int i=0;i<10;i++){
            Long firstKey = slotHolder.firstKey();
            listOfSlots.add(slotHolder.remove(firstKey));
        }
        return listOfSlots;
    }

}
