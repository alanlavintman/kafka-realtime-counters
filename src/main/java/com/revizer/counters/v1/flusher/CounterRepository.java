package com.revizer.counters.v1.flusher;

import com.revizer.counters.v1.counters.metadata.AggregationCounterKey;
import org.apache.commons.configuration.Configuration;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by alanl on 11/13/14.
 */
public interface CounterRepository {

    void initialize(Configuration configuration);

    void persist(String topic, Integer slotKeyInMinute, ConcurrentHashMap<AggregationCounterKey, AtomicLong> olderSlot) throws CounterRepositoryException;

}
