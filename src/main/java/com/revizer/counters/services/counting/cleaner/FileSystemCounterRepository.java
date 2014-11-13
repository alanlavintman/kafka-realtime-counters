package com.revizer.counters.services.counting.cleaner;

import com.revizer.counters.services.counting.exceptions.CounterRepositoryException;
import com.revizer.counters.services.counting.model.AggregationCounterKey;
import org.apache.commons.configuration.Configuration;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by alanl on 11/13/14.
 */
public class FileSystemCounterRepository implements CounterRepository {

    @Override
    public void initialize(Configuration configuration) {

    }

    @Override
    public void persist(String topic, Integer slotKeyInMinute, ConcurrentHashMap<AggregationCounterKey, AtomicLong> olderSlot) throws CounterRepositoryException {

    }

}
