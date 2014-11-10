package com.revizer.counters.services.counting;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by alanl on 11/10/14.
 */
public class Counter {

    private String name;
    private ConcurrentHashMap<String, AtomicCounter> counterHolderMap = new ConcurrentHashMap<String, AtomicCounter>(100);

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ConcurrentHashMap<String, AtomicCounter> getCounterHolderMap() {
        return counterHolderMap;
    }

    public void setCounterHolderMap(ConcurrentHashMap<String, AtomicCounter> counterHolderMap) {
        this.counterHolderMap = counterHolderMap;
    }

}
