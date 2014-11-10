package com.revizer.counters.services.counting.model;

import org.codehaus.jackson.JsonNode;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by alanl on 11/10/14.
 */
public class Counter {

    private String name;
    private String[] fields;
    private ConcurrentHashMap<String,AtomicLong> count = new ConcurrentHashMap<String, AtomicLong>(10);

    public Counter(String name, String[] fields) {
        this.name = name;
        this.fields = fields;
    }

    public void inc(JsonNode node){

    }

    public long flush(){
        return count
    }

}
