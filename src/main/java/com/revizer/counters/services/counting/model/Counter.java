package com.revizer.counters.services.counting.model;

import com.google.common.base.Joiner;
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

    public long inc(JsonNode node){
        String key = composeKey(node);
        if (key != null){
            AtomicLong atomicLong = new AtomicLong();
            atomicLong = count.putIfAbsent(key, atomicLong);
            return atomicLong.incrementAndGet();
        }
        return -1;
    }

    public String composeKey(JsonNode node) {
        String composedKey = null;
        String[] fieldValues = new String[fields.length];
        int fieldCount = 0;
        for (int i =0;i<fields.length;i++){
            String fieldName = fields[i];
            if (node.has(fieldName)){
                JsonNode fieldNode = node.get(fieldName);
                if (!fieldNode.isNull() && fieldNode.isTextual()){
                    fieldValues[i] = fieldNode.getTextValue();
                    fieldCount++;
                }
            }
        }
        if (fieldCount != fields.length){
            return null;
        } else {
            return Joiner.on(".").join(fieldValues);
        }
    }

    public long flush(){
        return count
    }

}
