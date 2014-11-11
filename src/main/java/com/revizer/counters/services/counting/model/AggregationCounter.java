package com.revizer.counters.services.counting.model;

import com.google.common.base.Joiner;
import org.codehaus.jackson.JsonNode;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by alanl on 11/10/14.
 */
public class AggregationCounter {

    private static SimpleDateFormat format = new SimpleDateFormat("YYYYMMDD");

    private String name;
    private String[] fields;

    public AggregationCounter(String name, String[] fields) {
        this.name = name;
        this.fields = fields;
    }

    public AggregationCounterKey getCounterKey(Long now, JsonNode node) {
        AggregationCounterKey aggregationCounterKey = new AggregationCounterKey();
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
                } else {
                    fieldValues[i] = "none";
                }
            }
        }
        /* Compose the final key */
        /* The read from cassandra side should be done with RangeReader to bring the data in a paged way. */
        Date date = new Date(now*1000);
        aggregationCounterKey.setCounterKey(Joiner.on(".").join(fieldValues));
        aggregationCounterKey.setDate(format.format(date));
        return aggregationCounterKey;
    }

    public long flush(){
        return 0;
    }



}
