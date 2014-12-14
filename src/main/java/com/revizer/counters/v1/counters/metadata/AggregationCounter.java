package com.revizer.counters.v1.counters.metadata;

import com.google.common.base.Joiner;
import org.codehaus.jackson.JsonNode;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by alanl on 11/10/14.
 */
public class AggregationCounter {

    public static SimpleDateFormat formatter = new SimpleDateFormat("YYYYMMdd");

    private String topic;
    private String name;
    private String[] fields;
    private Joiner joiner;

    public AggregationCounter(String topic, String name, String[] fields) {
        this.topic = topic;
        this.name = name;
        this.fields = fields;
        this.joiner = Joiner.on(".");
    }

    /**
     * Method in charge of composing the cassandra key according to the aggregation configuration.
     * If the aggregation has 3 fields, lets say: affid, subaffid, country. The output key that will be returned
     * will be the values of those fields in the node passed by parameter concatenated by the character "."
     * @param eventNow The ocurrance of the event in seconds..
     * @param node The node containing the rest of the information for further lookup.
     * @return A reference to a {@link java.util.List< AggregationCounterKey >}, The result is a list because we want to offer
     * the flexibility to do two things:
     *
     * If i have a counter:
     * aff,subaff,country
     * And i have an event now of: 12345678412 (Lets say its DAY 20141101)
     * And i have an event:
     * {aff:123,subaff:3456, country: IL}
     *      *
     * The return value of this method call is and instance of AggregationCounterKey
     * with fields:
     * counterKey: 123.3456.IL
     * DAY: 20141101
     *
     *
     * TODO: Very important, support array columns
     * If i have a counter: aff,subaff,revmods
     * And i have an event now of: 12345678412 (Lets say its DAY 20141101)
     * And i have an event:
     * {aff:123,subaff:3456, revmods: ['a','b']}
     *
     * The return value of this method call is and instance of a list of 2 AggregationCounterKey
     * 1)
     * counterKey: 123.3456.a
     * DAY: 20141101
     *
     * 2)
     * counterKey: 123.3456.b
     * DAY: 20141101
     *
     * 1) Support hirerachy counters, if you pass affid, subaffid, country
     * i would like to increment:
     * affid
     * affid.subaffid
     * affid.subaffid.country
     * 2) Support for array columns. If your field is an array, then we need to compute as many counter keys as the array.
     */
    public List<AggregationCounterKey> getCounterKey(Long eventNow, JsonNode node) {
        List<AggregationCounterKey> aggregationKeys = new ArrayList<AggregationCounterKey>();
        AggregationCounterKey aggregationCounterKey = new AggregationCounterKey();
        String[] fieldValues = new String[fields.length+1];
        fieldValues[0] = this.topic;
        //TODO: Create multiple AggregationCounterKey in case we find an array field.
        //TODO: Support inner keys so we can process aggregations when the fieldNode a dictionary.
        for (int i =0;i<fields.length;i++){
            String fieldName = fields[i];
            if (node.has(fieldName)) {
                JsonNode fieldNode = node.get(fieldName);
                if (!fieldNode.isNull()) {
                    if (fieldNode.isTextual() || fieldNode.isNumber()){
                        //TODO: Escape the text so it wont contain any bad values for:
                        // cassandra: It does not accept keys with characters:
                        // Us: We concatenate the keys with "." so if the value contains that character, it will brake the lookup.
                        fieldValues[i+1] = fieldNode.asText();
                    } else {
                        fieldValues[i+1] = "badformat";
                    }
                } else {
                    fieldValues[i+1] = "novalue";
                }
            } else {
                fieldValues[i+1] = "nofield";
            }
        }
        /* Compose the final key */
        /* The read from cassandra side should be done with RangeReader to bring the data in a paged way. */
        Date date = new Date(eventNow*1000);

        aggregationCounterKey.setCounterKey(joiner.join(fieldValues));
        aggregationCounterKey.setDate(formatter.format(date));
        aggregationKeys.add(aggregationCounterKey);
        return aggregationKeys;
    }

    public long flush(){
        return 0;
    }



}
