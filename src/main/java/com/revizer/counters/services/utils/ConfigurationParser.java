package com.revizer.counters.services.utils;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.primitives.Ints;
import com.revizer.counters.services.counting.model.AggregationCounter;
import org.apache.commons.configuration.Configuration;

import java.util.*;

/**
 * Created by alanl on 11/10/14.
 */
public class ConfigurationParser {

    private static String COUNTERS_CONFIGURATION_KEYS_STARTS_WITH="counters.counter.";
    private static String DEFAULT_NOW_FIELD="now";


    public static Map<String, Integer> getTopicAndNumOfStreams(Configuration configuration){
        String[] topicsSplit = configuration.getStringArray("streaming.kafka.topics");
        Map<String, Integer> topicStreamMap = new HashMap<String, Integer>();
        for (String topicSplit : topicsSplit) {
            List<String> topicAndStreams = Splitter.on(":").trimResults().omitEmptyStrings().splitToList(topicSplit);
            String topicName = topicAndStreams.get(0);
            int numOfStreams = 1;
            if (topicAndStreams.size() == 2){
                String numOfStreamsString = topicAndStreams.get(1);
                Preconditions.checkNotNull(Ints.tryParse(numOfStreamsString), "The number of streams arguments is not valid");
                numOfStreams = Ints.tryParse(numOfStreamsString);
            }
            topicStreamMap.put(topicName,numOfStreams);
        }
        return topicStreamMap;
    }

    public static Map<String, String> getTopicNowField(Configuration configuration){
        Map<String, Integer> topicAndNumOfStreams = getTopicAndNumOfStreams(configuration);
        Map<String, String> topicMapField = new HashMap<String, String>();
        for (String topic : topicAndNumOfStreams.keySet()) {
            String nowFieldKey = "counters.topic.timefield." + topic;
            String nowField = configuration.getString(nowFieldKey, DEFAULT_NOW_FIELD);
            topicMapField.put(topic, nowField);
        }
        return topicMapField;
    }

    public static List<String> getKeysThatStartsWith(Configuration configuration, String startsWith){
        List<String> returnKeys = new ArrayList<String>();
        Iterator<String> keys = configuration.getKeys();
        while(keys.hasNext()){
            String key = keys.next();
            if (key.startsWith(startsWith)){
                returnKeys.add(key);
            }
        }
        return returnKeys;
    }

    public static Map<String, List<AggregationCounter>> getCountersByTopic(Configuration configuration) {
        Map<String, List<AggregationCounter>> counters = new HashMap<String, List<AggregationCounter>>();
        Map<String, Integer> topicAndNumOfStreams = getTopicAndNumOfStreams(configuration);
        for (String topic : topicAndNumOfStreams.keySet()) {
            List<AggregationCounter> counterList = new ArrayList<AggregationCounter>();
            String keyRetrival = COUNTERS_CONFIGURATION_KEYS_STARTS_WITH.concat(topic).concat(".");
            List<String> countersKeys = getKeysThatStartsWith(configuration, keyRetrival);
            for (String counterKey : countersKeys) {
                String counterName = counterKey.substring(keyRetrival.length());
                String[] fields = configuration.getStringArray(counterKey);
                AggregationCounter counter = new AggregationCounter(counterName, fields);
                counterList.add(counter);
            }
            counters.put(topic,counterList);
        }
        return counters;
    }
}
