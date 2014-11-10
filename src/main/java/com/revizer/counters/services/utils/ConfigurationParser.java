package com.revizer.counters.services.utils;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.primitives.Ints;
import org.apache.commons.configuration.Configuration;

import java.util.*;

/**
 * Created by alanl on 11/10/14.
 */
public class ConfigurationParser {

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
        String startsWith = "counters.topic.timefield.";
        Map<String, String> topicMapField = new HashMap<String, String>();
        List<String> keys = getKeysThatStartsWith(configuration, startsWith);
        for (String key : keys) {
            String nowField = configuration.getString(key);
            String topic = key.substring(startsWith.length());
            topicMapField.put(topic, nowField) ;
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

}
