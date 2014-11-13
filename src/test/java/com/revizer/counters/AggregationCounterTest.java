package com.revizer.counters;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Joiner;
import com.revizer.counters.services.counting.model.AggregationCounter;
import com.revizer.counters.services.counting.model.AggregationCounterKey;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * Created by alanl on 11/12/14.
 */
public class AggregationCounterTest extends BaseCounterTest{

    @Test
    public void testGetCounterKeyWithPerfectScenario() {
        /* Data preparation */
        String topic = "inject";
        String name = "aff,subaff,country";
        String[] fields = new String[]{"affid","subaffid","country_code"};

        long now = 1415788921; // 2014-11-12 10:42:01 in UTC!!!
        Date machineDate = new Date(now*1000);
        String expectedDateField = AggregationCounter.formatter.format(machineDate);
        String aff = "1234";
        String subaff = "5678";
        String country="IL";
        String browser="chrome";
        String[] revmods = new String[]{"1","2","3"};
        String event = this.createEvent(now, aff, subaff,country, browser, revmods);
        JsonNode eventJson = null;
        try {
            eventJson = mapper.readTree(event);
        } catch (IOException e) {
            Assert.fail("There was an error while trying to unserialize the event");
        }

        AggregationCounter counter = new AggregationCounter(topic, name, fields);
        List<AggregationCounterKey> counterKey = counter.getCounterKey(now, eventJson);
        Assert.assertEquals(counterKey.size(),1);
        AggregationCounterKey key = counterKey.get(0);
        String outputKey = Joiner.on(".").join(topic,aff,subaff,country);
        Assert.assertEquals(key.getCounterKey(),outputKey);
        Assert.assertEquals(key.getDate(),expectedDateField);
    }


    @Test
    public void benchmarkSamplingAggregationCounterGetKey() {
        MetricRegistry registry = new MetricRegistry();
        /* Data preparation */
        String topic = "inject";
        String name = "aff,subaff,country";
        String[] fields = new String[]{"affid","subaffid","country_code","browser"};

        long now = 1415788921; // 2014-11-12 10:42:01 in UTC!!!
        Date machineDate = new Date(now*1000);
        String expectedDateField = AggregationCounter.formatter.format(machineDate);
        String aff = "1234";
        String subaff = "5678";
        String country="IL";
        String browser="chrome";
        String[] revmods = new String[]{"1","2","3"};
        String event = this.createEvent(now, aff, subaff,country, browser, revmods);
        JsonNode eventJson = null;
        try {
            eventJson = mapper.readTree(event);
        } catch (IOException e) {
            Assert.fail("There was an error while trying to unserialize the event");
        }

        long total = 0;
        for (int sampleTimes=0;sampleTimes<100;sampleTimes++){
            long start = System.currentTimeMillis();
            for(int i=0;i<20;i++){
                AggregationCounter counter = new AggregationCounter(topic, name, fields);
                List<AggregationCounterKey> counterKey = counter.getCounterKey(now, eventJson);
                Assert.assertEquals(counterKey.size(),1);
            }
            long end = System.currentTimeMillis();
            total = end-start;
        }

        long value = total/100;
        System.out.println("Average: " + String.valueOf(value));
//        Assert.assertTrue(value <= 1L); // Assert that this process should take less than 1 millisecond.


        long start = System.currentTimeMillis();
        for(int i=0;i<100;i++){
            AggregationCounter counter = new AggregationCounter(topic, name, fields);
            List<AggregationCounterKey> counterKey = counter.getCounterKey(now, eventJson);
            Assert.assertEquals(counterKey.size(),1);
        }
        long end = System.currentTimeMillis();
        value = end-start;
        System.out.println("Testing 100 aggregations, time spent: " + String.valueOf(value));
//        Assert.assertTrue(value <= 10L); // Assert that this process should take less than 1 millisecond.



        start = System.currentTimeMillis();
        for(int i=0;i<500;i++){
            AggregationCounter counter = new AggregationCounter(topic, name, fields);
            List<AggregationCounterKey> counterKey = counter.getCounterKey(now, eventJson);
            Assert.assertEquals(counterKey.size(),1);
        }
        end = System.currentTimeMillis();
        value = end-start;
        System.out.println("Testing 500 aggregations, time spent: " + String.valueOf(value));
//        Assert.assertTrue(value <= 20L); // Assert that this process should take less than 1 millisecond.


        start = System.currentTimeMillis();
        for(int i=0;i<1000;i++){
            AggregationCounter counter = new AggregationCounter(topic, name, fields);
            List<AggregationCounterKey> counterKey = counter.getCounterKey(now, eventJson);
            Assert.assertEquals(counterKey.size(),1);
        }
        end = System.currentTimeMillis();
        value = end-start;
        System.out.println("Testing 1000 aggregations, time spent: " + String.valueOf(value));
//        Assert.assertTrue(value <= 30L); // Assert that this process should take less than 1 millisecond.


    }


}