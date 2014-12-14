package com.revizer.counters;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.codehaus.jackson.JsonNode;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by alanl on 11/12/14.
 */
public class JsonCounterServiceTest extends BaseCounterTest {

//    @Test
//    public void testSimpleProcess(){
//
//        DateTime jodaTime = new DateTime();
//        long now = jodaTime.getMillis() / 1000;
//        Integer minuteOfDay = jodaTime.getMinuteOfDay();
//
//        Configuration configuration = prepareCountryCounterConfiguration();
//        MetricsService metricsService = new MetricsService(configuration);
//        JsonCounterService counterService = new JsonCounterService(configuration, metricsService);
//        JsonNode node = createTipicalInjectEvent(now);
//        counterService.process("inject",node);
//
//        /* Assert that the counterService has only one CounterSlotHolder since it has only one topic. */
//        Map<String, CounterSlotHolder> countersSlotHolder = counterService.getCountersSlotHolder();
//        Assert.assertEquals(countersSlotHolder.keySet().size(), 1);
//
//        /* Assert that the counterService has only one CounterSlotHolder since it has only one topic. */
//        CounterSlotHolder injectSlotHolder = countersSlotHolder.get("inject");
//        ConcurrentSkipListMap<Integer, ConcurrentHashMap<AggregationCounterKey, AtomicLong>> slotHolder = injectSlotHolder.getSlotHolder();
//        Assert.assertEquals(slotHolder.keySet().size(), 1);
//
//        ConcurrentHashMap<AggregationCounterKey, AtomicLong> aggregationCounterKeyAtomicLongConcurrentHashMap = slotHolder.get(minuteOfDay);
//        Assert.assertEquals(aggregationCounterKeyAtomicLongConcurrentHashMap.size(), 1);
//        for (AggregationCounterKey aggregationCounterKey : aggregationCounterKeyAtomicLongConcurrentHashMap.keySet()) {
//            Assert.assertEquals(aggregationCounterKey.getCounterKey(),"inject.IL");
//            Assert.assertEquals(aggregationCounterKey.getDate(),AggregationCounter.formatter.format(new Date(jodaTime.getMillis())));
//            AtomicLong counter = aggregationCounterKeyAtomicLongConcurrentHashMap.get(aggregationCounterKey);
//            Assert.assertEquals(counter.get(),1);
//        }
//    }
//
//    /**
//     * In order to test counter contention.
//     */
//    @Test
//    public void testSimpleAggregationProcessWithMultipleThreadsAndOneKey(){
//
//        DateTime jodaTime = new DateTime();
//        long now = jodaTime.getMillis() / 1000;
//        Integer minuteOfDay = jodaTime.getMinuteOfDay();
//
//        Configuration configuration = prepareCountryCounterConfiguration();
//        MetricsService metricsService = new MetricsService(configuration);
//        final JsonCounterService counterService = new JsonCounterService(configuration, metricsService);
//        final JsonNode node = createTipicalInjectEvent(now);
//
//        final int amountOfClients = 100;
//        final int amountOfEventsPerClient = 1000;
//        /* We want to test this contention against 100 clients.*/
//        ExecutorService executorService = Executors.newFixedThreadPool(amountOfClients);
//        for (int i=0;i<amountOfClients;i++){
//            executorService.execute(new Runnable() {
//                @Override
//                public void run() {
//                    for (int j=0;j<amountOfEventsPerClient;j++){
//                        counterService.process("inject",node);
//                    }
//                }
//            });
//        }
//
//        try {
//            executorService.shutdown();
//            executorService.awaitTermination(1, TimeUnit.HOURS);
//        } catch (InterruptedException e) {
//            Assert.fail("Failed waiting for termination !!");
//        }
//
//        /* Assert that the counterService has only one CounterSlotHolder since it has only one topic. */
//        Map<String, CounterSlotHolder> countersSlotHolder = counterService.getCountersSlotHolder();
//        Assert.assertEquals(countersSlotHolder.keySet().size(), 1);
//
//        /* Assert that the counterService has only one CounterSlotHolder since it has only one topic. */
//        CounterSlotHolder injectSlotHolder = countersSlotHolder.get("inject");
//        ConcurrentSkipListMap<Integer, ConcurrentHashMap<AggregationCounterKey, AtomicLong>> slotHolder = injectSlotHolder.getSlotHolder();
//        Assert.assertEquals(slotHolder.keySet().size(), 1);
//
//        ConcurrentHashMap<AggregationCounterKey, AtomicLong> aggregationCounterKeyAtomicLongConcurrentHashMap = slotHolder.get(minuteOfDay);
//        Assert.assertEquals(aggregationCounterKeyAtomicLongConcurrentHashMap.size(), 1);
//        for (AggregationCounterKey aggregationCounterKey : aggregationCounterKeyAtomicLongConcurrentHashMap.keySet()) {
//            Assert.assertEquals(aggregationCounterKey.getCounterKey(),"inject.IL");
//            Assert.assertEquals(aggregationCounterKey.getDate(),AggregationCounter.formatter.format(new Date(jodaTime.getMillis())));
//            AtomicLong counter = aggregationCounterKeyAtomicLongConcurrentHashMap.get(aggregationCounterKey);
//            Assert.assertEquals(counter.get(),amountOfClients*amountOfEventsPerClient);
//        }
//    }
//
//    /**
//     * In order to test counter contention.
//     */
//    @Test
//    public void testSimpleAggregationProcessWithMultipleThreadsAndMultipleKeysAndSameTime(){
//
//        DateTime jodaTime = new DateTime();
//        long now = jodaTime.getMillis() / 1000;
//        Integer minuteOfDay = jodaTime.getMinuteOfDay();
//
//        Configuration configuration = prepareAffiliateCounterConfiguration();
//        MetricsService metricsService = new MetricsService(configuration);
//        final JsonCounterService counterService = new JsonCounterService(configuration, metricsService);
//
//        final int amountOfClients = 10;
//        int[] outputFromClients = new int[amountOfClients];
////        for (int i=0;i<amountOfClients;i++){
////            outputFromClients[i] = 0;
////        }
//        final int amountOfEventsPerClient = 1000;
//        /* We want to test this contention against 100 clients.*/
//        ExecutorService executorService = Executors.newFixedThreadPool(amountOfClients);
//        for (int i=0;i<amountOfClients;i++){
//            final int threadNumber = i;
//            executorService.execute(new Runnable() {
//                @Override
//                public void run() {
//
//                    DateTime jodaTime = new DateTime();
//                    long now = jodaTime.getMillis() / 1000;
//                    long threadNow = now-(60*threadNumber); // Each thread needs to write to a different slot of time. We will use the tread number to simulate "a minute".
//                    int threadAffiliate = threadNumber; // Each thread needs to write to a different affiliate. We use the thread number to simulate the affiliate number.
//                    JsonNode node = createTipicalInjectEventWithAffiliateParameter(now, threadAffiliate);
//                    for (int j=0;j<amountOfEventsPerClient;j++){
//                        counterService.process("inject",node);
//                    }
//                }
//            });
//        }
//
//        try {
//            executorService.shutdown();
//            executorService.awaitTermination(1, TimeUnit.HOURS);
//        } catch (InterruptedException e) {
//            Assert.fail("Failed waiting for termination !!");
//        }
//
//        /* Assert that the counterService has only one CounterSlotHolder since it has only one topic. */
//        Map<String, CounterSlotHolder> countersSlotHolder = counterService.getCountersSlotHolder();
//        Assert.assertEquals(countersSlotHolder.keySet().size(), 1);
//
//        /* Assert that the counterService has only one CounterSlotHolder since it has only one topic. */
//        CounterSlotHolder injectSlotHolder = countersSlotHolder.get("inject");
//        ConcurrentSkipListMap<Integer, ConcurrentHashMap<AggregationCounterKey, AtomicLong>> slotHolder = injectSlotHolder.getSlotHolder();
//        Assert.assertEquals(slotHolder.keySet().size(), 1);
//
//        ConcurrentHashMap<AggregationCounterKey, AtomicLong> aggregationCounterKeyAtomicLongConcurrentHashMap = slotHolder.get(minuteOfDay);
//        Assert.assertEquals(aggregationCounterKeyAtomicLongConcurrentHashMap.size(), amountOfClients);
//        for (AggregationCounterKey aggregationCounterKey : aggregationCounterKeyAtomicLongConcurrentHashMap.keySet()) {
//            String[] split = aggregationCounterKey.getCounterKey().split("\\."); // get the second part of it.
//            outputFromClients[Integer.valueOf(split[1])]++;
//            Assert.assertEquals(aggregationCounterKey.getDate(),AggregationCounter.formatter.format(new Date(jodaTime.getMillis())));
//            AtomicLong counter = aggregationCounterKeyAtomicLongConcurrentHashMap.get(aggregationCounterKey);
//            Assert.assertEquals(counter.get(),amountOfEventsPerClient);
//        }
//
//        /* Assert that all the elements of the filled in array has only one value in the counter. */
//        for (int outputFromClient : outputFromClients) {
//            Assert.assertEquals(outputFromClient, 1);
//        }
//    }
//
//    public JsonNode createTipicalInjectEvent(Long now){
//
//        Date machineDate = new Date(now*1000);
//        String expectedDateField = AggregationCounter.formatter.format(machineDate);
//        String aff = "1234";
//        String subaff = "5678";
//        String country="IL";
//        String browser="chrome";
//        String[] revmods = new String[]{"1","2","3"};
//        String event = this.createEvent(now, aff, subaff,country, browser, revmods);
//        JsonNode eventJson = null;
//        try {
//            eventJson = mapper.readTree(event);
//        } catch (IOException e) {
//            Assert.fail("There was an error while trying to unserialize the event");
//        }
//        return eventJson;
//    }
//
//    public JsonNode createTipicalInjectEventWithAffiliateParameter(Long now, int affiliateId){
//        Date machineDate = new Date(now*1000);
//        String expectedDateField = AggregationCounter.formatter.format(machineDate);
//        String aff = String.valueOf(affiliateId);
//        String subaff = "5678";
//        String country="IL";
//        String browser="chrome";
//        String[] revmods = new String[]{"1","2","3"};
//        String event = this.createEvent(now, aff, subaff,country, browser, revmods);
//        JsonNode eventJson = null;
//        try {
//            eventJson = mapper.readTree(event);
//        } catch (IOException e) {
//            Assert.fail("There was an error while trying to unserialize the event");
//        }
//        return eventJson;
//    }
//
//
//    public Configuration prepareCountryCounterConfiguration(){
//        Configuration configuration = new BaseConfiguration();
//        configuration.addProperty("streaming.kafka.topics","inject:24");
//        configuration.addProperty("counters.topic.timefield.inject","now");
//        configuration.addProperty("counters.counter.inject.country","country_code");
//        return configuration;
//    }
//
//    public Configuration prepareAffiliateCounterConfiguration(){
//        Configuration configuration = new BaseConfiguration();
//        configuration.addProperty("streaming.kafka.topics","inject:24");
//        configuration.addProperty("counters.topic.timefield.inject","now");
//        configuration.addProperty("counters.counter.inject.affid","affid");
//        return configuration;
//    }

}
