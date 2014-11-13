package com.revizer.counters;

import com.revizer.counters.services.ServiceFactory;
import com.revizer.counters.services.metrics.MetricsService;
import org.apache.commons.configuration.Configuration;

/**
 * Created by alanl on 11/13/14.
 */
public class SystemTest {

    public void SimpleTest(){

        //1)  Start Embedded Kafka ! http://pannoniancoder.blogspot.co.il/2014/08/embedded-kafka-and-zookeeper-for-unit.html

        //2) Start Embedded Cassandra.
        //

        //3) Build the configuration you want
        /*
        *
        *  public Configuration prepareCountryCounterConfiguration(){
        Configuration configuration = new BaseConfiguration();
        configuration.addProperty("streaming.kafka.topics","inject:24");
        configuration.addProperty("counters.topic.timefield.inject","now");
        configuration.addProperty("counters.counter.inject.country","country_code");
        return configuration;
        }
        IMPORTANT: Define some counters that you will use later for double check the data.
        * */
        Configuration configuration = null;
        MetricsService metrics = new MetricsService(configuration);

        // 4) Build a countingSystem
        ServiceFactory service = new ServiceFactory(configuration, metrics);
        CountingSystem system = new CountingSystem(service);
        // 5) Start the counting System
        system.start();

        // 6) Send events to kafka as many as you want.
        // use a method similar to BaseCounterTest.createEvent You give him values, and it gives you back a json string.

        //7) Get the data from the embedded cassandra. and assert the values.



    }

}
