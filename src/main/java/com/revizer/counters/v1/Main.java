package com.revizer.counters.v1;

import com.revizer.counters.utils.ConfigurationParser;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Created by alanl on 12/4/14.
 */
public class Main {

    private static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        ConfigurationParser.printLine(logger);
        logger.info("   Starting Revizer real time counting system.                                 ");
        ConfigurationParser.printLine(logger);

        try {

            Configuration configuration = new PropertiesConfiguration("rt.properties");
            printProperties(configuration);

            CounterContext context = CounterContextConfiguration.build(configuration);

            final CountingSystem system = new CountingSystem(context);

            system.start();

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    system.stop();
                }
            });

        } catch (ConfigurationException e) {
            logger.error("There was an error while trying to initialize the counting system.", e);
        }

    }

    private static void printProperties(Configuration configuration) {
        ConfigurationParser.printLine(logger);
        logger.info("   Starting with properties:                                 ");
        ConfigurationParser.printLine(logger);
        Iterator<String> keys = configuration.getKeys();
        while(keys.hasNext()){
            String key = keys.next();
            String value = configuration.getString(key);
            logger.info("{}:{}",key, value);
        }
    }

}
