package com.revizer.counters.v2;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by alanl on 12/4/14.
 */
public class Main {

    private static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        try {

            Configuration configuration = new PropertiesConfiguration("rt.properties");

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
}
