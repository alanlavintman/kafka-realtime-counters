package com.revizer.counters;

import com.revizer.counters.services.ServiceFactory;
import com.revizer.counters.services.metrics.MetricsService;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by alanl on 08/11/14.
 */
public class Main {

    private static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        logger.info("Initializing revizer real time counters service.");

        try {

            /* Initialize the basic 2 components of the system: configuration + metrics*/
            Configuration configuration = new PropertiesConfiguration("rt.properties");
            MetricsService metricsService = new MetricsService(configuration);

            final ServiceFactory serviceFactory = new ServiceFactory(configuration, metricsService);
            final CountingSystem countingSystem = new CountingSystem(serviceFactory);

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    countingSystem.stop();
                }
            });

            countingSystem.start();

        } catch (ConfigurationException e) {
            logger.error("There was an error while trying to initialize the counting system.", e);
        }

    }

}
