package com.revizer.counters;

import com.revizer.counters.services.CounterService;
import com.revizer.counters.services.MetricsService;
import com.revizer.counters.services.StreamService;
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

        Configuration configuration = null;
        StreamService streamService = null;
        CounterService counterService = null;
        MetricsService metricsService = null;

        try {
            /* Low level dependencies, configuration and metrics. */
            configuration = new PropertiesConfiguration("rt.properties");
            metricsService = new MetricsService(configuration);

            /* Main business services, the one who brings the stream, and the one who counts and flush. */
            counterService = new CounterService(configuration, metricsService);
            streamService = new StreamService(configuration, metricsService, counterService);

            streamService.start();


            final StreamService finalStreamService = streamService;
            Runtime.getRuntime().addShutdownHook(new Thread() {
                    @Override
                    public void run() {
                        finalStreamService.stop();
                    }
                });


        } catch (ConfigurationException e) {
            logger.error("There was an error while trying to make an instance of ConfigurationService", e);
        }


    }

}
