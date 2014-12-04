package com.revizer.counters.v2;

import com.revizer.counters.CountingSystem;
import com.revizer.counters.services.ServiceFactory;
import com.revizer.counters.services.metrics.MetricsService;
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

            /* Initialize the basic 2 components of the system: configuration + metrics*/
            // Check if there is an argument, if there is, use that instead of the hardcoded load from the classpath.
            Configuration configuration = new PropertiesConfiguration("rt.properties");


        } catch (ConfigurationException e) {
            logger.error("There was an error while trying to initialize the counting system.", e);
        }

    }
}
