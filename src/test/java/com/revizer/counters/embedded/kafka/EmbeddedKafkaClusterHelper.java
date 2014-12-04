package com.revizer.counters.embedded.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by alanl on 12/4/14.
 */
public class EmbeddedKafkaClusterHelper {

    private static int DEFAULT_ZK_PORT = 2181;
    public static int zkPort;
    public static EmbeddedZookeeper embeddedZookeeper;
    public static EmbeddedKafkaCluster embeddedKafkaCluster;
    private static Logger logger = LoggerFactory.getLogger(EmbeddedKafkaClusterHelper.class);

    public static void startEmbeddedKafkaCluster() throws IOException {
        if (EmbeddedKafkaClusterHelper.zkPort == 0){
            EmbeddedKafkaClusterHelper.zkPort = EmbeddedKafkaClusterHelper.DEFAULT_ZK_PORT;
        }
        embeddedZookeeper = new EmbeddedZookeeper(EmbeddedKafkaClusterHelper.zkPort);
        List<Integer> kafkaPorts = new ArrayList<Integer>();
        // -1 for any available port
        kafkaPorts.add(9092);
        embeddedKafkaCluster = new EmbeddedKafkaCluster(embeddedZookeeper.getConnection(), new Properties(), kafkaPorts);
        embeddedZookeeper.startup();
        logger.info("### Embedded Zookeeper connection: " + embeddedZookeeper.getConnection());
        embeddedKafkaCluster.startup();
        logger.info("### Embedded Kafka cluster broker list: " + embeddedKafkaCluster.getBrokerList());
    }

    public static void shutDownEmbeddedKafkaCluster(){
        embeddedKafkaCluster.shutdown();
        embeddedZookeeper.shutdown();
    }

}
