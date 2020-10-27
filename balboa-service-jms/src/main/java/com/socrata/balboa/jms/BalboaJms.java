package com.socrata.balboa.jms;

import com.socrata.balboa.jms.activemq.ConsumerPool;
import com.socrata.balboa.metrics.WatchDog;
import com.socrata.balboa.metrics.config.Keys;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DefaultDataStoreFactory;
import com.socrata.balboa.util.LoggingConfigurator;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BalboaJms {
    public static void main(String[] args) throws Exception {
        Config config = ConfigFactory.load();
        LoggingConfigurator.configureLogging(config);

        Logger log = LoggerFactory.getLogger(BalboaJms.class);

        log.info("Loading with config: " + config.root().render(ConfigRenderOptions.concise()));

        Integer threads = config.getInt(Keys.JMSActiveMQThreadsPerServer());
        String[] servers = config.getString(Keys.JMSActiveMQServer()).split(",");
        String channel = config.getString(Keys.JMSActiveMQQueue());
        Integer metricCountLimit = config.getInt("balboa.metric-count-limit");
        Boolean stopWrites = config.getBoolean("balboa.stop-writes");

        log.info("Receivers starting, awaiting messages.");
        DataStore ds = DefaultDataStoreFactory.get();
        ConsumerPool consumers = new ConsumerPool(servers, channel, threads, ds, metricCountLimit, stopWrites);
        new WatchDog().watchAndWait(consumers, ds);
    }
}
