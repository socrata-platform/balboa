package com.socrata.balboa.jms;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BalboaJms {

    private static final Logger log = LoggerFactory.getLogger(BalboaJms.class);

    public static void main(String[] args) throws Exception {
        Config config = ConfigFactory.load();

        log.info("Loading with config: " + config.root().render(ConfigRenderOptions.concise()));

        Integer threads = config.getInt(Keys.JMSActiveMQThreadsPerServer());
        String[] servers = config.getString(Keys.JMSActiveMQServer()).split(",");
        String channel = config.getString(Keys.JMSActiveMQQueue());

        LoggingConfigurator.configureLogging(config);

        log.info("Receivers starting, awaiting messages.");
        DataStore ds = DefaultDataStoreFactory.get();
        ActiveMQReceiver receiver = new ActiveMQReceiver(servers, channel, threads, ds);
        new WatchDog().watchAndWait(receiver, ds);
    }
}
