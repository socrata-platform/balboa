package com.socrata.balboa.jms;

import com.socrata.balboa.jms.activemq.ActiveMQReceiver;
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

        log.info("Receivers starting, awaiting messages.");
        DataStore ds = DefaultDataStoreFactory.get();
        ActiveMQReceiver receiver = new ActiveMQReceiver(servers, channel, threads, ds);
        new WatchDog().watchAndWait(receiver, ds);
    }
}
