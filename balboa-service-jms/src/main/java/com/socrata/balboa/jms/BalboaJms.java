package com.socrata.balboa.jms;

import com.socrata.balboa.metrics.WatchDog;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DefaultDataStoreFactory;
import com.socrata.balboa.util.LoggingConfigurator;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BalboaJms {

    private static final Logger log = LoggerFactory.getLogger(BalboaJms.class);

    private static void usage() {
        System.err.println("java -jar balboa-jms [thread count] [activemq urls] [activemq channel]");
    }

    static List<String> parseServers(String[] args, Config conf) {
        String soTimeout = "soTimeout=" + conf.getInt("activemq.sotimeout");
        String writeTimeout = "soWriteTimeout=" + conf.getInt("activemq.sowritetimeout");
        List<String> servers = new ArrayList<>();
        for (String arg: args) {
            String[] srvs = arg.split(",+(?![^\\(]*\\))");
            for (String s:srvs) {
                if (!s.contains("?")) {
                    s += "?"+soTimeout;
                } else if (!s.contains("soTimeout=")) {
                    s += "&"+soTimeout;
                }
                if (!s.contains("soWriteTimeout=")) {
                    s+="&"+writeTimeout;
                }
                servers.add(s);
            }
        }
        return servers;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2)
        {
            usage();
            System.exit(1);
        }

        Config config = ConfigFactory.load();

        int threads = Integer.parseInt(args[0]);
        List<String> servers = parseServers(Arrays.copyOfRange(args, 1, args.length - 1), config);
        String channel = args[args.length - 1];

        LoggingConfigurator.configureLogging(config);

        log.info("Receivers starting, awaiting messages.");
        DataStore ds = DefaultDataStoreFactory.get();

        DataStore hawkular = new HawkularMetricStore(
                new URI("http://10.110.35.139:14996/"),
                "jdoe",
                "password",
                "secret-admin-token");

        List<DataStore> datastores = new ArrayList<DataStore>();
        datastores.add(ds);
        datastores.add(hawkular);
        ActiveMQReceiver receiver = new ActiveMQReceiver(servers, channel, threads, datastores);
        new WatchDog().watchAndWait(receiver, ds);
    }
}
