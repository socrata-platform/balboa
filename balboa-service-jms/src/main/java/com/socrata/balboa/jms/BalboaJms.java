package com.socrata.balboa.jms;

import com.socrata.balboa.metrics.WatchDog;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreFactory;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class BalboaJms {

    private static final Logger log = LoggerFactory.getLogger(BalboaJms.class);

    static void usage() {
        System.err.println("java -jar balboa-jms [thread count] [activemq urls] [activemq channel]");
    }

    static Integer parseThreads(String[] args)
    {
        return Integer.parseInt(args[0]);
    }

    static String[] parseServers(String[] args)
    {
        return Arrays.copyOfRange(args, 1, args.length - 1);
    }

    static String parseChannel(String[] args)
    {
        return args[args.length-1];
    }

    static void configureLogging() throws IOException
    {
        Properties p = new Properties();
        p.load(BalboaJms.class.getClassLoader().getResourceAsStream("config/config.properties"));
        PropertyConfigurator.configure(p);
    }

    public static void main(String[] args) throws Exception
    {
        if (args.length < 2)
        {
            usage();
            System.exit(1);
        }

        Integer threads = parseThreads(args);
        String[] servers = parseServers(args);
        String channel = parseChannel(args);

        configureLogging();

        log.info("Receivers starting, awaiting messages.");
        DataStore ds = DataStoreFactory.get();
        ActiveMQReceiver receiver = new ActiveMQReceiver(servers, channel, threads, ds);
        new WatchDog().watchAndWait(receiver, ds);
    }
}
