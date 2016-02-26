package com.socrata.balboa.jms;

import com.socrata.balboa.metrics.WatchDog;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreFactory;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

    static List<String> parseServers(String[] args, Properties p)
    {
        String soTimeout = "soTimeout="+p.getProperty("activemq.sotimeout", "15000");
        String writeTimeout = "soWriteTimeout="+p.getProperty("activemq.sowritetimeout", "15000");
        List<String> servers = new ArrayList<>();
        for(int i=1; i< args.length-1; i++){
            String[] srvs = args[i].split(",+(?![^\\(]*\\))");
            for(String s:srvs) {
                if(s.indexOf("?")<0) {
                    s += "?"+soTimeout;
                } else if (s.indexOf("soTimeout=")<0) {
                    s += "&"+soTimeout;
                }
                if(s.indexOf("soWriteTimeout=")<0) {
                    s+="&"+writeTimeout;
                }
                servers.add(s);
            }
        }
        return servers;
    }

    static String parseChannel(String[] args)
    {
        return args[args.length-1];
    }

    static void configureLogging(Properties p) throws IOException
    {
        PropertyConfigurator.configure(p);
    }

    public static void main(String[] args) throws Exception
    {
        if (args.length < 2)
        {
            usage();
            System.exit(1);
        }

        Properties p = new Properties();
        p.load(BalboaJms.class.getClassLoader().getResourceAsStream("config/config.properties"));

        Integer threads = parseThreads(args);
        List<String> servers = parseServers(args,p);
        String channel = parseChannel(args);

        configureLogging(p);
        log.info("Receivers starting, awaiting messages.");
        DataStore ds = DataStoreFactory.get();
        ActiveMQReceiver receiver = new ActiveMQReceiver(servers, channel, threads, ds);
        new WatchDog().watchAndWait(receiver, ds);
    }
}
