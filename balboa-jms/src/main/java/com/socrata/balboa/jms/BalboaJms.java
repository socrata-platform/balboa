package com.socrata.balboa.jms;

import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class BalboaJms
{
    static void usage()
    {
        System.err.println("java -jar balboa-jms [activemq urls] [activemq channel]");
    }

    static String[] parseServers(String[] args)
    {
        return Arrays.copyOfRange(args, 0, args.length - 1);
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

        String[] servers = parseServers(args);
        String channel = parseChannel(args);

        configureLogging();

        System.out.println("Receivers starting, awaiting messages.");
        ActiveMQReceiver receiver = new ActiveMQReceiver(servers, channel);

        while (true)
        {
            Thread.sleep(100);
        }
    }
}
