package com.socrata.balboa.jms;

import com.socrata.balboa.metrics.config.Configuration;

public class BalboaJms
{
    public static void main(String[] args) throws Exception
    {
        Configuration config = Configuration.get();
        String[] servers = config.getProperty("activemq.urls").split(" ");
        String channel = config.getProperty("activemq.channel");

        ActiveMQReceiver receiver = new ActiveMQReceiver(servers, channel);

        while (true)
        {
            Thread.sleep(100);
        }
    }
}
