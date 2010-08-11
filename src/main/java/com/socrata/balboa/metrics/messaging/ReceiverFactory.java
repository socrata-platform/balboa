package com.socrata.balboa.metrics.messaging;

import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.messaging.impl.ActiveMQReceiver;
import com.socrata.balboa.metrics.messaging.impl.ListReceiver;
import com.socrata.balboa.server.exceptions.InternalException;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ReceiverFactory
{
    private static Log log = LogFactory.getLog(ReceiverFactory.class);

    public static Receiver get()
    {
        String environment = System.getProperty("socrata.env");

        if ("test".equals(environment))
        {
            log.debug("Retrieving a ListReceiver instance.");
            return new ListReceiver();
        }
        else
        {
            log.debug("Retrieving a ActiveMQReceiver instance.");
            
            try
            {
                Configuration config = Configuration.get();
                String server = config.getProperty("activemq.url");
                String channel = config.getProperty("activemq.channel");

                ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(server);
                factory.setTransportListener(new ActiveMQReceiver.TransportLogger());
                
                return new ActiveMQReceiver(factory, channel);
            }
            catch (Exception e)
            {
                log.warn("Unable to create an ActiveMQReceiver.", e);
                throw new InternalException("Unable to create an ActiveMQReceiver.", e);
            }
        }
    }
}
