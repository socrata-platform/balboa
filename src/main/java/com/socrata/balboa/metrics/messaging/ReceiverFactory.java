package com.socrata.balboa.metrics.messaging;

import com.socrata.balboa.metrics.messaging.impl.ActiveMQReceiver;
import com.socrata.balboa.metrics.messaging.impl.ListReceiver;
import com.socrata.balboa.server.exceptions.InternalException;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.ConnectionFactory;

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
                ConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
                return new ActiveMQReceiver(factory, "Metrics");
            }
            catch (Exception e)
            {
                log.fatal("Unable to create an ActiveMQReceiver.", e);
                throw new InternalException("Unable to create an ActiveMQReceiver.", e);
            }
        }
    }
}
