package com.socrata.balboa.metrics.messaging.impl;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreFactory;
import com.socrata.balboa.metrics.messaging.Receiver;
import com.socrata.balboa.server.exceptions.InternalException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;

import javax.jms.*;
import javax.naming.NamingException;
import java.io.IOException;
import java.util.Map;
import java.util.TimeZone;

public class ActiveMQReceiver implements Receiver, MessageListener
{
    private static Log log = LogFactory.getLog(ActiveMQReceiver.class);

    private Session session;
    private Queue queue;
    private MessageConsumer consumer;
    private Connection connection;

    public ActiveMQReceiver(ConnectionFactory connFactory, String queueName) throws NamingException, JMSException
    {
        connection = connFactory.createConnection();

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        queue = session.createQueue(queueName);

        consumer = session.createConsumer(queue);
        consumer.setMessageListener(this);

        connection.start();
    }

    @Override
    public void onMessage(Message message)
    {
        // Make sure that we never consume messages unless we're in UTC.
        if (!TimeZone.getDefault().equals(TimeZone.getTimeZone("UTC")))
        {
            throw new InternalException("Default timezone is not UTC so this " +
                    "request will not be serviced so our data stays consistent.");
        }
        
        try
        {
            TextMessage text = (TextMessage)message;

            String serialized = text.getText();
            ObjectMapper mapper = new ObjectMapper();

            Map<String, Object> data = (Map<String, Object>)mapper.readValue(serialized, Object.class);
            String entityId = (String)data.remove("entityId");
            Number timestamp = (Number)data.remove("timestamp");

            // Create an actual summary.
            Summary summary = new Summary(Summary.Type.REALTIME, timestamp.longValue(), data);

            received(entityId, summary);
        }
        catch (IOException e)
        {
            log.error("There was a problem parsing the JSON into a summary. Ignoring.", e);
        }
        catch (JMSException e)
        {
            log.error("The was a problem reading text from the message. Are you connected to the right channel? Ignoring.");
        }
    }

    @Override
    public void received(String entityId, Summary summary)
    {
        DataStore ds = DataStoreFactory.get();

        try
        {
            ds.persist(entityId, summary);
        }
        catch (IOException e)
        {
            throw new InternalException("Unable to persist a message to the datastore.", e);
        }
    }
}
