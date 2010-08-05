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

        session = connection.createSession(true, Session.SESSION_TRANSACTED);
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

            Map<String, Object> data;
            try
            {
                data = (Map<String, Object>)mapper.readValue(serialized, Object.class);
            }
            catch (IOException e)
            {
                log.error("There was a problem parsing the JSON into a summary. Ignoring this message.", e);
                return;
            }

            String entityId = (String)data.remove("entityId");
            Number timestamp = (Number)data.remove("timestamp");

            // Create an actual summary.
            Summary summary = new Summary(Summary.Type.REALTIME, timestamp.longValue(), data);

            received(entityId, summary);
            session.commit();
        }
        catch (Throwable e)
        {
            log.error("There weas some problem processing a message. Marking it as needing redelivery.", e);
            try
            {
                session.rollback();
            }
            catch (JMSException e1)
            {
                e1.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
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
