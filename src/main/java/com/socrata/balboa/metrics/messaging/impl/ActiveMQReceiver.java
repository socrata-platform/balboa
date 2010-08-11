package com.socrata.balboa.metrics.messaging.impl;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreFactory;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.metrics.messaging.Receiver;
import org.apache.activemq.transport.TransportListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;

import javax.jms.*;
import javax.naming.NamingException;
import java.io.IOException;
import java.util.Map;

/**
 * The ActiveMQReceiver listens to the JMS queue/server configured in your
 * configuration and tries to save events that are put on the queue. Messages on
 * the queue should be text messages and their body should be a JSON encoded
 * object of the event. 
 *
 * Events on the queue require two keys:
 *
 *     "entityId" and "timestamp"
 *
 * The entityId is the row id that the summary applies to and the timestamp is
 * the moment that the event occurred in milliseconds.
 *
 * All other key/values in the event are considered metric/value pairs.
 *
 * e.g.
 *
 * <code>
 *     {
 *         "entityId": "foo",
 *         "timestamp": 1281037944000,
 *         "view-count": 1,
 *         "rows-loaded": 500
 *     }
 * </code>
 *
 * Here the key "foo" will persist at the timestamp the two metrics "view-count"
 * and "rows-loaded".
 *
 * If for some reason a message cannot be persisted, it will be rejected and
 * redelivered as the JMS provider sees fit.
 */
public class ActiveMQReceiver implements Receiver, MessageListener
{
    private static Log log = LogFactory.getLog(ActiveMQReceiver.class);

    private Session session;
    private Queue queue;
    private MessageConsumer consumer;
    private Connection connection;

    public static class TransportLogger implements TransportListener
    {
        @Override
        public void onCommand(Object o)
        {
        }

        @Override
        public void onException(IOException e)
        {
            log.error("There was an exception on the ActiveMQReceiver's transport.", e);
        }

        @Override
        public void transportInterupted()
        {
            log.error("ActiveMQ transport interrupted.");
        }

        @Override
        public void transportResumed()
        {
            log.info("ActiveMQ transport resumed.");
        }
    }

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

                // If there was a problem parsing the JSON in a message,
                // we can never possibly recover, so just commit that this
                // message was received and move on.
                session.commit();
                
                return;
            }

            String entityId = (String)data.remove("entityId");
            Number timestamp = (Number)data.remove("timestamp");

            // Create an actual summary.
            Summary summary = new Summary(DateRange.Type.REALTIME, timestamp.longValue(), data);

            received(entityId, summary);

            // Provided there were no exceptions receiving the message and
            // persisting it, commit the result.
            session.commit();
        }
        catch (Exception e)
        {
            log.error("There was some problem processing a message. Marking it as needing redelivery.", e);
            
            try
            {
                // If there was some problem receiving the message or commiting
                // it or, really, any problem in the processing that's not
                // expected, we should rollback the results (which marks the
                // message as not-delivered). The JMS provider will try to
                // redeliver as it sees fit and hopefully things will be working
                // for the next node that gets it.
                session.rollback();
            }
            catch (JMSException e1)
            {
                log.error("There was a problem rolling back the session. This is really bad.", e1);
            }
        }
    }

    @Override
    public void received(String entityId, Summary summary) throws IOException
    {
        DataStore ds = DataStoreFactory.get();
        ds.persist(entityId, summary);
    }
}
