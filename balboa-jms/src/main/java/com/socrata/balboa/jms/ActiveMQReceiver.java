package com.socrata.balboa.jms;

import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreFactory;
import com.socrata.balboa.metrics.impl.JsonMessage;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.transport.TransportListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.*;
import javax.naming.NamingException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The ActiveMQReceiver listens to the JMS queue/server configured in your
 * configuration and tries to save events that are put on the queue. Messages on
 * the queue should be text messages and their body should be a JSON encoded
 * object of the event.
 *
 * The entityId is the entity on whom metrics are being tracked. The timestamp
 * is the time, in milliseconds, when the metrics themselves occurred. And
 * finally "metrics" is the collection of metrics.
 *
 * e.g.
 *
 * <code>
 *     {
 *         "entityId": "foo",
 *         "timestamp": 1281037944000,
 *         "metrics": {
 *              "view-loaded" : {
 *                  "value": 104,
 *                  "type": AGGREGATE
 *              },
 *              "view-downloaded" : {
 *                  "value": 10,
 *                  "type": ABSOLUTE
 *              },
 *         }
 *     }
 * </code>
 *
 * Here the key "foo" will persist at the timestamp the two metrics "view-count"
 * and "view-downloaded".
 *
 * If for some reason a message cannot be persisted, it will be rejected and
 * redelivered as the JMS provider sees fit.
 */
public class ActiveMQReceiver
{
    private static Log log = LogFactory.getLog(ActiveMQReceiver.class);
    private List<Listener> listeners;

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

    class Listener implements MessageListener
    {
        private ActiveMQSession session;
        private Queue queue;
        private ActiveMQMessageConsumer consumer;
        private ActiveMQConnection connection;

        public Listener(ActiveMQConnectionFactory connFactory, String queueName) throws NamingException, JMSException
        {
            connection = (ActiveMQConnection) connFactory.createConnection();
            session = (ActiveMQSession) connection.createSession(true, Session.SESSION_TRANSACTED);
            queue = session.createQueue(queueName);
            consumer = (ActiveMQMessageConsumer) session.createConsumer(queue);
            consumer.setMessageListener(this);
            connection.start();
        }

        @Override
        public void onMessage(Message payload)
        {
            try
            {
                TextMessage text = (TextMessage)payload;

                JsonMessage message = new JsonMessage(text.getText());

                DataStore ds = DataStoreFactory.get();
                ds.persist(message.getEntityId(), message.getTimestamp(), message.getMetrics());

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
                } finally {
                    // stop the consumer for now. The message is hopefully rolledback.
                    // restart will occur when the WatchDog says so, upon decree from the
                    // BalboaFastFailCheck
                    stop();
                }
            }
        }

        public void stop() {
            consumer.stop();
        }

        public void restart() {
            try {
                consumer.start();
            } catch (JMSException e) {
                log.error("Unable to restart the consumer after data store failure. This is bad.");
            }
        }
    }

    volatile boolean stopped = false;

    public ActiveMQReceiver(String[] servers, String channel, Integer threads) throws NamingException, JMSException
    {
        listeners = new ArrayList<Listener>(servers.length);
        for (String server : servers)
        {
            for (int i=0; i < threads; i++)
            {
                ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(server);
                factory.setTransportListener(new ActiveMQReceiver.TransportLogger());

                listeners.add(new Listener(factory, channel));
            }
        }

        log.info("Listeners all started.");
    }

    // Used to restart the receiver on DataStore failure
    public void restart() {
        stopped = false;
        for (Listener listener : listeners)
            listener.restart();
    }

    // Used to stop all listeners, whether they are already stopped
    // or not.
    public void stop() {
        stopped = true;
        for (Listener listener : listeners)
            listener.stop();
    }

    public boolean isStopped() {
        return stopped;
    }
}
