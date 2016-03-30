package com.socrata.balboa.jms;

import com.socrata.balboa.metrics.WatchDog;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.impl.JsonMessage;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.transport.TransportListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class ActiveMQReceiver implements WatchDog.WatchDogListener
{
    private static Logger log = LoggerFactory.getLogger(ActiveMQReceiver.class);
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
        private DataStore ds;

        public Listener(ActiveMQConnectionFactory connFactory, String queueName, DataStore ds) throws NamingException, JMSException
        {
            this.ds = ds;
            connection = (ActiveMQConnection) connFactory.createConnection();
            session = (ActiveMQSession) connection.createSession(true, Session.SESSION_TRANSACTED);
            queue = session.createQueue(queueName);
            consumer = (ActiveMQMessageConsumer) session.createConsumer(queue);
            consumer.setMessageListener(this);
            connection.start();
        }

        @Override
        public void onMessage(Message payload) {
            long start = System.currentTimeMillis();
            String messageText = null;
            try {
                TextMessage text = (TextMessage)payload;
                messageText = text.getText();

                JsonMessage message = new JsonMessage(messageText);

                ds.persist(message.getEntityId(), message.getTimestamp(), message.getMetrics());

                session.commit();
                log.info("Consumed message of size " + (messageText == null ? "null" : messageText.length())
                                 + " for entity: " + message.getEntityId()
                                 + " - took " + (System.currentTimeMillis() - start) + "ms");
            } catch (Exception e) {
                log.error("There was some problem processing a message. Marking it as needing redelivery. Took " + (System.currentTimeMillis() - start) + "ms", e);

                try {
                    // If there was some problem receiving the message or committing
                    // it or, really, any problem in the processing that's not
                    // expected, we should rollback the results (which marks the
                    // message as not-delivered). The JMS provider will try to
                    // redeliver as it sees fit and hopefully things will be working
                    // for the next node that gets it.
                    session.rollback();
                } catch (JMSException e1) {
                    String messageFragment = messageText;
                    if (messageText != null && messageText.length() > 1024) {
                        messageFragment = messageText.substring(0, 1024);
                    }
                    log.error("There was a problem rolling back the session. The message was lost. [1st 1kb = {}]", messageFragment, e1);
                }
            }
        }

        public void stop() {
            synchronized (this) {
                log.error("Stopping JMS listener");
                consumer.stop();
            }
        }

        public void restart() {
            synchronized (this) {
                try {
                    log.error("Restarting JMS listener");
                    consumer.start();
                } catch (JMSException e) {
                    log.error("Unable to restart the consumer after data store failure. This is bad.");
                }
            }
        }
    }

    volatile boolean stopped = false;

    public ActiveMQReceiver(List<String> servers, String channel, Integer threads, DataStore ds) throws NamingException, JMSException
    {
        listeners = new ArrayList<>(servers.size());
        for (String server : servers)
        {
            log.info("Starting server {}",server);
            for (int i=0; i < threads; i++)
            {
                ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(server);
                factory.setTransportListener(new ActiveMQReceiver.TransportLogger());

                listeners.add(new Listener(factory, channel, ds));
            }
        }

        log.info("Listeners all started.");
    }

    // Used to restart the receiver on DataStore failure
    public void onStart() {
        synchronized (this) {
            stopped = false;
            for (Listener listener : listeners)
                listener.restart();
        }
    }

    // Used to stop all listeners, whether they are already stopped
    // or not.
    public void onStop() {
        synchronized (this) {
            stopped = true;
            for (Listener listener : listeners)
                listener.stop();
        }
    }

    @Override
    public void heartbeat() {
        // noop
    }

    public void ensureStarted() {
        if (stopped) {
            onStart();
        }
    }
}
