package com.socrata.balboa.metrics.messaging.impl;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreFactory;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.metrics.messaging.Receiver;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;

import javax.jms.*;
import javax.naming.NamingException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @see ActiveMQReceiver
 *
 * The Asynchronous receiver is a large performance boost over the synchronous
 * one, but may flood the backend with too many writes. Instead of using a
 * message transaction, which blocks the queue from delivering any subsequent
 * messages while the current message is being processed, it uses auto message
 * acknowledgement and in the case of a failure pushes a failed message back
 * onto the queue.
 *
 * Actual writes are spawned in a seperate thread and processed after the
 * message has been acknowledged.
 */
public class AsyncActiveMQReceiver implements Receiver
{
    private static Log log = LogFactory.getLog(AsyncActiveMQReceiver.class);
    private List<Listener> listeners;

    private static final int MAX_REDELIVERY = 5;

    /**
     * We can't have a dynamic thread pool because if the threads manage to get
     * backed up, it could bring the whole servlet to a halt. Fifty writer
     * threads should be more than enough </unscientific statement>.
     */
    private static final int THREAD_POOL_SIZE = 50;
    private static final ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    /**
     * This is our thread runner that does the actual processing of the message.
     */
    static class Writer implements Runnable
    {
        Receiver receiver;
        Listener listener;
        TextMessage original;

        Writer(Receiver receiver, Listener listener, TextMessage original)
        {
            this.listener = listener;
            this.original = original;
            this.receiver = receiver;
        }

        @Override
        public void run()
        {
            try
            {
                // Deserialize the message contents
                String serialized = original.getText();
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

                // Extract the entity and timestamp so they don't get written as
                // stats.
                String entityId = (String)data.remove("entityId");
                Number timestamp = (Number)data.remove("timestamp");

                // Create an actual summary.
                Summary summary = new Summary(DateRange.Type.REALTIME, timestamp.longValue(), data);

                receiver.received(entityId, summary);
            }
            catch (Exception e)
            {
                try
                {
                    listener.redeliverOrFail(original);
                }
                catch (Exception ex)
                {
                    log.error("There was a problem redelivering a message. Probably ActiveMQ is hugely broken and there's no choice but to drop this message.", ex);
                }

            }
        }
    }

    static class Listener implements MessageListener
    {
        private Session session;
        private Queue queue;
        private MessageConsumer consumer;
        private MessageProducer producer;
        private Connection connection;
        private Receiver receiver;
        
        public Listener(Receiver receiver, ConnectionFactory connFactory, String queueName) throws NamingException, JMSException
        {
            this.receiver = receiver;
            connection = connFactory.createConnection();

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            queue = session.createQueue(queueName);

            consumer = session.createConsumer(queue);
            producer = session.createProducer(queue);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            consumer.setMessageListener(this);

            connection.start();
        }

        @Override
        public void onMessage(Message message)
        {
            long startTime = System.currentTimeMillis();

            try
            {
                TextMessage text = (TextMessage)message;

                pool.submit(new Writer(receiver, this, text));
            }
            catch (Exception e)
            {
                // If there was an exception here, either the message was the
                // wrong format or something is terribly severely wrong and
                // and there's nothing we can do about it. Either way, just
                // drop the message.
                log.error("Unexpected error when trying to start a message writer thread. Dropping this message", e);
            }
            finally
            {
                long totalTime = System.currentTimeMillis() - startTime;
                log.debug("Total time processing message (does not include write time) " + totalTime + " (ms)");
            }
        }

        /**
         * Given a failed message, attempt to redeliver it, or if the maximum
         * redelivery count has been reached, simply discard the message and log
         * that something went wrong.
         */
        private void redeliverOrFail(TextMessage message) throws JMSException
        {
            int redelivered;

            try
            {
                redelivered = message.getIntProperty("redelivered");
            }
            catch (NumberFormatException e)
            {
                redelivered = 0;
            }

            if (redelivered < MAX_REDELIVERY)
            {
                log.warn("Message failed to process properly. Attempting redelivery number " + redelivered + ". '" + message.getText() + "'");
                TextMessage redelivery = session.createTextMessage(message.getText());
                redelivery.setIntProperty("redelivered", redelivered + 1);
                redelivery.setJMSRedelivered(true);
                producer.send(redelivery);
            }
            else
            {
                // This message is a bad apple. Get rid of it. Which is easy,
                // we just ignore it. Boosh!
                // TODO: Write the message contents out to an email or to a file
                log.error("Message has been delivered too many times without a success. Dropping it. '" + message.getText() + "'");
            }
        }
    }

    public AsyncActiveMQReceiver(String[] servers, String channel) throws NamingException, JMSException
    {
        listeners = new ArrayList<Listener>(servers.length);

        for (String server : servers)
        {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(server);
            factory.setTransportListener(new ActiveMQReceiver.TransportLogger());

            listeners.add(new Listener(this, factory, channel));
        }
    }

    @Override
    public void received(String entityId, Summary summary) throws IOException
    {
        DataStore ds = DataStoreFactory.get();
        ds.persist(entityId, summary);
    }
}
