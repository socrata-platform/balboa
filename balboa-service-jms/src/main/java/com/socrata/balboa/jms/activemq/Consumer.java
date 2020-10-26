package com.socrata.balboa.jms.activemq;

import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.impl.JsonMessage;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.ActiveMQSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.naming.NamingException;

public class Consumer implements MessageListener {
    private static Logger log = LoggerFactory.getLogger(Consumer.class);

    private ActiveMQSession session;
    private ActiveMQMessageConsumer consumer;
    private DataStore dataStore;
    private Producer producer;
    private int metricCountLimit;
    private boolean stopWrites;

    public Consumer(ActiveMQSession session, String queueName, DataStore ds, int metricCountLimit, boolean stopWrites) throws NamingException, JMSException {
        this.dataStore = ds;
        this.session = session;
        this.metricCountLimit = metricCountLimit;
        this.stopWrites = stopWrites;

        Queue queue = session.createQueue(queueName);
        consumer = (ActiveMQMessageConsumer) session.createConsumer(queue);

        consumer.setMessageListener(this);

        producer = new Producer(session, queueName);
    }

    @Override
    public void onMessage(Message payload) {
        long start = System.currentTimeMillis();
        String messageText = null;
        try {
            // Before we even consider chunking a large message, check if we even need to write to send out the message
            if (stopWrites) {
                session.commit();
                log.debug("Config was set to stop writes from Balboa to Cassandra. Ignoring message");
            } else {
                TextMessage text = (TextMessage) payload;
                messageText = text.getText();

                JsonMessage message = JsonMessage.apply(messageText);

                int metricsSize = message.getMetrics().size();
                if (metricsSize > metricCountLimit) {
                    log.info(
                            "Message contained " +
                                    metricsSize +
                                    " metrics which is larger than the configured max of " +
                                    metricCountLimit +
                                    "; chunking the message and re-adding it the queue");
                    producer.splitAndResendMessage(message, metricCountLimit);
                } else {
                    dataStore.persist(message.getEntityId(), message.getTimestamp(), message.getMetrics());
                }

                session.commit();
                log.info("Consumed message of size " + (messageText == null ? "null" : messageText.length())
                        + " for entity: " + message.getEntityId()
                        + " - took " + (System.currentTimeMillis() - start) + "ms");
            }
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
                log.error("Unable to restart the consumer after data store failure. This is bad.", e);
            }
        }
    }
}
