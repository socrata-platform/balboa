package com.socrata.balboa.jms.activemq;

import com.socrata.balboa.metrics.Metrics;
import com.socrata.balboa.metrics.impl.JsonMessage;
import org.apache.activemq.ActiveMQMessageProducer;
import org.apache.activemq.ActiveMQSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.TextMessage;
import javax.naming.NamingException;
import java.util.Iterator;

public class Producer {
    private static Logger log = LoggerFactory.getLogger(Producer.class);

    private ActiveMQSession session;
    private ActiveMQMessageProducer producer;

    public Producer(ActiveMQSession session, String queueName) throws NamingException, JMSException {
        this.session = session;

        Queue queue = session.createQueue(queueName);
        producer = (ActiveMQMessageProducer) session.createProducer(queue);
    }

    /**
     * Send a JsonMessage to the queue.
     * @param json Message to send
     * @throws JMSException If the message fails to send
     */
    public void sendMessage(JsonMessage json) throws JMSException {
        TextMessage textMessage = session.createTextMessage(json.toString());
        producer.send(textMessage);
    }

    /**
     * Send a metrics message to the queue.
     * @param entityId Entity id of message
     * @param timestamp Timestamp of message
     * @param metrics List of metrics
     * @throws JMSException If the message fails to send
     */
    public void sendMetrics(String entityId, long timestamp, Metrics metrics) throws JMSException {
        log.info("Sending chunked message with " + metrics.size() + " metrics for entity " + entityId + " back to the queue");
        JsonMessage newMessage = new JsonMessage(entityId, timestamp, metrics);
        this.sendMessage(newMessage);
    }

    /**
     * Splits the given message into chunks that are metricCountLimit size and puts them back on the queue.
     * Next time they are read from the queue, they will be small enough and will automaticlly be persisted to the data store.
     * @param message Message to chunk
     * @param metricCountLimit Size of chunks to send
     * @throws JMSException If the message fails to send
     */
    public void splitAndResendMessage(JsonMessage message, int metricCountLimit) throws JMSException {
        Metrics metrics = message.getMetrics();
        Iterator<String> keysIterator = metrics.keySet().iterator();

        Metrics newMessageMetrics = new Metrics(metricCountLimit);

        // iterate through all the existing metrics...
        while(keysIterator.hasNext()) {
            String key = keysIterator.next();
            newMessageMetrics.put(key, metrics.get(key));

            // send the smaller message if we've hit the limit
            if (newMessageMetrics.size() >= metricCountLimit) {
                this.sendMetrics(
                        message.getEntityId(),
                        message.getTimestamp(),
                        newMessageMetrics
                );

                newMessageMetrics = new Metrics();
            }
        }

        // if there are any remaining, send them too
        if (!newMessageMetrics.isEmpty()) {
            this.sendMetrics(
                    message.getEntityId(),
                    message.getTimestamp(),
                    newMessageMetrics
            );
        }
    }
}
