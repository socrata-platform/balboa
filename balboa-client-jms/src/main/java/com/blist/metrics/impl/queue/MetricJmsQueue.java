package com.blist.metrics.impl.queue;

import com.socrata.balboa.metrics.Metric;
import com.socrata.balboa.metrics.Metrics;
import com.socrata.balboa.metrics.impl.JsonMessage;
import com.socrata.metrics.IdParts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.sql.Timestamp;
import java.util.Collection;


/**
 * This class mirrors the Event class except that it drops the events in the
 * JMS queue and doesn't actually create "Event" objects, instead it creates
 * messages that the metrics service consumes.
 */
public class MetricJmsQueue extends AbstractJavaMetricQueue {
    private static final Logger log = LoggerFactory.getLogger(MetricJmsQueue.class);
    private final MetricsBuffer writeBuffer = new MetricsBuffer();
    private final Session session;
    private final Destination queue;
    private final MessageProducer producer;
    private final int bufferCapacity;

    // TODO Handle the case where we need graceful shutdown.

    /**
     * Creates a JMS Queue.
     *
     * @param connection The Activemq connection.
     * @param queueName The name of the queue to send metrics to.
     * @param bufferCapacity The maximum size of the buffer to maintain.
     * @throws JMSException When there is a problem sending to the JMS Server.
     */
    public MetricJmsQueue(Connection connection, String queueName, int bufferCapacity) throws JMSException {
        if (bufferCapacity < 0) {
            throw new IllegalArgumentException("Buffer capacity cannot be negative. Actual: " + bufferCapacity);
        }

        this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        this.queue = session.createQueue(queueName);
        this.producer = session.createProducer(queue);
        this.bufferCapacity = bufferCapacity;
    }

    private void flushWriteBuffer() {
        Collection<MetricsBucket> buckets = writeBuffer.popAll(); // Remove and empty
        log.info("Flushing the write buffer of all {} metric buckets.", buckets.size());
        for (MetricsBucket bucket : buckets) {
            queue(bucket.getId(), bucket.getTimeBucket(), bucket.getData());
        }
    }

    private void queue(String entityId, long timestamp, Metrics metrics) {
        try {
            log.debug("Sending metrics to queue for entity id: {} with associated time {} and metrics (Size: {}) {}",
                    entityId, new Timestamp(timestamp).toString(), metrics.size(), metrics);
            JsonMessage msg = new JsonMessage();
            msg.setEntityId(entityId);
            msg.setMetrics(metrics);
            msg.setTimestamp(timestamp);
            byte[] bytes = msg.serialize();
            producer.send(session.createTextMessage(new String(bytes)));
        } catch (Exception e) {
            log.error("Error sending metrics to Queue for {} with associated time {} because of {}", entityId,
                    new Timestamp(timestamp).toString(), e.getMessage());
            throw new RuntimeException("Unable to queue a message because there was a JMS error.", e);
        }
    }

    private void create(String entityId, String name, Number value, long timestamp, Metric.RecordType type) {
        log.debug("Creating metric {}:{} for {} with associated time {} by sending it to the write buffer",
                name, value, entityId, new Timestamp(timestamp).toString());
        Metrics metrics = new Metrics();
        Metric metric = new Metric();
        metric.setType(type);
        metric.setValue(value);
        metrics.put(name, metric);

        // Flush the buffer if it becomes too large.
        // A simpler model given the Java default concurrency model.
        writeBuffer.add(entityId, metrics, timestamp);
        if (writeBuffer.size() >= this.bufferCapacity) {
            flushWriteBuffer();
        }
    }

    @Override
    public void close() throws Exception {
        log.info("Closing {}, attempting to flush all the metrics to the queue.", this.getClass().getSimpleName());
        flushWriteBuffer();
    }

    @Override
    public void create(IdParts entity, IdParts name, long value, long timestamp, Metric.RecordType type) {
        create(entity.toString(), name.toString(), value, timestamp, type);
    }

}
