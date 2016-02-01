package com.blist.metrics.impl.queue;

import com.socrata.balboa.config.JavaJMSClientConfig;
import com.socrata.balboa.metrics.Metric;
import com.socrata.balboa.metrics.Metrics;
import com.socrata.balboa.metrics.impl.JsonMessage;
import com.socrata.metrics.IdParts;
import com.socrata.metrics.MetricQueue$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;


/**
 * This class mirrors the Event class except that it drops the events in the
 * JMS queue and doesn't actually create "Event" objects, instead it creates
 * messages that the metrics service consumes.
 */
public class MetricJmsQueueNotSingleton extends AbstractJavaMetricQueue {
    private static final Logger log = LoggerFactory.getLogger(MetricJmsQueueNotSingleton.class);
    private final MetricsBuffer writeBuffer = new MetricsBuffer();
    private final Session session;
    private final Destination queue;
    private final MessageProducer producer;

    // TODO Handle the case where we need graceful shutdown.

    public MetricJmsQueueNotSingleton(Connection connection, String queueName) throws JMSException {
        this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        this.queue = session.createQueue(queueName);
        this.producer = session.createProducer(queue);
    }

    private void flushWriteBuffer() {
        Collection<MetricsBag> items = writeBuffer.popAll(); // Remove and empty
        log.info("Flushing the write buffer of all {} metric bags.", items.size());
        for (MetricsBag bag : items) {
            queue(bag.getId(), bag.getTimeBucket(), bag.getData());
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

        // Flush the buffer if it becomes to large.
        // A simpler model given the Java default concurrency model.
        writeBuffer.add(entityId, metrics, timestamp);
        if (writeBuffer.size() >= JavaJMSClientConfig.bufferSize()) {
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
