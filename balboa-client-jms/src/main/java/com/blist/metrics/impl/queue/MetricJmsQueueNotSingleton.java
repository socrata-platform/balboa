package com.blist.metrics.impl.queue;

import com.socrata.balboa.metrics.Metric;
import com.socrata.balboa.metrics.Metrics;
import com.socrata.balboa.metrics.impl.JsonMessage;
import com.socrata.metrics.IdParts;
import com.socrata.metrics.MetricQueue$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;


/**
 * This class mirrors the Event class except that it drops the events in the
 * JMS queue and doesn't actually create "Event" objects, instead it creates
 * messages that the metrics service consumes.
 */
public class MetricJmsQueueNotSingleton extends AbstractJavaMetricQueue {
    private static final Logger log = LoggerFactory.getLogger(MetricJmsQueue.class);
    private final Buffer writeBuffer = new Buffer();
    private final Session session;
    private final Destination queue;
    private final MessageProducer producer;
    private final UpdateTimer flusher;

    private static class Buffer {
        static class Item {
            Metrics data;
            long timestamp;
            String id;

            Item(String id, Metrics data, long timestamp) {
                this.id = id;
                this.data = data;
                this.timestamp = timestamp;
            }

            @Override
            public String toString() {
                return "{id: \"" + id + "\", timestamp: " + timestamp + "}";
            }
        }

        Map<String, Item> buffer = new HashMap<String, Item>();

        synchronized void add(String entityId, Metrics data, long timestamp) {
            long nearestSlice = timestamp - (timestamp %  MetricQueue$.MODULE$.AGGREGATE_GRANULARITY());
            String bufferKey = entityId + ":" + nearestSlice;

            Item notBuffered = new Item(entityId, data, nearestSlice);

            if (buffer.containsKey(bufferKey)) {
                Item buffered = buffer.get(bufferKey);
                buffered.data.merge(notBuffered.data);
            } else {
                buffer.put(bufferKey, notBuffered);
            }
        }

        public int size() {
            return buffer.size();
        }
    }

    private class UpdateTimer extends Thread {
        public final Semaphore shutdown = new Semaphore(0);
        
        public UpdateTimer(String threadName) {
            setName(threadName);
        }

        @Override
        public void run() {
            try {
                // Note from non author: Having a semaphore with 0 initial permits looks like this is forcing this
                // call to acquire to time out.  In effect this is attempting to force this thread to wait the allotted
                // aggregate granularity until flushing.
                while(!shutdown.tryAcquire(MetricQueue$.MODULE$.AGGREGATE_GRANULARITY(), TimeUnit.MILLISECONDS)) {
                    flushWriteBuffer();
                }
                flushWriteBuffer();
            } catch(Exception e) {
                log.error("Unexpected exception while flushing queue!  This is BAD!!", e);
            }
        }
    }

    private void flushWriteBuffer() {
        synchronized (writeBuffer) {
            int size = writeBuffer.size();
            if (size > 0) {
                log.info("Flushing Metric buffer of " + size + " items.");

                for (Buffer.Item gunk : writeBuffer.buffer.values()) {
                    queue(gunk.id, gunk.timestamp, gunk.data);
                }

                writeBuffer.buffer.clear();
            }
        }
    }

    public MetricJmsQueueNotSingleton(Connection connection, String queueName) throws JMSException {
        this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        this.queue = session.createQueue(queueName);
        this.producer = session.createProducer(queue);

        flusher = new UpdateTimer("metrics-update-timer");
    }

    public void start() {
        flusher.start();
    }

    public void close() {
        flusher.shutdown.release();
        try {
            flusher.join();
        } catch(InterruptedException e) {
            // sigh....
        }
        try {
            session.close();
        } catch (JMSException e) {
            // I hate you, Java
            throw new RuntimeException(e);
        }
    }

    private void updateWriteBuffer(String entityId, long timestamp, Metrics metrics) {
        if (entityId == null) {
            throw new RuntimeException("Unable to insert data without an entityId.");
        } else if (timestamp <= 0) {
            throw new RuntimeException("Unable to insert data without a timestamp.");
        }
        synchronized (writeBuffer) {
            writeBuffer.add(entityId, metrics, timestamp);
        }
    }

    private void queue(String entityId, long timestamp, Metrics metrics) {
        try {
            JsonMessage msg = new JsonMessage();
            msg.setEntityId(entityId);
            msg.setMetrics(metrics);
            msg.setTimestamp(timestamp);
            try {
                producer.send(session.createTextMessage(new String(msg.serialize())));
            } catch (IOException e) {
                log.error("Unable to serialize metric for entity " + entityId, e);
            }
        } catch (JMSException e) {
            log.error("Unable to queue a message because there was a JMS error.");
            throw new RuntimeException("Unable to queue a message because there was a JMS error.", e);
        }
    }

    private void create(String entityId, String name, Number value, long timestamp, Metric.RecordType type) {
        Metrics metrics = new Metrics();
        Metric metric = new Metric();
        metric.setType(type);
        metric.setValue(value);
        metrics.put(name, metric);


        updateWriteBuffer(entityId, timestamp, metrics);
    }

    @Override
    public void create(IdParts entity, IdParts name, long value, long timestamp, Metric.RecordType type) {
        create(entity.toString(), name.toString(), value, timestamp, type);
    }

}
