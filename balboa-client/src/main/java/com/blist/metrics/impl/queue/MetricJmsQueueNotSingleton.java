package com.blist.metrics.impl.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.jms.*;

import static com.socrata.util.deepcast.DeepCast.*;
import com.socrata.balboa.metrics.impl.JsonMessage;
import com.socrata.balboa.metrics.Metric;
import com.socrata.balboa.metrics.*;
import com.socrata.metrics.*;


/**
 * This class mirrors the Event class except that it drops the events in the
 * JMS queue and doesn't actually create "Event" objects, instead it creates
 * messages that the metrics service consumes.
 */
public class MetricJmsQueueNotSingleton extends AbstractMetricQueue {
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
                merge(mapOf(string, mapOfObject).cast(buffered.data), mapOf(string, mapOfObject).cast(notBuffered.data));
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

    private static Object merge(Map<String, Map<String, Object>> first, final Map<String, Map<String, Object>> second) {
        // Get the union of the two key sets.
        Set<String> unionKeys = new HashSet<String>(first.keySet());
        unionKeys.addAll(second.keySet());

        // Combine the two maps.
        for (String key : unionKeys) {
            if (!first.containsKey(key)) {
                Map<String, Object> metric = new HashMap<String, Object>();
                metric.put("value", 0);

                String type = Metric.RecordType.AGGREGATE.toString();
                if (second.containsKey(key)) {
                    type = (String) second.get(key).get("type");
                }

                metric.put("type", type);
                first.put(key, metric);
            }

            if (!second.containsKey(key)) {
                Map<String, Object> metric = new HashMap<String, Object>();
                metric.put("value", 0);
                metric.put("type", first.get(key).get("type"));
                second.put(key, metric);
            }

            if (!first.get(key).get("type").equals(second.get(key).get("type"))) {
                throw new IllegalArgumentException("Invalid metric combination, types are different. " + key + " (" + first.get(key).get("type") + ", " + second.get(key).get("type") + ").");
            } else {
                Map<String, Object> merged = new HashMap<String, Object>();
                merged.put("value", ((Number) first.get(key).get("value")).longValue() + ((Number) second.get(key).get("value")).longValue());
                merged.put("type", first.get(key).get("type"));
                first.put(key, merged);
            }
        }

        return first;
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
