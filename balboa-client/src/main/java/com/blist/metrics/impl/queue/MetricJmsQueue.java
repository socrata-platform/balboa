package com.blist.metrics.impl.queue;

import com.socrata.metrics.*;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.socrata.util.deepcast.DeepCast.*;

import com.socrata.balboa.metrics.impl.JsonMessage;
import com.socrata.balboa.metrics.Metric;
import com.socrata.balboa.metrics.*;

import java.io.IOException;

/**
 * This class mirrors the Event class except that it drops the events in the
 * JMS queue and doesn't actually create "Event" objects, instead it creates
 * messages that the metrics service consumes.
 */
public class MetricJmsQueue extends AbstractMetricQueue {
    private static final Logger log = LoggerFactory.getLogger(MetricJmsQueue.class);
    static MetricJmsQueue instance;
    static Buffer writeBuffer = new Buffer();

    public static class Buffer {
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

    ConnectionFactory factory;
    private Session session;
    private Destination queue;
    private MessageProducer producer;
    private Connection connection;
    private UpdateTimer flusher;

    static class UpdateTimer extends Thread {
        public String server;
        public String queueName;

        public UpdateTimer(String server, String queueName, String threadName) {
            setName(threadName);
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep( MetricQueue$.MODULE$.AGGREGATE_GRANULARITY());
                } catch (InterruptedException e) {
                    // We want to make sure the final messages are sent to JMS before
                    // bombing out from an interrupt
                    flushWriteBuffer(server, queueName);
                    throw new RuntimeException("The buffer flush thread was interrupted. Flushing");
                }
                flushWriteBuffer(server, queueName);

            }
        }
    }

    public static void flushWriteBuffer(String server, String queueName) {
        synchronized (writeBuffer) {
            MetricJmsQueue queue = MetricJmsQueue.getInstance(server, queueName);

            int size = queue.writeBuffer.size();
            if (size > 0) {
                log.info("Flushing Metric buffer of " + size + " items.");

                for (Buffer.Item gunk : writeBuffer.buffer.values()) {
                    queue.queue(gunk.id, gunk.timestamp, gunk.data);
                }

                writeBuffer.buffer.clear();
            }
        }
    }

    public static MetricJmsQueue getInstance(String server, String queueName) {
        if (instance == null) {
            try {
                ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(server);
                factory.setUseAsyncSend(true);
                instance = new MetricJmsQueue(factory, server, queueName);
            } catch (JMSException e) {
                log.error("Unable to create a new Metric logger for JMS. Falling back to a NOOP logger.");

                instance = null;
            }
        }

        return instance;
    }

    MetricJmsQueue(ConnectionFactory factory, String server, String queueName) throws JMSException {
        this.factory = factory;

        connection = factory.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        queue = session.createQueue(queueName);
        producer = session.createProducer(queue);
        connection.start();

        flusher = new UpdateTimer("metrics-update-timer", server, queueName);
        flusher.start();
    }

    static Object merge(Map<String, Map<String, Object>> first, final Map<String, Map<String, Object>> second) {
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

    void updateWriteBuffer(String entityId, long timestamp, Metrics metrics) {
        if (entityId != null) {
            throw new RuntimeException("Unable to insert data without an entityId.");
        } else if (timestamp <= 0) {
            throw new RuntimeException("Unable to insert data without a timestamp.");
        }
        synchronized (writeBuffer) {
            writeBuffer.add(entityId, metrics, timestamp);
        }
    }

    void queue(String entityId, long timestamp, Metrics metrics) {
        try {
            JsonMessage msg = new JsonMessage();
            msg.setEntityId(entityId);
            msg.setMetrics(metrics);
            msg.setTimestamp(timestamp);
            try {
                producer.send(session.createObjectMessage(msg.serialize()));
            } catch (IOException e) {
                log.error("Unable to serialize metric for entity " + entityId, e);
            }
        } catch (JMSException e) {
            log.error("Unable to queue a message because there was a JMS error.");
            throw new RuntimeException("Unable to queue a message because there was a JMS error.", e);
        }
    }

    public void create(String entityId, String name, Number value, long timestamp, Metric.RecordType type) {
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
