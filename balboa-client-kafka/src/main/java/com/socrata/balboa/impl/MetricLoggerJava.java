package com.socrata.balboa.impl;

import com.blist.metrics.impl.queue.AbstractJavaMetricQueue;
import com.socrata.balboa.metrics.Metric;
import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.config.Keys;
import com.socrata.metrics.IdParts;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Java wrapper around @see {@link MetricLoggerToKafka}.
 */
public class MetricLoggerJava extends AbstractJavaMetricQueue {

    /**
     * Mapping topics => MetricLoggers.
     */
    private static Map<String, MetricLoggerJava> instances = new HashMap<>();

    /**
     * Underlying Metrics logger
     */
    private MetricLoggerToKafka.MetricLogger logger;

    /**
     * Returns a Metric Logger Instance for a given topic.
     *
     * @param topic Topic to get Metric Logger for.
     * @return The producer for a given topic.
     */
    public static MetricLoggerJava getInstance(String topic) {
        if (instances.containsKey(topic)) {
            return instances.get(topic);
        } else {
            MetricLoggerJava logger;
            synchronized (MetricLoggerJava.class) {
                logger = new MetricLoggerJava(topic);
                instances.put(topic, logger);
            }
            return logger;
        }
    }

    /**
     * Creates a metrics logger that published to a specific topic.
     *
     * @param topic The topic to publish messages to.
     */
    private MetricLoggerJava(String topic) {
        String brokerList;
        try {
            brokerList = Configuration.get().getProperty(Keys.KAFKA_METADATA_BROKERLIST, "localhost:9062");
        } catch (IOException e) {
            throw new IllegalStateException("Unable to load configuration", e);
        }
        logger = new MetricLoggerToKafkaJava().MetricLogger(brokerList, topic, topic + ".backup");
    }

    @Override
    public void create(IdParts entity, IdParts name, long value, long timestamp, Metric.RecordType recordType) {
        this.logger.logMetric(entity.toString(), name.toString(), value, timestamp, recordType);
    }
}
