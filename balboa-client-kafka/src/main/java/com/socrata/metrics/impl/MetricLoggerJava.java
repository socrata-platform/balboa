package com.socrata.metrics.impl;

import com.blist.metrics.impl.queue.AbstractJavaMetricQueue;
import com.socrata.balboa.metrics.Metric;
import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.metrics.IdParts;

import java.io.IOException;

/**
 * Java wrapper around MetricLogger.
 */
public class MetricLoggerJava extends AbstractJavaMetricQueue {

    private static MetricLoggerJava instance;

    /**
     * Underlying Metrics logger
     */
    private MetricLoggerToKafka.MetricLogger logger;

    public static MetricLoggerJava getInstance(String topic) {
        if (instance == null)
            instance = new MetricLoggerJava(topic);
        return instance;
    }

    /**
     * Create an instance of a Metric Logger.
     */
    private MetricLoggerJava(String topic) {
        String brokerList;
        try {
            brokerList = Configuration.get().getProperty("balboa.client.kafka.brokers", "localhost:9062");
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
