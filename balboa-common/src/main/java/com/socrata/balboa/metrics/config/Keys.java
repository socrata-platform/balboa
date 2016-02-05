package com.socrata.balboa.metrics.config;

/**
 * Reference to all Property Keys for Balboa.  This prevents naming collisions
 * and potential type errors.  It also can serve as a single source reference for
 * what the keys are intended to be referencing.
 */
public class Keys {

    // TODO: Migrate all the String literal keys into here.
    // TODO: Migrate Keys to individual sub projects after Restructure.
    // Currently Balboa Scatters String literals all over the source code.

    private static final String ROOT_CONFIG_NAMESPACE = "balboa";

    private static final String ROOT_DISPATCHER_NAMESPACE = join(ROOT_CONFIG_NAMESPACE, "dispatcher");

    private static final String ROOT_KAFKA_NAMESPACE = join(ROOT_CONFIG_NAMESPACE, "kafka");

    private static final String ROOT_JMS_NAMESPACE = join(ROOT_CONFIG_NAMESPACE, "jms");

    private static final String ROOT_JMS_ACTIVEMQ_NAMESPACE = join(ROOT_JMS_NAMESPACE, "activemq");

    private static final String ROOT_EMERGENCY_DIR = join(ROOT_CONFIG_NAMESPACE, "emergency");

    /**
     * Key for activemq server
     */
    public static final String JMS_ACTIVEMQ_SERVER = join(ROOT_JMS_ACTIVEMQ_NAMESPACE, "server");

    /**
     * Key for the queue name in activemq
     */
    public static final String JMS_ACTIVEMQ_QUEUE = join(ROOT_JMS_ACTIVEMQ_NAMESPACE, "queue");

    /**
     * Key for the queue name in activemq
     */
    public static final String JMS_ACTIVEMQ_USER = join(ROOT_JMS_ACTIVEMQ_NAMESPACE, "user");

    /**
     * Key for the queue name in activemq
     */
    public static final String JMS_ACTIVEMQ_PASSWORD = join(ROOT_JMS_ACTIVEMQ_NAMESPACE, "password");

    /**
     * The buffer size threshold to keep until flushing to the queue.
     */
    public static final String JMS_ACTIVEMQ_MAX_BUFFER_SIZE = join(ROOT_JMS_ACTIVEMQ_NAMESPACE, "buffer.size");

    /**
     * Key to Kafka Brokers address:port for pulling metadata.
     */
    public static final String KAFKA_METADATA_BROKERLIST = join(ROOT_KAFKA_NAMESPACE, "brokers");

    /**
     * Key to the Kafka topic to publish messages to.
     */
    public static final String KAFKA_TOPIC = join(ROOT_KAFKA_NAMESPACE, "topic");

    /**
     * Key for emergency back up file.
     */
    public static final String BACKUP_DIR = join(ROOT_EMERGENCY_DIR, "backup.dir");

    /**
     * Key for different types of clients to configure the Dispatcher for.
     */
    public static final String DISPATCHER_CLIENT_TYPES = join(ROOT_DISPATCHER_NAMESPACE, "types");

    private static String join(String parentNS, String childNS) {
        validateNameSpace(parentNS);
        validateNameSpace(childNS);
        return parentNS + "." + childNS;
    }

    /**
     * Validates a given namespace.
     *
     * @param nameSpace The String representation of the namespace to validate.
     */
    private static void validateNameSpace(String nameSpace) {
        if (nameSpace == null)
            throw new NullPointerException("Name Space cannot be null");
        if (nameSpace.trim().isEmpty())
            throw new IllegalArgumentException("Name Space cannot be empty or just white space.");

    }

}
