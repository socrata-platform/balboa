package com.blist.metrics.impl.queue;

import com.socrata.balboa.metrics.Metric;
import com.socrata.metrics.IdParts;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

/** Prefer to use MetricJmsQueueNotSingleton if possible. */
public class MetricJmsQueue extends AbstractJavaMetricQueue {

    // TODO Merge the Singleton + NonSingleton

    private static final Logger log = LoggerFactory.getLogger(MetricJmsQueue.class);
    private static volatile MetricJmsQueue instance;
    private static boolean loggedFailToCreateOnce = false;
    private final ConnectionFactory factory;
    private final MetricJmsQueueNotSingleton realInstance;

    private static synchronized void createInstance(String server, String queueName) {
        if (instance == null) {
            try {
                ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(server);
                factory.setUseAsyncSend(true);
                instance = new MetricJmsQueue(factory, queueName);
            } catch (JMSException e) {
                if(loggedFailToCreateOnce) e = null;
                else loggedFailToCreateOnce = true;
                log.error("Unable to create a new Metric logger for JMS. Falling back to a NOOP logger.", e);
            }
        }
    }

    // Ugh really did not want to contribute to this design pattern.  This has caused us so much pain in the past.
    // Globally visible instance that many things can touch == future sustainability problem.
    private static synchronized void createInstance(String username, String password, String server, String queueName) {
        if (instance == null) {
            try {
                ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(username, password, server);
                factory.setUseAsyncSend(true);
                instance = new MetricJmsQueue(factory, queueName);
            } catch (JMSException e) {
                if(loggedFailToCreateOnce) e = null;
                else loggedFailToCreateOnce = true;
                log.error("Unable to create a new Metric logger for JMS. Falling back to a NOOP logger.", e);
            }
        }
    }

    /** Note that the parameters are only used on the first invocation. */
    public static MetricJmsQueue getInstance(String server, String queueName) {
        if (instance == null) createInstance(server, queueName);
        return instance;
    }

    /** Note that the parameters are only used on the first invocation. */
    public static MetricJmsQueue getInstance(String username, String password, String server, String queueName) {
        if (instance == null) createInstance(username, password, server, queueName);
        return instance;
    }

    private MetricJmsQueue(ConnectionFactory factory, String queueName) throws JMSException {
        this.factory = factory; // badness happens if it's GC'd
        Connection connection = factory.createConnection();
        realInstance = new MetricJmsQueueNotSingleton(connection, queueName);
    }

    @Override
    public void close() throws Exception {
        if (instance != null)
            instance.close();
        if (realInstance != null)
            realInstance.close();
    }

    @Override
    public void create(IdParts entity, IdParts name, long value, long timestamp, Metric.RecordType type) {
        realInstance.create(entity, name, value, timestamp, type);
    }

}
