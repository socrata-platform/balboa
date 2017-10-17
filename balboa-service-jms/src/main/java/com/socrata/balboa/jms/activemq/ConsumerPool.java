package com.socrata.balboa.jms.activemq;

import com.socrata.balboa.metrics.WatchDog;
import com.socrata.balboa.metrics.data.DataStore;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.naming.NamingException;
import java.util.ArrayList;
import java.util.List;

/**
 * The ConsumerPool listens to the JMS queue/server configured in your
 * configuration and tries to save events that are put on the queue. Messages on
 * the queue should be text messages and their body should be a JSON encoded
 * object of the event.
 * <p>
 * The entityId is the entity on whom metrics are being tracked. The timestamp
 * is the time, in milliseconds, when the metrics themselves occurred. And
 * finally "metrics" is the collection of metrics.
 * <p>
 * e.g.
 * <p>
 * <code>
 *  {
 *      "entityId": "foo",
 *      "timestamp": 1281037944000,
 *      "metrics": {
 *          "view-loaded" : {
 *              "value": 104,
 *              "type": AGGREGATE
 *          },
 *          "view-downloaded" : {
 *              "value": 10,
 *              "type": ABSOLUTE
 *          },
 *      }
 *  }
 * </code>
 * <p>
 * Here the key "foo" will persist at the timestamp the two metrics "view-count"
 * and "view-downloaded".
 * <p>
 * If for some reason a message cannot be persisted, it will be rejected and
 * redelivered as the JMS provider sees fit.
 */
public class ConsumerPool implements WatchDog.WatchDogListener {
    private static Logger log = LoggerFactory.getLogger(ConsumerPool.class);
    private List<Consumer> consumers;

    volatile boolean stopped = false;

    public ConsumerPool(String[] servers, String channel, int threads, DataStore ds) throws NamingException, JMSException {
        consumers = new ArrayList<>(servers.length * threads);

        for (String server : servers) {
            log.info("Starting consumer for {}", server);
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(server);
            factory.setTransportListener(new TransportLogger());

            for (int i = 0; i < threads; i++) {
                ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();
                consumers.add(new Consumer(connection, channel, ds));
            }
        }

        log.info("All consumers started");
    }

    // Used to restart the receiver on DataStore failure
    public void onStart() {
        synchronized (this) {
            stopped = false;
            for (Consumer consumer : consumers)
                consumer.restart();
        }
    }

    // Used to stop all consumers, whether they are already stopped
    // or not.
    public void onStop() {
        synchronized (this) {
            stopped = true;
            for (Consumer consumer : consumers)
                consumer.stop();
        }
    }

    @Override
    public void heartbeat() {
        // noop
    }

    public void ensureStarted() {
        if (stopped) {
            onStart();
        }
    }
}
