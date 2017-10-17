package com.socrata.balboa.jms.activemq;

import com.socrata.balboa.metrics.WatchDog;
import com.socrata.balboa.metrics.data.DataStore;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.naming.NamingException;
import java.util.ArrayList;
import java.util.List;

/**
 * The ActiveMQReceiver listens to the JMS queue/server configured in your
 * configuration and tries to save events that are put on the queue. Messages on
 * the queue should be text messages and their body should be a JSON encoded
 * object of the event.
 *
 * The entityId is the entity on whom metrics are being tracked. The timestamp
 * is the time, in milliseconds, when the metrics themselves occurred. And
 * finally "metrics" is the collection of metrics.
 *
 * e.g.
 *
 * <code>
 *     {
 *         "entityId": "foo",
 *         "timestamp": 1281037944000,
 *         "metrics": {
 *              "view-loaded" : {
 *                  "value": 104,
 *                  "type": AGGREGATE
 *              },
 *              "view-downloaded" : {
 *                  "value": 10,
 *                  "type": ABSOLUTE
 *              },
 *         }
 *     }
 * </code>
 *
 * Here the key "foo" will persist at the timestamp the two metrics "view-count"
 * and "view-downloaded".
 *
 * If for some reason a message cannot be persisted, it will be rejected and
 * redelivered as the JMS provider sees fit.
 */
public class ActiveMQReceiver implements WatchDog.WatchDogListener
{
    private static Logger log = LoggerFactory.getLogger(ActiveMQReceiver.class);
    private List<Listener> listeners;

    volatile boolean stopped = false;

    public ActiveMQReceiver(String[] servers, String channel, int threads, DataStore ds) throws NamingException, JMSException
    {
        listeners = new ArrayList<>(servers.length * threads);
        for (String server : servers) {
            log.info("Starting server {}",server);
            for (int i=0; i < threads; i++) {
                ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(server);
                factory.setTransportListener(new TransportLogger());

                listeners.add(new Listener(factory, channel, ds));
            }
        }

        log.info("Listeners all started.");
    }

    // Used to restart the receiver on DataStore failure
    public void onStart() {
        synchronized (this) {
            stopped = false;
            for (Listener listener : listeners)
                listener.restart();
        }
    }

    // Used to stop all listeners, whether they are already stopped
    // or not.
    public void onStop() {
        synchronized (this) {
            stopped = true;
            for (Listener listener : listeners)
                listener.stop();
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
