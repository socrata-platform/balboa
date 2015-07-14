package com.socrata.balboa.metrics;

import com.socrata.balboa.metrics.data.BalboaFastFailCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs periodic checks and signalling within Balboa. Currently
 * just integrates with the BalboaFastFailCheck to start and stop
 * services in response to DataStore issues.
 */
public class WatchDog {
    private final static long CHECK_INTERVAL_MS = 1000;
    private static Logger log = LoggerFactory.getLogger(WatchDog.class);

    /**
     * Receive signals from the WatchDog when failures
     * or interrupts occur. Multiple signals may be sent.
     */
    public interface WatchDogListener {
        public void onStart();
        public void onStop();
        public void heartbeat();
        public void ensureStarted();
    }

    public void watchAndWait(WatchDogListener ... listeners) throws InterruptedException {
        log.debug("Starting watch");
        BalboaFastFailCheck failCheck = BalboaFastFailCheck.getInstance();
        log.debug("Fail");
        while(true)
        {
            log.debug("Sleeping for: " + CHECK_INTERVAL_MS);
            Thread.sleep(CHECK_INTERVAL_MS);
            log.debug("Checking");
            check(failCheck, listeners);
        }
    }

    public void check(BalboaFastFailCheck failCheck, WatchDogListener ... listeners) {
        // When we are in a failure mode, we will send multiple on* messages until exiting.
        if (failCheck.isInFailureMode()) {
            log.debug("Is in failure mode");
            if (failCheck.proceed()) {
                log.debug("Proceed");
                // We are in a failure mode; but the backoff time has expired, so we restart
                // things tenatively.
                log.debug("Starting all");
                for (WatchDogListener listener : listeners)
                    listener.onStart();
            } else {
                // We are in a failure mode;
                log.debug("Keep on failing");
                for (WatchDogListener listener : listeners)
                    listener.onStop();
            }
        } else {
            log.debug("Ensure all listeners started");
            for (WatchDogListener listener : listeners)
                listener.ensureStarted();
            log.debug("We checked on them allq");
        }

        log.debug("Heartbeating");
        for (WatchDogListener listener : listeners)
            listener.heartbeat();
        log.debug("Heartbeats complete");
    }
}
