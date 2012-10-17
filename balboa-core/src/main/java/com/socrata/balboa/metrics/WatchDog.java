package com.socrata.balboa.metrics;

import com.socrata.balboa.metrics.data.BalboaFastFailCheck;

/**
 * Performs periodic checks and signalling within Balboa. Currently
 * just integrates with the BalboaFastFailCheck to start and stop
 * services in response to DataStore issues.
 */
public class WatchDog {
    private final static long CHECK_INTERVAL_MS = 1000;

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
        BalboaFastFailCheck failCheck = BalboaFastFailCheck.getInstance();
        while(true)
        {
            Thread.sleep(CHECK_INTERVAL_MS);
            check(failCheck, listeners);
        }
    }

    public void check(BalboaFastFailCheck failCheck, WatchDogListener ... listeners) {
        // When we are in a failure mode, we will send multiple on* messages until exiting.
        if (failCheck.isInFailureMode()) {
            if (failCheck.proceed()) {
                // We are in a failure mode; but the backoff time has expired, so we restart
                // things tenatively.
                for (WatchDogListener listener : listeners)
                    listener.onStart();
            } else {
                // We are in a failure mode;
                for (WatchDogListener listener : listeners)
                    listener.onStop();
            }
        } else {
            for (WatchDogListener listener : listeners)
                listener.ensureStarted();
        }
        for (WatchDogListener listener : listeners)
            listener.heartbeat();
    }
}
