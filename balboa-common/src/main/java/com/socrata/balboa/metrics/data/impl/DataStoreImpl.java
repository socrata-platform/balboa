package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.common.logging.JavaBalboaLogging;
import com.socrata.balboa.metrics.data.DataStore;
import org.slf4j.Logger;

// TODO Why is an abstract class called Implementation.

public abstract class DataStoreImpl implements DataStore
{
    private static final Logger log = JavaBalboaLogging.instance(DataStoreImpl.class);

    public void heartbeat() {
        // noop
    }

    public void onStart() {
        log.error("Received start message from watchdog");
    }

    public void onStop() {
        log.error("Recieved stop message from watchdog");
    }

    public void ensureStarted() {
        //noop
    }
}
