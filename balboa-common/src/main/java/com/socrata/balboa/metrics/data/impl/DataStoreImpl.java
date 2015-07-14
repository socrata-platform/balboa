package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.data.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DataStoreImpl implements DataStore
{
    private static final Logger log = LoggerFactory.getLogger(DataStoreImpl.class);

    public void heartbeat() {
        log.debug("Heartbeating datastore");
    }

    public void onStart() {
        log.debug("Received start message from watchdog");
    }

    public void onStop() { log.debug("Recieved stop message from watchdog"); }

    public void ensureStarted() {
        log.debug("Ensuring started datastore");
    }
}
