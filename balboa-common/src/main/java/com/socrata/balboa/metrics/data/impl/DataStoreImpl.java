package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.data.DataStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class DataStoreImpl implements DataStore
{
    private static Log log = LogFactory.getLog(DataStoreImpl.class);

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
