package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.config.PropertiesConfiguration;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.metrics.data.Period;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;

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
}
