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

    Period getClosestTypeOrError(Period period) throws IOException
    {
        Period originalPeriod = period;

        List<Period> periods;
        try
        {
            periods = Configuration.get().getSupportedPeriods();
        }
        catch (IOException e)
        {
            throw new PropertiesConfiguration.ConfigurationException("Unable to load configuration for some reason.", e);
        }

        while (!periods.contains(period) && period != null)
        {
            period = period.moreGranular();
        }

        if (period == null)
        {
            throw new IOException("There are no supported summarization periods.");
        }

        if (period != originalPeriod)
        {
            log.debug("Originally requested a " + originalPeriod + " summary, but the closest supported level is " + period + ". Range scanning that instead.");
        }

        return period;
    }

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
