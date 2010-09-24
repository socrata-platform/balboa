package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.server.exceptions.InternalException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;

public abstract class DataStoreImpl
{
    private static Log log = LogFactory.getLog(DataStoreImpl.class);

    DateRange.Type getClosestTypeOrError(DateRange.Type type) throws IOException
    {
        DateRange.Type originalType = type;

        List<DateRange.Type> types;
        try
        {
            types = Configuration.get().getSupportedTypes();
        }
        catch (IOException e)
        {
            throw new InternalException("Unable to load configuration for some reason.", e);
        }

        while (!types.contains(type) && type != null)
        {
            type = type.moreGranular();
        }

        if (type == null)
        {
            throw new IOException("There are no supported summarization types.");
        }

        if (type != originalType)
        {
            log.debug("Originally requested a " + originalType + " summary, but the closest supported level is " + type + ". Range scanning that instead.");
        }

        return type;
    }
}
