package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.Summary;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

public interface DataStore
{
    public Iterator<Summary> find(String entityId, DateRange.Type type, Date date);
    public Iterator<Summary> find(String entityId, DateRange.Type type, Date start, Date end);
    public void persist(String entityId, Summary summary) throws IOException;
}
