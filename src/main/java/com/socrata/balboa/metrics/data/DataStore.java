package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.Summary.Type;
import java.util.Date;
import java.util.Iterator;

public interface DataStore
{
    public Iterator<Summary> find(String entityType, String entityId, Type type, Date date);
    public void persist(String entityId, Summary summary);
}
