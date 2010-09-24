package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.data.DateRange;

import java.io.IOException;
import java.sql.Connection;
import java.util.Iterator;
import java.util.Map;

public interface SqlQuery
{
    public Iterator<Summary> query(Connection connection, String entity, DateRange.Type type, DateRange range) throws IOException;
    public Iterator<Summary> query(String entity, DateRange.Type type, DateRange range) throws IOException;
    public void persist(Connection connection, String entityId, long timestamp, Map<String, Object> data, DateRange.Type type, SqlDataStore.Operation ops) throws IOException;
    public void persist(String entityId, long timestamp, Map<String, Object> data, DateRange.Type type, SqlDataStore.Operation ops) throws IOException;
}
