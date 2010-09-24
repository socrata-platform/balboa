package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.data.DateRange;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SqlQueryImpl implements SqlQuery
{
    private static Log log = LogFactory.getLog(SqlQueryImpl.class);

    @Override
    public void persist(String entityId, long timestamp, Map<String, Object> data, DateRange.Type type, SqlDataStore.Operation ops) throws IOException
    {
        Connection connection = null;
        try
        {
            connection = SqlConnectionPool.get().getConnection();
            persist(connection, entityId, timestamp, data, type, ops);
        }
        catch (SQLException e)
        {
            throw new IOException("Unable to connect to the SQL server.", e);
        }
        finally
        {
            if (connection != null)
            {
                try
                {
                    connection.close();
                }
                catch (SQLException e)
                {
                    throw new IOException("Unable to close the connection in a finally block.", e);
                }
            }
        }
    }

    @Override
    public void persist(Connection connection, String entityId, long timestamp, Map<String, Object> data, DateRange.Type type, SqlDataStore.Operation ops) throws IOException
    {
        long startTime = System.currentTimeMillis();
        
        try
        {
            if (ops.updates.size() > 0)
            {
                log.debug("Creating an update batch for " + ops.updates.size());

                // Batch all updates into a single query.
                String updateMetric = "update " + type + " set value = value + ? where entity = ? and metric = ? and timestamp = ?";
                PreparedStatement update = connection.prepareStatement(updateMetric);
                for (String metric : ops.updates)
                {
                    update.setLong(1, ((Number)data.get(metric)).longValue());
                    update.setString(2, entityId);
                    update.setString(3, metric);
                    update.setLong(4, timestamp);
                    update.addBatch();
                }

                try
                {
                    update.executeBatch();
                }
                finally
                {
                    update.close();
                }
            }

            // Batch all the inserts into a single query.
            String insertMetric = "insert into " + type + " (entity, metric, value, timestamp) values (?, ?, ?, ?)";
            PreparedStatement insert = connection.prepareStatement(insertMetric);
            for (String metric : ops.inserts)
            {
                insert.setString(1, entityId);
                insert.setString(2, metric);
                insert.setLong(3, ((Number)data.get(metric)).longValue());
                insert.setLong(4, timestamp);
                insert.addBatch();
            }

            try
            {
                insert.executeBatch();
            }
            finally
            {
                insert.close();
            }
        }
        catch (SQLException e)
        {
            throw new IOException("There was a problem persisting to the SQL database.", e.getNextException());
        }
        finally
        {
            long totalTime = System.currentTimeMillis() - startTime;
            log.debug("Total write time for '" + entityId + "' to sql " + totalTime + " (ms)");
        }
    }

    @Override
    public Iterator<Summary> query(Connection connection, String entity, DateRange.Type type, DateRange range) throws IOException
    {
        long startTime = System.currentTimeMillis();
        
        String findEntityRangeInTier = "select * from " + type + " where timestamp between ? and ? and entity = ?";

        Map<Integer, Object> params = new HashMap<Integer, Object>();
        params.put(1, range.start);
        params.put(2, range.end);
        params.put(3, entity);

        try
        {
            return new SqlDataStore.SqlIterator(connection, findEntityRangeInTier, params);
        }
        catch (SQLException e)
        {
            throw new IOException("There was a problem executing a query.", e);
        }
        finally
        {
            long totalTime = System.currentTimeMillis() - startTime;
            log.debug("Total read time for '" + entity + "' to sql " + totalTime + " (ms).");
        }
    }

    @Override
    public Iterator<Summary> query(String entity, DateRange.Type type, DateRange range) throws IOException
    {
        Connection connection = null;
        
        try
        {
            connection = SqlConnectionPool.get().getConnection();
            return query(connection, entity, type, range);
        }
        catch (SQLException e)
        {
            throw new IOException("Unable to connect to the SQL server.", e);
        }
        finally
        {
            if (connection != null)
            {
                try
                {
                    connection.close();
                }
                catch (SQLException e)
                {
                    throw new IOException("Unable to close the connection in a finally block.", e);
                }
            }
        }
    }
}
