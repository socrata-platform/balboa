package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.data.CompoundIterator;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.metrics.data.QueryOptimizer;
import com.socrata.balboa.metrics.measurements.combining.Combinator;
import com.socrata.balboa.metrics.measurements.combining.Summation;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SqlDataStore extends DataStoreImpl implements DataStore
{
    private static Log log = LogFactory.getLog(SqlDataStore.class);
    
    static class SqlIterator implements Iterator<Summary>
    {
        Connection connection;
        String query;
        Map<Integer, Object> params;

        List<Summary> buffer;

        SqlIterator(Connection connection, String query, Map<Integer, Object> params) throws SQLException
        {
            this.connection = connection;
            this.query = query;
            this.params = params;

            buffer = getSummaries();
        }

        List<Summary> getSummaries() throws SQLException
        {
            final List<Summary> summaries = new ArrayList<Summary>();
            final DateRange.Type type = (DateRange.Type)params.get("tier");
            final Combinator com = new Summation();

            PreparedStatement statement = null;
            ResultSet results = null;
            try
            {
                statement = connection.prepareStatement(query);

                for (Map.Entry<Integer, Object> p : params.entrySet())
                {
                    if (p.getValue() instanceof Date)
                    {
                        statement.setLong(p.getKey(), ((Date)p.getValue()).getTime());
                    }
                    else if (p.getValue() instanceof Number)
                    {
                        statement.setLong(p.getKey(), ((Number)p.getValue()).longValue());
                    }
                    else
                    {
                        statement.setObject(p.getKey(), p.getValue());
                    }
                }
                
                results = statement.executeQuery();

                Summary current = null;
                while (results.next())
                {
                    long timestamp = results.getLong("timestamp");

                    if (current == null || timestamp != current.getTimestamp())
                    {
                        current = new Summary(type, timestamp, new HashMap<String, Object>());
                        summaries.add(current);
                    }

                    String metric = results.getString("metric");
                    if (!current.getValues().containsKey(metric))
                    {
                        current.getValues().put(metric, results.getObject("value"));
                    }
                    else
                    {
                        current.getValues().put(
                                metric,
                                com.combine(results.getObject("value"), current.getValues().get(metric))
                        );
                    }
                }
            }
            finally
            {
                if (results != null)
                {
                    results.close();
                }

                if (statement != null)
                {
                    statement.close();
                }
            }

            return summaries;
        }

        @Override
        public boolean hasNext()
        {
            if (buffer == null || buffer.size() == 0)
            {
                return false;
            }

            return true;
        }

        @Override
        public Summary next()
        {
            return buffer.remove(0);
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException("Not supported.");
        }
    }

    static class Operation
    {
        Collection<String> updates;
        Collection<String> inserts;

        Operation(Collection<String> updates, Collection<String> inserts)
        {
            this.updates = updates;
            this.inserts = inserts;
        }
    }

    Iterator<Summary> query(String entityId, DateRange.Type type, DateRange range) throws IOException
    {
        return SqlQueryFactory.get().query(entityId, type, range);
    }

    @Override
    public Iterator<Summary> find(String entityId, final DateRange.Type type, final Date date) throws IOException
    {
        DateRange range = DateRange.create(type, date);

        return query(entityId, type, range);
    }

    @Override
    public Iterator<Summary> find(String entityId, DateRange.Type type, Date start, Date end) throws IOException
    {
        return query(entityId, type, new DateRange(start, end));
    }

    @Override
    public Iterator<Summary> find(String entityId, Date start, Date end) throws IOException
    {
        DateRange range = new DateRange(start, end);
        QueryOptimizer optimizer = new QueryOptimizer();
        Map<DateRange.Type,  Set<DateRange>> slices = optimizer.optimalSlices(range.start, range.end);

        int numberOfQueries = 0;

        CompoundIterator iter = new CompoundIterator();
        for (Map.Entry<DateRange.Type,  Set<DateRange>> slice : slices.entrySet())
        {
            numberOfQueries += slice.getValue().size();

            for (DateRange r : slice.getValue())
            {
                iter.add(query(entityId, slice.getKey(), r));
            }
        }

        log.debug("Range scanned with " + numberOfQueries + " queries (lower is better).");

        return iter;
    }

    static class TierUpdate implements Callable<Exception>
    {
        Connection connection;
        String entity;
        DateRange.Type type;
        DateRange aligned;
        Map<String, Object> data;

        TierUpdate(Connection connection, String entity, DateRange.Type type, DateRange aligned, Map<String, Object> data)
        {
            this.connection = connection;
            this.entity = entity;
            this.type = type;
            this.aligned = aligned;
            this.data = data;
        }

        Operation getOperations(Iterator<Summary> iter, Map<String, Object> data) throws IOException
        {
            Collection<String> updates;
            Collection<String> inserts;
            if (iter.hasNext())
            {
                // If there was a summary that we got back from the lock, we
                // need to update any metrics that already exist.
                Summary existing = iter.next();

                if (iter.hasNext())
                {
                    // After reading the first summary there should be no more.
                    throw new IOException("There's more than one entity in the result set from a lock. This should never happen.");
                }

                updates = CollectionUtils.intersection(existing.getValues().keySet(), data.keySet());
                inserts = CollectionUtils.subtract(data.keySet(), existing.getValues().keySet());
            }
            else
            {
                // There were no existing summaries so we need to do a bunch of
                // inserts and no updates.
                updates = new ArrayList<String>(0);
                inserts = data.keySet();
            }

            return new Operation(updates, inserts);
        }

        @Override
        public Exception call() throws Exception
        {
            try
            {
                // We don't actually want a summary iterator here. We should only
                // receive back one Summary object from this iterator, but we need
                // the iterator to combine all of the different metrics into one map
                Iterator<Summary> iter = SqlQueryFactory.get().query(connection, entity, type, aligned);

                Operation ops = getOperations(iter, data);
                SqlQueryFactory.get().persist(connection, entity, aligned.start.getTime(), data, type, ops);

                // Signal that we have in fact accomplished our mission.
                return null;
            }
            catch (Exception e)
            {
                log.debug("Unhandled exception in update/insert thread.");
                return e;
            }
        }
    }

    @Override
    public void persist(String entityId, Summary summary) throws IOException
    {
        if (entityId.startsWith("__") && entityId.endsWith("__"))
        {
            throw new IllegalArgumentException("Unable to persist entities that start and end with two underscores '__'. These entities are reserved for meta data.");
        }

        if (summary.getType() != DateRange.Type.REALTIME)
        {
            // TODO: Necessary? Could I just summarize above the current? Doing
            // that could result in potentially inconsistent data, though...
            throw new IllegalArgumentException("Unable to persist anything but realtime events with this data store.");
        }

        Connection connection = null;
        ExecutorService pool = null;
        
        try
        {
            connection = SqlConnectionPool.get().getConnection();
            connection.setAutoCommit(false);

            List<DateRange.Type> types = Configuration.get().getSupportedTypes();
            pool = Executors.newFixedThreadPool(types.size());
            List<Future<Exception>> results = new ArrayList<Future<Exception>>(types.size());
            DateRange.Type type = DateRange.Type.leastGranular(types);
            while (type != null && type != DateRange.Type.REALTIME)
            {
                DateRange aligned = DateRange.create(type, new Date(summary.getTimestamp()));

                Future<Exception> thread = pool.submit(new TierUpdate(connection, entityId, type, aligned, summary.getValues()));
                results.add(thread);

                type = type.moreGranular();
                while (type != null && !types.contains(type))
                {
                    type = type.moreGranular();
                }
            }

            // Wait for all our threads to finish and abort the transaction if
            // any of them fail for some reason.
            for (Future<Exception> thread : results)
            {
                Exception e = thread.get();

                if (e != null)
                {
                    throw e;
                }
            }

            connection.commit();
        }
        catch (Exception e)
        {
            if (connection != null)
            {
                try
                {
                    connection.rollback();
                }
                catch (SQLException e1)
                {
                }
            }

            throw new IOException("There was a problem persisting a summary to the database.", e);
        }
        finally
        {
            if (pool != null)
            {
                pool.shutdown();
            }

            try
            {
                connection.close();
            }
            catch (SQLException e1)
            {
            }
        }
    }
}
