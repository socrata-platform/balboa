package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.Metric;
import com.socrata.balboa.metrics.Metrics;
import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.data.*;
import com.socrata.balboa.metrics.data.DateRange.Period;
import com.socrata.balboa.metrics.measurements.serialization.Serializer;
import com.socrata.balboa.metrics.measurements.serialization.SerializerFactory;
import com.socrata.balboa.server.exceptions.InternalException;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

/**
 * A DataStore implementation using <a href="http://cassandra.apache.org/">
 * cassandra</a> as the storage backend.
 *
 * The cassandra data model defines a keyspace in the balboa configuration file
 * (cassandra.keyspace). Underneath the keyspace, this datastore expects there
 * to be some super column ColumnFamilies. There should be a ColumnFamily for
 * each configured summary date range period (<code>DateRange.Period</code>).
 *
 * e.g.
 *
 * <code>
 *     &lt;ColumnFamily Name="hourly" ColumnType="Super" CompareWith="LongType" /&gt;
 *     &lt;ColumnFamily Name="daily" ColumnType="Super" CompareWith="LongType" /&gt;
 * </code>
 *
 * The keys for the ColumnFamily should be packed longs that are the timestamp
 * for the summary in milliseconds. The timestamp for the summary period should,
 * by convention always be the first millisecond of the time period, but in
 * practice the data store doesn't care as log as the timestamp is within the
 * time period.
 *
 * The organization of the data is most easily illustrated by some pseudo json:
 *
 * <code>
 *     {
 *         // The entity id (the thing for which you're storing stats) is the
 *         // cassandra row id.
 *         #{entityId}: {
 *             // Under the entity id is the super columns which are the various
 *             // summary tiers (i.e. hourly, secondly, yearly, etc.)
 *             hourly: { ... },
 *             daily: {
 *                 // The keys for each super column is the timestamp for the
 *                 // summary of items for that time slice...
 *                 1111111111: {
 *                     // The keys of the sub columns are the metric names and
 *                     // their value is the accumulated metric value.
 *                     metric1: 123,
 *                     metric2: 1,
 *                     metric3: 8239.32
 *                 }
 *             }
 *         }
 *     }
 * </code>
 *
 * When a set of data gets persisted, the datastore first locks the row that its
 * going to persist to then walks up each summary level and combines the values
 * to be written with the current values. Because of this, summary tiers are
 * always as up to date as the events that have been written are and reads are
 * extremely cheap (typically, even over a long range query with the default
 * summary range period configuration, balboa won't need to read more than seven
 * items). Writes on the other hand are relatively expensive, since each write
 * requires 'n' updates (where 'n' is the number of summary range types that are
 * enabled).
 *
 * Writes also have to lock the entire row because of the way column writes are
 * batched so some write nodes will block on writing until they can acquire a
 * lock and lock contention could be a problem on very active entity's. 
 */
public class CassandraDataStore extends DataStoreImpl implements DataStore
{
    /** The maximum number of retries to acquire a lock before giving up */
    private static final int MAX_RETRIES = 5;

    private static Log log = LogFactory.getLog(CassandraDataStore.class);

    public static class CassandraEntityMeta implements EntityMeta
    {
        Map<String, String> data;

        public CassandraEntityMeta()
        {
            data = new HashMap<String, String>();
        }

        public CassandraEntityMeta(SuperColumn superColumn)
        {
            data = new HashMap<String, String>(superColumn.getColumnsSize());

            for (Column column : superColumn.getColumns())
            {
                data.put(
                    new String(column.getName(), Charset.forName("UTF-8")),
                    new String(column.getValue(), Charset.forName("UTF-8"))
                );
            }
        }

        @Override
        public boolean containsKey(String metric)
        {
            return data.containsKey(metric);
        }

        @Override
        public String get(String metric)
        {
            return data.get(metric);
        }
    }

    /**
     * Thrown in the event that there's a problem with the query speaking
     * to cassandra.
     */
    public static class CassandraQueryException extends RuntimeException
    {
        public CassandraQueryException(String msg, Throwable cause)
        {
            super(msg, cause);
        }
    }

    /**
     * An iterator that pages cassandra rows in memory and loops over all of them
     * until there are no more remaining.
     */
    static class CassandraIterator implements Iterator<Metrics>
    {
        /** The maxiumum number of super columns to read in one query. */
        static final int QUERYBUFFER = 500;

        String entityId;
        Period period;
        DateRange range;
        EntityMeta meta;

        List<SuperColumn> buffer;

        /**
         *
         * @param entityId The row id to query.
         * @param period The period to look for. This determines which super column
         * should be searched for rows.
         * @param range The date range to constrain the search to.
         */
        CassandraIterator(String entityId, DateRange.Period period, DateRange range) throws IOException
        {
            this.period = period;
            this.range = range;
            this.entityId = entityId;
            this.meta = getEntityMeta(entityId); 

            buffer = new ArrayList<SuperColumn>(0);
        }

        EntityMeta getEntityMeta(String entityId) throws IOException
        {
            // TODO: Cachinate.
            SuperColumn data = CassandraQueryFactory.get().getMeta(entityId);

            if (data != null)
            {
                return new CassandraEntityMeta(data);
            }
            else
            {
                return new CassandraEntityMeta();
            }
        }

        /**
         * Create the search predicate for all super columns which date between the
         * date ranges.
         */
        SlicePredicate createPredicate(DateRange dateRange) throws IOException
        {
            SliceRange queryRange = new SliceRange(
                    CassandraUtils.packLong(dateRange.start.getTime()),
                    CassandraUtils.packLong(dateRange.end.getTime()),
                    false,
                    QUERYBUFFER
                    );

            SlicePredicate predicate = new SlicePredicate();
            predicate.setSlice_range(queryRange);

            return predicate;
        }
        
        /**
         * Fill the buffer after it's empty with new items from a cassandra
         * query.
         */
        public List<SuperColumn> nextBuffer()
        {
            // Make sure that we're not executing a query when we've still got
            // some unprocessed buffer left.
            if (buffer != null)
            {
                assert(buffer.size() == 0);
            }

            // Don't perform the query if we're already past the end of the buffer.
            if (range.start.getTime() > range.end.getTime())
            {
                return null;
            }

            try
            {
                // Create the predicate and execute the query.
                SlicePredicate predicate = createPredicate(range);

                List<SuperColumn> results = CassandraQueryFactory.get().find(entityId, predicate, period);

                // Update the range so that the next time we fill the buffer, we
                // do it starting from the last of the returned results.
                if (results != null && results.size() > 0)
                {
                    SuperColumn last = results.get(results.size() - 1);

                    // Add one to the last result's timestamp. This should never
                    // cause any summary to be skipped over since we have a quantum
                    // of time (1 nano/mill/whatever second) that's our highest
                    // resolution. In practice, for anything other than the realtime
                    // period, there should only be one summary per period.
                    range.start = new Date(CassandraUtils.unpackLong(last.getName()) + 1);

                    return results;
                }
                else
                {
                    return null;
                }
            }
            catch (IOException e)
            {
                // Well, no matter what happened that caused an exception, we
                // don't have any items to iterate over.
                throw new CassandraQueryException("There was some serious problem reading from cassandra.", e);
            }
        }

        @Override
        public boolean hasNext()
        {
            if (buffer == null || buffer.size() == 0)
            {
                buffer = nextBuffer();
            }

            if (buffer == null || buffer.size() == 0)
            {
                return false;
            }

            return true;
        }

        @Override
        public Metrics next()
        {
            if (!hasNext())
            {
                throw new NoSuchElementException("There are no more summaries.");
            }
            else
            {
                SuperColumn column = buffer.remove(0);
                Serializer ser = SerializerFactory.get();

                // When we query cassandra we get back a set of columns (the
                // children of the super column -- period -- on which we queried).
                // We need to take all of these columns and map them into a hash
                // which the summary uses as its values.
                Metrics metrics = new Metrics(column.getColumnsSize());

                try
                {
                    metrics.setTimestamp(CassandraUtils.unpackLong(column.getName()));
                }
                catch (IOException e)
                {
                    throw new InternalException("Invalid column name, unable to unpack into timestamp.", e);
                }

                for (Column subColumn : column.getColumns())
                {
                    String name = new String(subColumn.getName());

                    try
                    {
                        if (log.isTraceEnabled())
                        {
                            log.trace("READ: Raw serialized value from cassandra (" +
                                    subColumn.getTimestamp() + ") " +
                                    Arrays.toString(subColumn.getValue()));
                        }

                        Metric.RecordType type = Metric.RecordType.AGGREGATE;
                        if (meta.containsKey(name))
                        {
                            type = Metric.RecordType.valueOf(meta.get(name).toUpperCase());
                        }

                        Metric m = new Metric(
                                type,
                                (Number)ser.deserialize(subColumn.getValue())
                        );
                        
                        metrics.put(name, m);
                    }
                    catch (IOException e)
                    {
                        log.error("(" + entityId + " -> " + range + ") Unable " +
                                "to deserialize the value '" + Arrays.toString(subColumn.getValue()) +
                                "' in the column '" + name + "'. Ignoring " +
                                "this metric.");
                    }
                }

                return metrics;
            }
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException("Not supported.");
        }
    }

    Iterator<Metrics> query(String entityId, DateRange.Period period, DateRange range) throws IOException
    {
        return new CassandraIterator(entityId, period, range);
    }

    @Override
    public Iterator<Metrics> find(String entityId, DateRange.Period period, Date start, Date end) throws IOException
    {
        DateRange range = new DateRange(start, end);
        
        return query(entityId, getClosestTypeOrError(period), range);
    }
    
    @Override
    public Iterator<Metrics> find(String entityId, Period period, Date date) throws IOException
    {
        DateRange range = DateRange.create(period, date);

        return query(entityId, getClosestTypeOrError(period), range);
    }

    @Override
    public Iterator<Metrics> find(String entityId, Date start, Date end) throws IOException
    {
        DateRange range = new DateRange(start, end);
        QueryOptimizer optimizer = new QueryOptimizer();
        Map<Period,  List<DateRange>> slices = optimizer.optimalSlices(range.start, range.end);

        int numberOfQueries = 0;

        CompoundIterator<Metrics> iter = new CompoundIterator();
        for (Map.Entry<DateRange.Period,  List<DateRange>> slice : slices.entrySet())
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

    SuperColumn getSuperColumnMutation(long timestamp, Map<String, Metric> metrics) throws IOException
    {
        List<Column> columns = new ArrayList<Column>(metrics.size());
        Serializer ser = SerializerFactory.get();
        for (String metricName : metrics.keySet())
        {
            if (metricName.startsWith("__") && metricName.endsWith("__"))
            {
                throw new IllegalArgumentException("Unable to persist metrics " +
                    "that start and end with two underscores '__'. These " +
                    "entities are reserved for meta data.");
            }

            // This is terribly confusing, unfortunately: We have two concepts
            // of timestamps: The first is the timestamp that the metric
            // occurred on (adjusted to be on the period-barrier). The second is
            // the internal timestamp that Cassandra uses on all of it it's
            // columns. THIS IS THAT TIMESTAMP. We cannot backdate that
            // timestamp and it's in microseconds instead of milliseconds.
            long cassandraTimestamp = System.currentTimeMillis() * 1000;
            
            if (log.isTraceEnabled())
            {
                log.trace("WRITE: Writing byte array to cassandra (" +
                        cassandraTimestamp + ")" +
                        Arrays.toString(ser.serialize(metrics.get(metricName).getValue())));
            }
            Column column = new Column(
                    metricName.getBytes(),
                    ser.serialize(metrics.get(metricName).getValue()),
                    cassandraTimestamp
            );

            columns.add(column);
        }

        SuperColumn superColumn = new SuperColumn(CassandraUtils.packLong(timestamp), columns);

        return superColumn;
    }

    public Map<String, List<SuperColumn>> update(String entityId, long timestamp, Metrics metrics) throws IOException
    {
        Map<String, List<SuperColumn>> superColumnOperations = new HashMap<String, List<SuperColumn>>();

        // For every period that can be summarized read/increment/update
        // their summary.
        List<DateRange.Period> periods = Configuration.get().getSupportedTypes();
        Period period = DateRange.Period.leastGranular(periods);
        while (period != null && period != Period.REALTIME)
        {
            // Read the old data.
            log.trace("Reading existing records for " + period + ".");
            DateRange range = DateRange.create(period, new Date(timestamp));
            Iterator<Metrics> iter = find(entityId, period, range.start, range.end);

            log.trace("Summarizing existing records.");
            Metrics existing = Metrics.summarize(iter);

            // Update/merge with the existing that we'd like to insert.
            if (log.isTraceEnabled())
            {
                log.trace("Merging existing records with new values.");
                log.trace("    => " + new ObjectMapper().writeValueAsString(existing) + " / " + new ObjectMapper().writeValueAsString(metrics));
            }
            existing.merge(metrics);

            // Insert the newly updated summary.
            if (log.isTraceEnabled())
            {
                log.trace("Inserting a newly updated summary for entity '" + entityId + "', period '" + period + "'");
                log.trace("    => " + new ObjectMapper().writeValueAsString(existing));
            }

            Metrics replacement = existing;

            // Add the mutation to the list of batch stuffs
            List<SuperColumn> superColumns = new ArrayList<SuperColumn>(1);
            superColumns.add(getSuperColumnMutation(range.start.getTime(), replacement.getMetrics()));
            superColumnOperations.put(period.toString(), superColumns);

            period = period.moreGranular();
            while (period != null && !periods.contains(period))
            {
                period = period.moreGranular();
            }
        }

        return superColumnOperations;
    }

    @Override
    public void persist(String entityId, long timestamp, Metrics metrics) throws IOException
    {
        if (entityId.startsWith("__") && entityId.endsWith("__"))
        {
            throw new IllegalArgumentException("Unable to persist entities " +
                    "that start and end with two underscores '__'. These " +
                    "entities are reserved for meta data.");
        }

        Lock lock = LockFactory.get();

        // Because we're updating in a batch, the best lock we can do is to
        // lock the whole row. This kind of sucks, but hopefully the same
        // entity isn't concurrently written to extremely often.
        int attempts = 0;
        while (attempts < MAX_RETRIES)
        {
            try
            {
                if (lock.acquire(entityId))
                {
                    try
                    {
                        Map<String, List<SuperColumn>> superColumnOperations = update(entityId, timestamp, metrics);
                        CassandraQueryFactory.get().persist(entityId, superColumnOperations);

                        break;
                    }
                    finally
                    {
                        try
                        {
                            lock.release(entityId);
                        }
                        catch (IOException e)
                        {
                            log.error("Unable to release the lock that I " +
                                    "acquired. Summary took too long " +
                                    "processing, or memcached has " +
                                    "disappeared. Marking the message as " +
                                    "processed and not redelivering it.");
                        }
                    }
                }
                else
                {
                    // This row is already locked and being written to. Wait it out.
                    attempts += 1;
                    log.debug("'" + entityId + "' is already locked, waiting for it to free up (this was attempt " + attempts + " of " + MAX_RETRIES + ").");

                    // Do a linear backoff and sleep until we try to acquire
                    // the lock again.
                    Thread.sleep(lock.delay() * attempts);
                }
            }
            catch (InterruptedException e)
            {
                log.warn("Thread interrupted, unable to sleep. Ignoring and trying again anyway", e);
            }
        }

        if (attempts == MAX_RETRIES)
        {
            // We failed to acquire a lock to write the row and we've
            // exceeded the allowable number of retries. We're really borked
            // in a serious way, so throw an exception.
            throw new IOException("Unable to acquire a lock on the '" + entityId + "' row after " + MAX_RETRIES + " attempts. Aborting this write.");
        }
    }
}
