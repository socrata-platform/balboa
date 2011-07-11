package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.Metric;
import com.socrata.balboa.metrics.Metrics;
import com.socrata.balboa.metrics.Timeslice;
import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.data.*;
import com.socrata.balboa.metrics.data.DateRange.Period;
import com.socrata.balboa.metrics.measurements.serialization.Serializer;
import com.socrata.balboa.metrics.measurements.serialization.SerializerFactory;
import com.yammer.metrics.core.MeterMetric;
import com.yammer.metrics.core.TimerMetric;
import org.apache.cassandra.thrift.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * A DataStore implementation using <a href="http://cassandra.apache.org/">
 * cassandra</a> as the storage backend.
 *
 * The cassandra data model defines a keyspace in configuration
 * (cassandra.keyspace). Underneath the keyspace, this datastore expects there
 * to be some super column ColumnFamilies. There should be a ColumnFamily for
 * each configured summary date range period (<code>DateRange.Period</code>) and
 * one ColumnFamily for entity meta data (<code>EntityMeta</code>).
 *
 * e.g.
 *
 * <code>
 *     &lt;ColumnFamily Name="hourly" ColumnType="Super" CompareWith="LongType" /&gt;
 *     &lt;ColumnFamily Name="daily" ColumnType="Super" CompareWith="LongType" /&gt;
 *     &lt;ColumnFamily Name="meta" CompareWith="BytesType" /&gt;
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
 *     (hourly, daily, or monthly ColumnFamily)
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
 * going to persist to then walks up each metric summarization tier combinging
 * existing values with the values being persisted. Because of this, summary
 * tiers are always as up to date as the events that have been written are and
 * reads are extremely cheap (typically, even over a long range query with the
 * default summary range period configuration, balboa won't need to read more
 * than seven items). Writes on the other hand are relatively expensive, since
 * each write requires 'n' updates (where 'n' is the number of summary range
 * types that are enabled).
 *
 * Writes have to lock the entire row because of the way column writes are
 * batched so some write nodes will block on writing until they can acquire a
 * lock and lock contention could be a problem on very active entity's.
 *
 * By default all values are written as "aggregate" records, meaning: To get the
 * metric over an arbitrary time range, all known records from adjacent time
 * slices can be summed together. Records can also be written as "absolute"
 * meaning: values aren't combined, only the most recent value is used.
 */
public class CassandraDataStore extends DataStoreImpl implements DataStore
{
    /** The maximum number of retries to acquire a lock before giving up */
    private static final int MAX_RETRIES = 5;

    private static Log log = LogFactory.getLog(CassandraDataStore.class);

    public static final TimerMetric persistMeter = com.yammer.metrics.Metrics.newTimer(DataStore.class, "total persist (read, update & write) lifecycle", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    public static final TimerMetric lockAcquisitionMeter = com.yammer.metrics.Metrics.newTimer(DataStore.class, "total time acquiring locks (including retries)", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    public static final MeterMetric lockFailureMeter = com.yammer.metrics.Metrics.newMeter(DataStore.class, "failure rate of lock acquisition", "failures", TimeUnit.SECONDS);

    public static class CassandraEntityMeta extends HashMap<String, String> implements EntityMeta
    {
        public CassandraEntityMeta()
        {
        }

        public CassandraEntityMeta(List<Column> columns)
        {
            super(columns.size());

            for (Column column : columns)
            {
                put(
                        new String(column.getName(), Charset.forName("UTF-8")),
                        new String(column.getValue(), Charset.forName("UTF-8"))
                );
            }
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

    static EntityMeta getEntityMeta(String entityId) throws IOException
    {
        List<Column> data = CassandraQueryFactory.get().getMeta(entityId);

        if (data != null)
        {
            return new CassandraEntityMeta(data);
        }
        else
        {
            return new CassandraEntityMeta();
        }
    }

    static class CassandraRowsIterator implements Iterator<String>
    {
        /** The maxiumum number of super columns to read in one query. */
        static final int QUERYBUFFER = 500;
        String columnFamily;
        String currentKey = "";
        List<KeySlice> buffer;

        CassandraRowsIterator(Period columnFamily)
        {
            this.columnFamily = columnFamily.toString();
            buffer = new ArrayList<KeySlice>(0);
        }

        /**
         * Fill the buffer after it's empty with new items from a cassandra
         * query.
         */
        public List<KeySlice> nextBuffer()
        {
            // Make sure that we're not executing a query when we've still got
            // some unprocessed buffer left.
            if (buffer != null)
            {
                assert(buffer.size() == 0);
            }

            if (currentKey == null)
            {
                return null;
            }

            try
            {
                KeyRange range = new KeyRange(QUERYBUFFER);
                range.setStart_key(currentKey);
                range.setEnd_key("");

                List<KeySlice> results = CassandraQueryFactory.get().getKeys(columnFamily, range);

                // Update the range so that the next time we fill the buffer, we
                // do it starting from the last of the returned results.
                if (results != null && results.size() > 1)
                {
                    KeySlice last = results.remove(results.size() - 1);
                    currentKey = last.getKey();

                    return results;
                }
                else if (results != null && results.size() == 1)
                {
                    currentKey = null;

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
        public String next()
        {
            if (!hasNext())
            {
                throw new NoSuchElementException("There are no more summaries.");
            }
            else
            {
                return buffer.remove(0).getKey();
            }
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException("Not supported.");
        }
    }

    static abstract class CassandraIteratorBase<T> implements Iterator<T>
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
        CassandraIteratorBase(String entityId, DateRange.Period period, DateRange range, EntityMeta meta) throws IOException
        {
            this.period = period;
            this.range = range;
            this.entityId = entityId;
            this.meta = meta;

            buffer = new ArrayList<SuperColumn>(0);
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
            if (buffer == null)
            {
                return false;
            }

            if (buffer.size() == 0)
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
        public abstract T next();

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException("Not supported.");
        }
    }


    /**
     * An iterator that pages cassandra rows in memory and loops over all of them
     * until there are no more remaining.
     */
    static class CassandraIterator extends CassandraIteratorBase<Metrics>
    {
        CassandraIterator(String entityId, Period period, DateRange range, EntityMeta meta) throws IOException
        {
            super(entityId, period, range, meta);
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
    }

    static class CassandraSliceIterator extends CassandraIteratorBase<Timeslice>
    {
        CassandraSliceIterator(String entityId, Period period, DateRange range, EntityMeta meta) throws IOException
        {
            super(entityId, period, range, meta);
        }

        @Override
        public Timeslice next()
        {
            if (!hasNext())
            {
                throw new NoSuchElementException("There are no more summaries.");
            }
            else
            {
                SuperColumn column = buffer.remove(0);
                Serializer ser = SerializerFactory.get();

                Timeslice slice = new Timeslice();

                // When we query cassandra we get back a set of columns (the
                // children of the super column -- period -- on which we queried).
                // We need to take all of these columns and map them into a hash
                // which the summary uses as its values.
                Metrics metrics = new Metrics(column.getColumnsSize());

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

                try
                {
                    long timestamp = CassandraUtils.unpackLong(column.getName());
                    DateRange range = DateRange.create(period, new Date(timestamp));
                    slice.setStart(range.start.getTime());
                    slice.setEnd(range.end.getTime());
                }
                catch (IOException e)
                {
                    throw new CassandraQueryException("Invalid column name, doesn't appear to be a long.", e);
                }

                slice.setMetrics(metrics);

                return slice;
            }
        }
    }

    Iterator<Metrics> query(String entityId, DateRange.Period period, DateRange range, EntityMeta meta) throws IOException
    {
        return new CassandraIterator(entityId, period, range, meta);
    }

    @Override
    public Iterator<Timeslice> slices(String entityId, Period period, Date start, Date end) throws IOException
    {
        if (Configuration.get().getSupportedPeriods().contains(period))
        {
            // Do the fast/low memory way if we support this tier
            return new CassandraSliceIterator(entityId, period, new DateRange(start, end), getEntityMeta(entityId));
        }
        else
        {
            Iterator<Timeslice> iter = new CassandraSliceIterator(entityId, getClosestTypeOrError(period), new DateRange(start, end), getEntityMeta(entityId));
            List<Timeslice> slices = new ArrayList<Timeslice>();
            Timeslice currentSlice = new Timeslice();
            currentSlice.setMetrics(new Metrics());
            currentSlice.setStart(DateRange.create(period, start).start.getTime());
            currentSlice.setEnd(DateRange.create(period, start).end.getTime());

            while (iter.hasNext())
            {
                Timeslice slice = iter.next();
                if (currentSlice.getEnd() < slice.getStart())
                {
                    slices.add(currentSlice);
                    currentSlice = new Timeslice();
                    currentSlice.setStart(DateRange.create(period, new Date(slice.getStart())).start.getTime());
                    currentSlice.setEnd(DateRange.create(period, new Date(slice.getStart())).end.getTime());
                    currentSlice.setMetrics(new Metrics());
                }
                else
                {
                    currentSlice.getMetrics().merge(slice.getMetrics());
                }
            }

            return slices.iterator();
        }
    }

    @Override
    public Iterator<String> entities(final String pattern) throws IOException
    {
        final CassandraRowsIterator rows = new CassandraRowsIterator(Period.leastGranular(Configuration.get().getSupportedPeriods()));
        return new Iterator<String>() {
            String current = null;
            @Override
            public boolean hasNext()
            {
                if (current != null)
                {
                    return true;
                }

                while (rows.hasNext())
                {
                    String candidate = rows.next();
                    if (candidate.matches(pattern))
                    {
                        current = candidate;
                        return true;
                    }
                }

                return false;
            }

            @Override
            public String next()
            {
                String n = current;
                current = null;
                return n;
            }

            @Override
            public void remove()
            {
                rows.remove();
            }
        };
    }

    @Override
    public Iterator<String> entities() throws IOException
    {
        return new CassandraRowsIterator(Period.leastGranular(Configuration.get().getSupportedPeriods()));
    }

    @Override
    public EntityMeta meta(String entityId) throws IOException
    {
        return getEntityMeta(entityId);
    }

    @Override
    public Iterator<Metrics> find(String entityId, DateRange.Period period, Date start, Date end) throws IOException
    {
        DateRange range = new DateRange(start, end);
        
        return query(entityId, getClosestTypeOrError(period), range, getEntityMeta(entityId));
    }
    
    @Override
    public Iterator<Metrics> find(String entityId, Period period, Date date) throws IOException
    {
        DateRange range = DateRange.create(period, date);

        return query(entityId, getClosestTypeOrError(period), range, getEntityMeta(entityId));
    }

    @Override
    public Iterator<Metrics> find(String entityId, Date start, Date end) throws IOException
    {
        DateRange range = new DateRange(start, end);
        QueryOptimizer optimizer = new QueryOptimizer();
        Map<Period,  Set<DateRange>> slices = optimizer.optimalSlices(range.start, range.end);

        int numberOfQueries = 0;

        CompoundIterator<Metrics> iter = new CompoundIterator();
        EntityMeta meta = getEntityMeta(entityId);

        for (Map.Entry<DateRange.Period,  Set<DateRange>> slice : slices.entrySet())
        {
            numberOfQueries += slice.getValue().size();
            
            for (DateRange r : slice.getValue())
            {
                iter.add(query(entityId, slice.getKey(), r, meta));
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

    List<ColumnOrSuperColumn> getMetaMutation(String entityId, Metrics metrics, EntityMeta meta) throws IOException
    {
        List<ColumnOrSuperColumn> mutations = new ArrayList<ColumnOrSuperColumn>();

        for (Map.Entry<String, Metric> entry : metrics.entrySet())
        {
            if (meta.containsKey(entry.getKey()) && !meta.get(entry.getKey()).equals(entry.getValue().getType().toString()))
            {
                throw new IllegalArgumentException("Invalid metrics " + "persistence method: " +
                                                           entry.getKey() + " already exists in " +
                                                           entityId + "'s meta, but has a " +
                                                           "different record type than requests " +
                                                           entry.getValue().getType() + " <-> " +
                                                           meta.get(entry.getKey()));

            }
            else if (!meta.containsKey(entry.getKey()))
            {
                Column column = new Column(
                        entry.getKey().getBytes(),
                        entry.getValue().getType().toString().getBytes(),
                        System.currentTimeMillis() * 1000
                );
                ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
                columnOrSuperColumn.setColumn(column);
                mutations.add(columnOrSuperColumn);
            }
        }

        return mutations;
    }

    public Map<String, List<ColumnOrSuperColumn>> update(String entityId, long timestamp, Metrics metrics, EntityMeta meta) throws IOException
    {
        Map<String, List<ColumnOrSuperColumn>> operations = new HashMap<String, List<ColumnOrSuperColumn>>();

        // For every period that can be summarized read/increment/update
        // their summary.
        List<DateRange.Period> periods = Configuration.get().getSupportedPeriods();
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
            List<ColumnOrSuperColumn> columnsOrSuperColumns = new ArrayList<ColumnOrSuperColumn>(1);
            ColumnOrSuperColumn op = new ColumnOrSuperColumn();
            op.setSuper_column(getSuperColumnMutation(range.start.getTime(), replacement));

            columnsOrSuperColumns.add(op);
            operations.put(period.toString(), columnsOrSuperColumns);

            period = period.moreGranular();
            while (period != null && !periods.contains(period))
            {
                period = period.moreGranular();
            }
        }

        List<ColumnOrSuperColumn> metaMutations = getMetaMutation(entityId, metrics, meta);
        operations.put("meta", metaMutations);

        return operations;
    }

    @Override
    public void persist(String entityId, long timestamp, Metrics metrics) throws IOException
    {
        long begin = System.currentTimeMillis();

        if (entityId.equals("com.blist.services.views.RowsService:index") || entityId.equals("ip-socrata-token-used"))
        {
            return;
        }

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
                long lockBegin = System.currentTimeMillis();
                if (lock.acquire(entityId))
                {
                    lockAcquisitionMeter.update(System.currentTimeMillis() - lockBegin, TimeUnit.MILLISECONDS);
                    try
                    {
                        EntityMeta meta = getEntityMeta(entityId);
                        Map<String, List<ColumnOrSuperColumn>> operations = update(entityId, timestamp, metrics, meta);
                        CassandraQueryFactory.get().persist(entityId, operations);

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
                    lockFailureMeter.mark();

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

        persistMeter.update(System.currentTimeMillis() - begin, TimeUnit.MILLISECONDS);
    }
}
