package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.Summary.Type;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.metrics.measurements.serialization.ProtocolBuffersSerializer;
import com.socrata.balboa.metrics.measurements.serialization.Serializer;
import com.socrata.balboa.metrics.utils.MetricUtils;
import com.socrata.balboa.server.exceptions.InternalException;
import me.prettyprint.cassandra.service.*;
import org.apache.cassandra.thrift.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.*;

public class CassandraDataStore implements DataStore
{
    /**
     * An iterator that continues loading cassandra rows over a range until
     * there are no more left.
     */
    public class QueryRobot implements Iterator<Summary>
    {
        static final int QUERYBUFFER = 5000;
        
        String rowId;
        Type type;
        DateRange range;

        List<SuperColumn> buffer;

        /**
         *
         * @param rowId The row id to query.
         * @param type The type to look for. This determines which super column
         * should be searched for rows.
         * @param range The date range to constrain the search to.
         */
        QueryRobot(String rowId, Type type, DateRange range)
        {
            this.type = type;
            this.range = range;
            this.rowId = rowId;
            
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
         * Wrapper around the cassandra client. Get a client, and make sure it
         * gets returned to the pool.
         */
        public List<SuperColumn> executeQuery(SlicePredicate predicate) throws Exception
        {
            CassandraClient client = pool.borrowClient(hosts);

            try
            {
                Keyspace keyspace = client.getKeyspace(keyspaceName);
                return keyspace.getSuperSlice(rowId, new ColumnParent(type.toString()), predicate);
            }
            finally
            {
                pool.releaseClient(client);
            }
        }

        /**
         * Fill the buffer after it's empty with new items from a cassandra
         * query.
         */
        public List<SuperColumn> nextBuffer()
        {
            // Make sure that we're not executing a query when we've still got
            // some unprocessed buffer left.
            assert(buffer.size() == 0);

            // Don't perform the query if we're already past the end of the
            // buffer.
            if (range.start.getTime() > range.end.getTime())
            {
                return null;
            }

            try
            {
                // Create the predicate and execute the query.
                SlicePredicate predicate = createPredicate(range);

                double startTime = System.nanoTime();
                List<SuperColumn> results = executeQuery(predicate);
                log.debug("Queried cassandra " + (System.nanoTime() - startTime) / Math.pow(10,6) + " (ms)");

                // Update the range so that the next time we fill the buffer, we
                // do it starting from the last of the returned results.
                if (results.size() > 0)
                {
                    SuperColumn last = results.get(results.size() - 1);

                    // Add one to the last result's timestamp. This should never
                    // cause any summary to be skipped over since we have a quantum
                    // of time (1 nano/mill/whatever second) that's our highest
                    // resolution. In practice, for anything other than the realtime
                    // type, there should only be one summary per period.
                    range.start = new Date(CassandraUtils.unpackLong(last.getName()) + 1);

                    return results;
                }
                else
                {
                    return null;
                }
            }
            catch (Exception e)
            {
                if (e instanceof NotFoundException)
                {
                    log.error("There was some serious problem reading from cassandra.", e);
                }

                // Well, no matter what happened that caused an exception, we
                // don't have any items to iterate over.
                return null;
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
        public Summary next()
        {
            if (!hasNext())
            {
                throw new NoSuchElementException("There are no more summaries.");
            }
            else
            {
                SuperColumn column = buffer.remove(0);
                Serializer ser = new ProtocolBuffersSerializer();

                // When we query cassandra we get back a set of columns (the
                // children of the super column -- type -- on which we queried).
                // We need to take all of these columns and map them into a hash
                // which the summary uses as its values.
                Map<String, Object> values = new HashMap<String, Object>(column.getColumnsSize());

                for (Column subColumn : column.getColumns())
                {
                    String name = new String(subColumn.getName());

                    // All of the column values are serialized and have to be
                    // deserialized into their numeric values.
                    String serializedValue = new String(subColumn.getValue());

                    try
                    {
                        values.put(name, ser.deserialize(serializedValue.getBytes()));
                    }
                    catch (IOException e)
                    {
                        log.error("(" + rowId + " -> " + range + ") Unable " +
                                "to deserialize the value '" + serializedValue +
                                "' in the column '" + name + "'. Ignoring " +
                                "this metric.");
                    }
                }

                return new Summary(type, CassandraUtils.unpackLong(column.getName()), values);
            }
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException("Not supported.");
        }
    }

    private static Log log = LogFactory.getLog(CassandraDataStore.class);

    CassandraClientPool pool;
    String[] hosts;
    String keyspaceName;
    
    public CassandraDataStore(String[] hosts, String keyspaceName)
    {
        pool = CassandraClientPoolFactory.INSTANCE.get();
        this.hosts = hosts;
        this.keyspaceName = keyspaceName;
    }

    @Override
    public Iterator<Summary> find(String entityId, Type type, Date start, Date end)
    {
        DateRange range = new DateRange(start, end);

        return new QueryRobot(entityId, type, range);
    }
    
    @Override
    public Iterator<Summary> find(String entityId, Type type, Date date)
    {
        DateRange range = DateRange.create(type, date);
        
        return new QueryRobot(entityId, type, range);
    }

    void lock(String key)
    {
        // We don't actually implement locking right now, so this doesn't have
        // to do anything. Before we start running a cluster of metrics servers,
        // we'll have to implement some kind of distributed lock (ZooKeeper)
    }

    void unlock(String key)
    {
        // We don't actually implement locking right now, so this doesn't have
        // to do anything. Before we start running a cluster of metrics servers,
        // we'll have to implement some kind of distributed lock (ZooKeeper)
    }

    SuperColumn getSuperColumnMutation(Summary summary) throws IOException
    {
        List<Column> columns = new ArrayList<Column>(summary.getValues().size());
        Serializer ser = new ProtocolBuffersSerializer();
        for (String key : summary.getValues().keySet())
        {
            Column column = new Column(
                    key.getBytes(),
                    ser.serialize(summary.getValues().get(key)),
                    TimestampResolution.MICROSECONDS.createTimestamp()
            );

            columns.add(column);
        }

        SuperColumn superColumn = new SuperColumn(CassandraUtils.packLong(summary.getTimestamp()), columns);

        return superColumn;
    }

    @Override
    public void persist(String entityId, Summary summary)
    {
        if (summary.getType() != Type.REALTIME)
        {
            // TODO: Necessary? Could I just summarize above the current? Doing
            // that could result in potentially inconsistent data, though...
            throw new InternalException("Unable to persist anything but realtime events with this data store.");
        }

        CassandraClient client;
        try
        {
            client = pool.borrowClient(hosts);
        }
        catch (Exception e)
        {
            throw new InternalException("Unknown exception trying to borrow a cassandra client.", e);
        }

        Keyspace keyspace = null;
        try
        {
            keyspace = client.getKeyspace(keyspaceName);

            Map<String, List<SuperColumn>> superColumnOperations = new HashMap<String, List<SuperColumn>>();

            // For every type that can be summarized read/increment/update
            // their summary.
            Type type = Type.YEARLY;
            while (type != Type.REALTIME)
            {
                // Read the old data.
                DateRange range = DateRange.create(type, new Date(summary.getTimestamp()));
                Iterator<Summary> iter = find(entityId, type, range.start, range.end);
                Map<String, Object> values = MetricUtils.summarize(iter);

                // Update/merge with the values that we'd like to insert.
                MetricUtils.merge(values, summary.getValues());

                // Insert the newly updated summary.
                log.debug("Inserting a newly updated summary for entity '" + entityId + "', type '" + type + "'");
                Summary replacement = new Summary(type, range.start.getTime(), values);

                // Add the mutation to the list of batch stuffs
                List<SuperColumn> superColumns = new ArrayList<SuperColumn>(1);
                superColumns.add(getSuperColumnMutation(replacement));
                superColumnOperations.put(replacement.getType().toString(), superColumns);

                type = type.moreGranular();
            }

            keyspace.batchInsert(entityId, null, superColumnOperations);
        }
        catch (NotFoundException e)
        {
            throw new InternalException("Keyspace '" + keyspaceName + "' not found.");
        }
        catch (Exception e)
        {
            throw new InternalException("Unknown exception saving summary.", e);
        }
        finally
        {
            try
            {
                pool.releaseClient(keyspace == null ? client : keyspace.getClient());
            }
            catch (Exception e)
            {
                log.warn("Unable to release the cassandra client for some reason (not good).", e);
            }
        }
    }
}
