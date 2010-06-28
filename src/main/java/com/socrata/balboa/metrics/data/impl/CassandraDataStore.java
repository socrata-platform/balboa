package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.server.exceptions.InternalException;
import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.Summary.Type;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DateRange;
import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import me.prettyprint.cassandra.service.Keyspace;
import org.apache.cassandra.thrift.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.*;

public class CassandraDataStore implements DataStore
{
    public class QueryRobot implements Iterator<Summary>
    {
        static final int QUERYBUFFER = 5000;
        
        String rowId;
        Type type;
        DateRange range;

        List<SuperColumn> buffer;
        
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
                List<SuperColumn> results = executeQuery(predicate);

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
                
                Map<String, String> values = new HashMap<String, String>(column.getColumnsSize());
                for (Column subColumn : column.getColumns())
                {
                    values.put(new String(subColumn.getName()), new String(subColumn.getValue()));
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

    @Override
    public void persist(String entityId, Summary summary)
    {
        CassandraClient client;

        try
        {
            client = pool.borrowClient(hosts);
        }
        catch (Exception e)
        {
            throw new InternalException("Unknown exception trying to borrow a cassandra client.", e);
        }        

        try
        {
            Keyspace keyspace = client.getKeyspace(keyspaceName);

            for (String key : summary.getValues().keySet())
            {
                ColumnPath path = new ColumnPath(summary.getType().toString());
                path.setColumn(key.getBytes());
                path.setSuper_column(CassandraUtils.packLong(summary.getTimestamp()));

                keyspace.insert(entityId, path, summary.getValues().get(key).getBytes());
            }
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
                pool.releaseClient(client);
            }
            catch (Exception e)
            {
                log.warn("Unable to release the cassandra client for some reason (not good).", e);
            }
        }
    }


}
