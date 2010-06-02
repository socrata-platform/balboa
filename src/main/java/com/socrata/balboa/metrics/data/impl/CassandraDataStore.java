package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.exceptions.InternalException;
import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.Summary.Type;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DateRange;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import me.prettyprint.cassandra.service.Keyspace;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CassandraDataStore implements DataStore
{
    public class QueryRobot implements Iterator<Summary>
    {
        static final int QUERYBUFFER = 100;
        
        String rowId;
        ColumnParent parent;
        DateRange range;

        List<SuperColumn> buffer;
        
        QueryRobot(String rowId, ColumnParent parent, DateRange range)
        {
            this.parent = parent;
            this.range = range;
            
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
                return keyspace.getSuperSlice(rowId, parent, predicate);
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
                SuperColumn last = results.get(results.size() - 1);
                range.start = new Date(CassandraUtils.unpackLong(last.getName()));

                return results;
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

                return new Summary(CassandraUtils.unpackLong(column.getName()), values);
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
    public Iterator<Summary> find(String entityType, String entityId, Type type, Date date)
    {
        DateRange range = DateRange.create(type, date);
        String rowId = entityType + "-" + entityId;
        ColumnParent parent = new ColumnParent(type.toString());
        
        return new QueryRobot(rowId, parent, range);
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
                path.setColumn(key.getBytes("UTF-8"));
                path.setSuper_column(CassandraUtils.packLong(summary.getTimestamp()));

                keyspace.insert(entityId, path, summary.getValues().get(key).getBytes("UTF-8"));
            }
        }
        catch (NotFoundException e)
        {
            throw new InternalException("Keyspace '" + keyspaceName + "' not found.");
        }
        catch (Exception e)
        {
            throw new InternalException("Unknown exception saving summary.");
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
