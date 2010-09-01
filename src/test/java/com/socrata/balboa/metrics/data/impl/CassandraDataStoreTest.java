package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.metrics.data.Lock;
import com.socrata.balboa.metrics.data.LockFactory;
import com.socrata.balboa.metrics.measurements.serialization.SerializerFactory;
import com.socrata.balboa.metrics.utils.MetricUtils;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SuperColumn;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class CassandraDataStoreTest 
{
    public DataStore get()
    {
        return new CassandraDataStore();
    }

    @Before
    public void setup()
    {
        CassandraQueryFactory.setTestMock(null);
    }

    @Test
    public void testCantPersistSummariesThatArentRealtime() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(DateRange.Type.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, DateRange.Type type) throws IOException
                    {
                        return new ArrayList<SuperColumn>(0);
                    }

                    @Override
                    public void persist(String entityId, Map<String, List<SuperColumn>> inserts) throws IOException {}
                }
        );

        Map<String, Object> data = new HashMap<String, Object>();
        data.put("narfnarfnarf", 1);

        Summary summary = new Summary(DateRange.Type.MONTHLY, range.start.getTime(), data);

        try
        {
            ds.persist("testCantPersistSummariesThatArentRealtime", summary);
            Assert.fail();
        }
        catch (IllegalArgumentException e) {}
    }

    @Test
    public void testCantPersistUnderscoredColumns() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(DateRange.Type.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, DateRange.Type type) throws IOException
                    {
                        return new ArrayList<SuperColumn>(0);
                    }

                    @Override
                    public void persist(String entityId, Map<String, List<SuperColumn>> inserts) throws IOException {}
                }
        );

        Map<String, Object> data = new HashMap<String, Object>();
        data.put("__chompa__", 1);

        Summary summary = new Summary(DateRange.Type.REALTIME, range.start.getTime(), data);

        try
        {
            ds.persist("testCantPersistUnderscoredColumns", summary);
            Assert.fail();
        }
        catch (IllegalArgumentException e){}
    }

    @Test
    public void testCantPersistUnderscoredEntities() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(DateRange.Type.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, DateRange.Type type) throws IOException
                    {
                        return new ArrayList<SuperColumn>(0);
                    }

                    @Override
                    public void persist(String entityId, Map<String, List<SuperColumn>> inserts) throws IOException {}
                }
        );

        Map<String, Object> data = new HashMap<String, Object>();
        data.put("test1", 1);

        Summary summary = new Summary(DateRange.Type.REALTIME, range.start.getTime(), data);

        try
        {
            ds.persist("__illegal.key.name__", summary);
            Assert.fail();
        }
        catch (IllegalArgumentException e){}
    }

    @Test
    public void testReadIOError() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(DateRange.Type.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, DateRange.Type type) throws IOException
                    {
                        throw new IOException("Oh Cody. This is so wrong.");
                    }

                    @Override
                    public void persist(String entityId, Map<String, List<SuperColumn>> superColumnOperations) throws IOException {}
                }
        );

        Iterator<Summary> iter = ds.find("testRemoveFails", range.start, range.end);

        try
        {
            iter.hasNext();
            Assert.fail();
        }
        catch (CassandraDataStore.CassandraQueryException e) {}
    }

    @Test
    public void testRemoveFails() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(DateRange.Type.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, DateRange.Type type) throws IOException
                    {
                        long start = CassandraUtils.unpackLong(predicate.getSlice_range().getStart());
                        if (start > range.start.getTime())
                        {
                            return new ArrayList<SuperColumn>(0);
                        }

                        List<SuperColumn> items = new ArrayList<SuperColumn>(2);
                        List<Column> columns = new ArrayList<Column>(1);

                        columns.add(new Column("test1".getBytes(), SerializerFactory.get().serialize(1), 0));
                        columns.add(new Column("test2".getBytes(), SerializerFactory.get().serialize(2), 0));
                        items.add(new SuperColumn(CassandraUtils.packLong(range.start.getTime()), columns));

                        return items;
                    }

                    @Override
                    public void persist(String entityId, Map<String, List<SuperColumn>> superColumnOperations) throws IOException {}
                }
        );

        Iterator<Summary> iter = ds.find("testRemoveFails", range.start, range.end);

        Assert.assertTrue(iter.hasNext());

        try
        {
            iter.remove();
            Assert.fail();
        }
        catch (UnsupportedOperationException e) {}

    }

    @Test
    public void testOnAnOddTimeBoundaryTheSuperColumnNameIsForcedToTheRightBoundary() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(DateRange.Type.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, DateRange.Type type) throws IOException
                    {
                        return new ArrayList<SuperColumn>(0);
                    }

                    @Override
                    public void persist(String entityId, Map<String, List<SuperColumn>> inserts) throws IOException
                    {
                        String t = DateRange.Type.HOURLY.toString();
                        
                        Assert.assertTrue(inserts.get(t).size() == 1);

                        SuperColumn insert = inserts.get(t).get(0);
                        long timestamp = CassandraUtils.unpackLong(insert.getName());

                        Assert.assertNotSame(range.start.getTime() + 31337, timestamp);

                        DateRange boundary = DateRange.create(DateRange.Type.HOURLY, new Date(timestamp));
                        Assert.assertEquals(boundary.start.getTime(), timestamp);
                    }
                }
        );

        Map<String, Object> data = new HashMap<String, Object>();
        data.put("test1", 1);
        data.put("test2", 2);

        Summary summary = new Summary(DateRange.Type.REALTIME, range.start.getTime() + 31337, data);
        ds.persist("testCreate", summary);
    }

    @Test
    public void testNoSummariesNextCallThrowsException() throws Exception
    {
        DataStore ds = get();
        DateRange range = DateRange.create(DateRange.Type.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, DateRange.Type type) throws IOException
                    {
                        return null;
                    }

                    @Override
                    public void persist(String entityId, Map<String, List<SuperColumn>> superColumnOperations) throws IOException {}
                }
        );

        Iterator<Summary> iter = ds.find("testRangeScan4", range.start, range.end);

        Assert.assertNotNull(iter);

        try
        {
            iter.next();
            Assert.fail();
        }
        catch (NoSuchElementException e) {}
    }

    @Test
    public void testInvalidlySerializedValueDoesNotCrash() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(DateRange.Type.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, DateRange.Type type) throws IOException
                    {
                        long start = CassandraUtils.unpackLong(predicate.getSlice_range().getStart());
                        if (start > range.start.getTime())
                        {
                            return new ArrayList<SuperColumn>(0);
                        }

                        List<SuperColumn> items = new ArrayList<SuperColumn>(1);
                        List<Column> columns = new ArrayList<Column>(1);

                        columns.add(new Column("test2".getBytes(), new byte[] {(byte)0xde, (byte)0xad, (byte)0xbe, (byte)0xef}, 0));
                        items.add(new SuperColumn(CassandraUtils.packLong(range.start.getTime()), columns));

                        return items;
                    }

                    @Override
                    public void persist(String entityId, Map<String, List<SuperColumn>> superColumnOperations) throws IOException {}
                }
        );

        Iterator<Summary> iter = ds.find("evil", range.start, range.end);

        Assert.assertTrue(iter.hasNext());

        Summary sum = iter.next();

        Assert.assertEquals(0, sum.getValues().size());
    }

    @Test
    public void testNullResultMeansEmptySummary() throws Exception
    {
        DataStore ds = get();
        DateRange range = DateRange.create(DateRange.Type.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, DateRange.Type type) throws IOException
                    {
                        return null;
                    }

                    @Override
                    public void persist(String entityId, Map<String, List<SuperColumn>> superColumnOperations) throws IOException {}
                }
        );

        Iterator<Summary> iter = ds.find("testRangeScan4", range.start, range.end);

        Assert.assertNotNull(iter);
        Assert.assertFalse(iter.hasNext());
    }

    @Test
    public void testQueryWhichFillsUpBuffer() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(DateRange.Type.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, DateRange.Type type) throws IOException
                    {
                        long start = CassandraUtils.unpackLong(predicate.getSlice_range().getStart());
                        
                        if(start >= range.start.getTime() + CassandraDataStore.CassandraIterator.QUERYBUFFER + 1)
                        {
                            return new ArrayList<SuperColumn>(0);
                        }
                        else if (start >= range.start.getTime() + CassandraDataStore.CassandraIterator.QUERYBUFFER)
                        {
                            List<SuperColumn> items = new ArrayList<SuperColumn>(1);
                            List<Column> columns = new ArrayList<Column>(1);

                            columns.add(new Column("test2".getBytes(), SerializerFactory.get().serialize(2), 0));
                            items.add(new SuperColumn(CassandraUtils.packLong(range.start.getTime() + CassandraDataStore.CassandraIterator.QUERYBUFFER + 1), columns));

                            return items;
                        }
                        else
                        {
                            List<SuperColumn> items = new ArrayList<SuperColumn>(CassandraDataStore.CassandraIterator.QUERYBUFFER);
                            for (int i=0; i < CassandraDataStore.CassandraIterator.QUERYBUFFER; i++)
                            {
                                List<Column> columns = new ArrayList<Column>(1);

                                columns.add(new Column("test1".getBytes(), SerializerFactory.get().serialize(1), 0));
                                items.add(new SuperColumn(CassandraUtils.packLong(range.start.getTime() + i), columns));
                            }

                            return items;
                        }
                    }

                    @Override
                    public void persist(String entityId, Map<String, List<SuperColumn>> superColumnOperations) throws IOException {}
                }
        );

        Iterator<Summary> iter = ds.find("testRangeScan1", range.start, range.end);

        Assert.assertTrue(iter.hasNext());

        Map<String, Object> results = MetricUtils.summarize(iter);

        Assert.assertEquals(CassandraDataStore.CassandraIterator.QUERYBUFFER, results.get("test1"));
        Assert.assertEquals(2, results.get("test2"));

        Assert.assertFalse(iter.hasNext());
    }

    @Test
    public void testQueryWithMoreThanOneResult() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(DateRange.Type.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, DateRange.Type type) throws IOException
                    {
                        long start = CassandraUtils.unpackLong(predicate.getSlice_range().getStart());
                        if (start > range.start.getTime())
                        {
                            return new ArrayList<SuperColumn>(0);
                        }

                        List<SuperColumn> items = new ArrayList<SuperColumn>(2);
                        List<Column> columns = new ArrayList<Column>(2);

                        columns.add(new Column("test1".getBytes(), SerializerFactory.get().serialize(1), 0));
                        columns.add(new Column("test2".getBytes(), SerializerFactory.get().serialize(2), 0));
                        items.add(new SuperColumn(CassandraUtils.packLong(range.start.getTime()), columns));

                        columns = new ArrayList<Column>(1);
                        columns.add(new Column("test3".getBytes(), SerializerFactory.get().serialize(3), 0));
                        items.add(new SuperColumn(CassandraUtils.packLong(range.start.getTime() + 1), columns));

                        return items;
                    }

                    @Override
                    public void persist(String entityId, Map<String, List<SuperColumn>> superColumnOperations) throws IOException {}
                }
        );

        Iterator<Summary> iter = ds.find("testRangeScan1", range.start, range.end);

        Assert.assertTrue(iter.hasNext());

        Map<String, Object> results = MetricUtils.summarize(iter);

        Assert.assertEquals(1, results.get("test1"));
        Assert.assertEquals(2, results.get("test2"));
        Assert.assertEquals(3, results.get("test3"));

        Assert.assertFalse(iter.hasNext());
    }

    @Test
    public void testRangeScanSimple() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(DateRange.Type.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, DateRange.Type type) throws IOException
                    {
                        long start = CassandraUtils.unpackLong(predicate.getSlice_range().getStart());
                        if (start > range.start.getTime())
                        {
                            return new ArrayList<SuperColumn>(0);
                        }

                        List<SuperColumn> items = new ArrayList<SuperColumn>(2);
                        List<Column> columns = new ArrayList<Column>(1);

                        columns.add(new Column("test1".getBytes(), SerializerFactory.get().serialize(1), 0));
                        columns.add(new Column("test2".getBytes(), SerializerFactory.get().serialize(2), 0));
                        items.add(new SuperColumn(CassandraUtils.packLong(range.start.getTime()), columns));

                        return items;
                    }

                    @Override
                    public void persist(String entityId, Map<String, List<SuperColumn>> superColumnOperations) throws IOException {}
                }
        );

        Iterator<Summary> iter = ds.find("testRangeScan1", range.start, range.end);

        Assert.assertTrue(iter.hasNext());

        Map<String, Object> results = MetricUtils.summarize(iter);

        Assert.assertEquals(1, results.get("test1"));
        Assert.assertEquals(2, results.get("test2"));

        Assert.assertFalse(iter.hasNext());
    }

    @Test
    public void testRangeScanNoItems() throws Exception
    {
        DataStore ds = get();
        DateRange range = DateRange.create(DateRange.Type.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, DateRange.Type type) throws IOException
                    {
                        return new ArrayList<SuperColumn>(0);
                    }

                    @Override
                    public void persist(String entityId, Map<String, List<SuperColumn>> superColumnOperations) throws IOException {}
                }
        );

        Iterator<Summary> iter = ds.find("testRangeScan4", range.start, range.end);

        Assert.assertFalse(iter.hasNext());
    }

    @Test
    public void testCreate() throws Exception
    {
        DataStore ds = get();

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, DateRange.Type type) throws IOException
                    {
                        return new ArrayList<SuperColumn>(0);
                    }

                    @Override
                    public void persist(String entityId, Map<String, List<SuperColumn>> inserts) throws IOException
                    {
                        Assert.assertEquals("testCreate", entityId);
                        Assert.assertTrue(Configuration.get().getSupportedTypes().size() > 0);

                        for (DateRange.Type t : Configuration.get().getSupportedTypes())
                        {
                            Assert.assertTrue(inserts.containsKey(t.toString()));
                        }

                        String t = Configuration.get().getSupportedTypes().get(0).toString();
                        
                        Assert.assertTrue(inserts.get(t).size() == 1);

                        List<Column> cols = inserts.get(t).get(0).getColumns();
                        Assert.assertTrue(cols.size() == 2);
                        Assert.assertTrue("test1".equals(new String(cols.get(0).getName())));
                        Assert.assertTrue("test2".equals(new String(cols.get(1).getName())));
                    }
                }
        );

        Map<String, Object> data = new HashMap<String, Object>();
        data.put("test1", 1);
        data.put("test2", 2);

        DateRange range = DateRange.create(DateRange.Type.MONTHLY, new Date(0));
        Summary summary = new Summary(DateRange.Type.REALTIME, range.start.getTime(), data);
        ds.persist("testCreate", summary);
    }

    @Test
    public void testCreateSummaryActuallyUpdatesTheSummaryIfItAlreadyExists() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(DateRange.Type.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, DateRange.Type type) throws IOException
                    {
                        long start = CassandraUtils.unpackLong(predicate.getSlice_range().getStart());
                        if (start > range.start.getTime())
                        {
                            return new ArrayList<SuperColumn>(0);
                        }

                        List<SuperColumn> items = new ArrayList<SuperColumn>(2);
                        List<Column> columns = new ArrayList<Column>(1);

                        columns.add(new Column("test1".getBytes(), SerializerFactory.get().serialize(1), 0));
                        columns.add(new Column("test2".getBytes(), SerializerFactory.get().serialize(2), 0));
                        items.add(new SuperColumn(CassandraUtils.packLong(start), columns));

                        return items;
                    }

                    @Override
                    public void persist(String entityId, Map<String, List<SuperColumn>> inserts) throws IOException
                    {
                        Assert.assertEquals("testCreateSummaryActuallyUpdatesTheSummaryIfItAlreadyExists", entityId);
                        Assert.assertTrue(Configuration.get().getSupportedTypes().size() > 0);

                        for (DateRange.Type t : Configuration.get().getSupportedTypes())
                        {
                            Assert.assertTrue(inserts.containsKey(t.toString()));
                        }

                        String t = Configuration.get().getSupportedTypes().get(0).toString();

                        Assert.assertTrue(inserts.get(t).size() == 1);

                        List<Column> cols = inserts.get(t).get(0).getColumns();
                        Assert.assertTrue(cols.size() == 3);
                        Assert.assertTrue("test1".equals(new String(cols.get(0).getName())));
                        Assert.assertTrue("test2".equals(new String(cols.get(1).getName())));
                        Assert.assertTrue("test3".equals(new String(cols.get(2).getName())));
                        
                        Assert.assertEquals(2, SerializerFactory.get().deserialize(cols.get(0).getValue()));
                        Assert.assertEquals(4, SerializerFactory.get().deserialize(cols.get(1).getValue()));
                        Assert.assertEquals(3, SerializerFactory.get().deserialize(cols.get(2).getValue()));
                    }
                }
        );

        Map<String, Object> data = new HashMap<String, Object>();
        data.put("test1", 1);
        data.put("test2", 2);
        data.put("test3", 3);

        Summary summary = new Summary(DateRange.Type.REALTIME, range.start.getTime(), data);
        ds.persist("testCreateSummaryActuallyUpdatesTheSummaryIfItAlreadyExists", summary);
    }

    @Test(expected=java.io.IOException.class)
    public void testLockingAKeyWontWriteIt() throws Exception
    {
        DataStore ds = get();

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, DateRange.Type type) throws IOException
                    {
                        return new ArrayList<SuperColumn>(0);
                    }

                    @Override
                    public void persist(String entityId, Map<String, List<SuperColumn>> superColumnOperations) throws IOException
                    {
                        Assert.fail("Shouldn't persist an item if I'm unable to get a lock.");
                    }
                }
        );

        Lock lock = LockFactory.get();
        lock.acquire("123");

        try
        {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("test1", 1);

            Summary summary = new Summary(DateRange.Type.REALTIME, new Date(0).getTime(), data);
            ds.persist("123", summary);
        }
        finally
        {
            lock.release("123");
        }
    }
}
