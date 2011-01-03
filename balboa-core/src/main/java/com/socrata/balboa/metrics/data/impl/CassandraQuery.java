package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.data.DateRange;
import org.apache.cassandra.thrift.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface CassandraQuery
{
    List<KeySlice> getKeys(String columnFamily, KeyRange range) throws IOException;
    List<Column> getMeta(String entityId) throws IOException;
    List<SuperColumn> find(String entityId, SlicePredicate predicate, DateRange.Period period) throws IOException;
    void persist(String entityId, Map<String, List<ColumnOrSuperColumn>> superColumnOperations) throws IOException;
}
