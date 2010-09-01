package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.Summary;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

public interface DataStore
{
    /**
     * Given a date and given a summary range type, create the appropriate range
     * for the date (explained below) and perform a query that returns all the
     * summaries for that time period.
     *
     * The range is created by taking the type and finding the boundaries for
     * that type that the date belongs to. For example is the type is "month"
     * and the date is 2010-01-04, the range that will be queried is
     * 2010-01-01 -> 2010-01-31. For more details
     *
     * @see DateRange
     */
    public Iterator<Summary> find(String entityId, DateRange.Type type, Date date) throws IOException;

    /**
     * Find all the summaries of a particular tier between start and end. This
     * is not necessarily the most optimal way to query for an arbitrary range
     * and should only be used when you need to query a specific tier for some
     * reason.
     */
    public Iterator<Summary> find(String entityId, DateRange.Type type, Date start, Date end) throws IOException;

    /**
     * Find the total summaries between two particular dates. The query
     * optimizer should plan the query so that start and end align along a date
     * date boundary of your most granular type.
     *
     * @see com.socrata.balboa.metrics.data.DateRange.Type 
     */
    public Iterator<Summary> find(String entityId, Date start, Date end) throws IOException;

    /**
     * Save a summary. The datastore is responsible for making sure the persist
     * applies correctly to all supported tiers.
     */
    public void persist(String entityId, Summary summary) throws IOException;
}
