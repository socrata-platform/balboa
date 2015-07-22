package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.Metrics;
import com.socrata.balboa.metrics.Timeslice;
import com.socrata.balboa.metrics.WatchDog;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

public interface DataStore extends WatchDog.WatchDogListener
{
    /**
     * Retrieve an iterator that contains all the entity ids that the pattern
     * string matches. You probably don't want to use this, because it's
     * terribly slow and possibly dangerous.
     */
    @Deprecated
    public Iterator<String> entities(String pattern) throws IOException;

    /**
     * Retrieve an iterator that encompasses all entity ids for which there are
     * metrics being tracked. Generally speaking, this query is likely to be
     * very expensive and you probably don't want to use it unless you know
     * what you're doing.
     */
    @Deprecated
    public Iterator<String> entities() throws IOException;

    /**
     * Return a list of metrics for a period of timeslices over an arbitrary
     * date range, chronologically ascending.
     *
     * e.g. Give me all of the metrics for some entity broken down by hours in
     * the range 2010-01-01 -> 2010-01-31.
     */
    public Iterator<Timeslice> slices(String entityId, Period period, Date start, Date end) throws IOException;

    /**
     * Given a date and given a summary range period, create the appropriate range
     * for the date (explained below) and perform a query that returns all the
     * summaries for that time period.
     *
     * The range is created by taking the period and finding the boundaries for
     * that period that the date belongs to. For example is the period is "month"
     * and the date is 2010-01-04, the range that will be queried is
     * 2010-01-01 -> 2010-01-31. For more details
     *
     * @see DateRange
     */
    public Iterator<Metrics> find(String entityId, Period period, Date date) throws IOException;

    /**
     * Find all the summaries of a particular tier between start and end, ordered
     * by date. This is not necessarily the most optimal way to query for an
     * arbitrary range and should only be used when you need to query a specific
     * tier for some reason.
     */
    public Iterator<Metrics> find(String entityId, Period period, Date start, Date end) throws IOException;

    /**
     * Find the total summaries between two particular dates, ordered by date.
     * The query optimizer should plan the query so that start and end align
     * along a date date boundary of your most granular type.
     *
     * @see com.socrata.balboa.metrics.data.Period
     */
    public Iterator<Metrics> find(String entityId, Date start, Date end) throws IOException;

    /**
     * Save a set of metrics. The datastore is responsible for making sure the
     * persist applies correctly to all supported tiers.
     */
    public void persist(String entityId, long timestamp, Metrics metrics) throws IOException;
}
