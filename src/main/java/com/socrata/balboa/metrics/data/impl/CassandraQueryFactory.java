package com.socrata.balboa.metrics.data.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

public class CassandraQueryFactory
{
    private static Log log = LogFactory.getLog(CassandraQueryFactory.class);

    static CassandraQuery testMock;

    static void setTestMock(CassandraQuery testMock)
    {
        CassandraQueryFactory.testMock = testMock;
    }

    public static CassandraQuery get() throws IOException
    {
        String environment = System.getProperty("socrata.env");

        if ("test".equals(environment))
        {
            return testMock;
        }
        else
        {
            return new CassandraQueryImpl();
        }
    }
}
