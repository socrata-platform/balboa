package com.socrata.balboa.metrics.data.impl;

import java.io.IOException;

public class SqlQueryFactory
{
    static SqlQuery testMock;

    static void setTestMock(SqlQuery testMock)
    {
        SqlQueryFactory.testMock = testMock;
    }

    public static SqlQuery get() throws IOException
    {
        String environment = System.getProperty("socrata.env");

        if ("test".equals(environment))
        {
            return testMock;
        }
        else
        {
            return new SqlQueryImpl();
        }
    }
}
