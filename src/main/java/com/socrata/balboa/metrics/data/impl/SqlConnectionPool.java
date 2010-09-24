package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.config.Configuration;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class SqlConnectionPool
{
    private static Log log = LogFactory.getLog(SqlConnectionPool.class);

    private static SqlConnectionPool instance;

    public synchronized static SqlConnectionPool get() throws IOException
    {
        if (instance == null)
        {
            instance = new SqlConnectionPool();
        }

        return instance;
    }

    private BasicDataSource ds;

    SqlConnectionPool() throws IOException
    {
        BasicDataSource dataSource = new BasicDataSource();

        String url = (String)Configuration.get().get("sql.url");
        String user = (String)Configuration.get().get("sql.username");
        String password = (String)Configuration.get().get("sql.password");

        dataSource.setUrl(url);
        dataSource.setUsername(user);
        dataSource.setPassword(password);
        
        this.ds = dataSource;
    }

    public Connection getConnection() throws SQLException
    {
        long startTime = System.currentTimeMillis();

        try
        {
            log.trace("There are " + ds.getNumActive() + " active sql connections in pool.");
            log.trace("There are " + ds.getNumIdle() + " idle sql connections in pool.");
            return ds.getConnection();
        }
        finally
        {
            long totalTime = System.currentTimeMillis() - startTime;
            log.trace("Total time acquiring connection to sql server " + totalTime + " (ms)");
        }
    }
}
