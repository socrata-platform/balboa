package com.socrata.balboa.metrics.data.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;
import me.prettyprint.cassandra.testutils.EmbeddedServerHelper;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.thrift.transport.TTransportException;

/**
 * Mostly ripped off from the EmbeddedCassandraServiceHelper in Hector, but
 * there's a linking problem in the current Cassandra release (0.6.1) that makes
 * it so that I can't use it. Instead I'll just hack around it.
 */
public class CassandraHelper
{
    /**
     * A cleanup utility that wipes the cassandra data directories.
     *
     * Ripped from the Cassandra trunk since for whatever reason, 0.6.1 doesn't
     * seem to have it.
     */
    static class CassandraServiceDataCleaner
    {
        /**
        * Creates all data dir if they don't exist and cleans them
        * @throws IOException
        */
        public void prepare() throws IOException
        {
            makeDirsIfNotExist();
            cleanupDataDirectories();
        }

        /**
        * Deletes all data from cassandra data directories, including the commit log.
        * @throws IOException in case of permissions error etc.
        */
        public void cleanupDataDirectories() throws IOException
        {
            for (String s: getDataDirs())
            {
                cleanDir(s);
            }
        }

        /**
        * Creates the data diurectories, if they didn't exist.
        * @throws IOException if directories cannot be created (permissions etc).
        */
        public void makeDirsIfNotExist() throws IOException
        {
            for (String s: getDataDirs())
            {
                mkdir(s);
            }
        }

        /**
        * Collects all data dirs and returns a set of String paths on the file system.
        *
        * @return
        */
        private Set<String> getDataDirs()
        {
            Set<String> dirs = new HashSet<String>();
            for (String s : DatabaseDescriptor.getAllDataFileLocations())
            {
                dirs.add(s);
            }
            dirs.add(DatabaseDescriptor.getLogFileLocation());
            return dirs;
        }

        /**
        * Creates a directory
        *
        * @param dir
        * @throws IOException
        */
        private void mkdir(String dir) throws IOException
        {
            FileUtils.createDirectory(dir);
        }

        /**
        * Removes all directory content from file the system
        *
        * @param dir
        * @throws IOException
        */
        private void cleanDir(String dir) throws IOException
        {
            File dirFile = new File(dir);
            if (dirFile.exists() && dirFile.isDirectory())
            {
                FileUtils.delete(dirFile.listFiles());
            }
        }
    }

    private static final String TMP = "tmp";

    private EmbeddedCassandraService cassandra;

    /**
     * Set embedded cassandra up and spawn it in a new thread.
     *
     * @throws TTransportException
     * @throws IOException
     * @throws InterruptedException
     */
    public void setup() throws TTransportException, IOException, InterruptedException
    {
        // delete tmp dir first
        rmdir(TMP);
        // make a tmp dir and copy storag-conf.xml and log4j.properties to it
        copy("/storage-conf.xml", TMP);
        copy("/log4j.properties", TMP);
        System.setProperty("storage-config", TMP);

        CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
        cleaner.prepare();
        cassandra = new EmbeddedCassandraService();
        cassandra.init();
        Thread t = new Thread(cassandra);
        t.setDaemon(true);
        t.start();
    }

    public void teardown()
    {
        CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();

        try {
            cleaner.cleanupDataDirectories();
            rmdir(TMP);
        } catch (IOException e) {
            // IGNORE
        }
    }

    private static void rmdir(String dir) throws IOException
    {
        File dirFile = new File(dir);
        if (dirFile.exists()) {
            FileUtils.deleteDir(new File(dir));
        }
    }
    /**
     * Copies a resource from within the jar to a directory.
     *
     * @param resource
     * @param directory
     * @throws IOException
     */
    private static void copy(String resource, String directory) throws IOException
    {
        mkdir(directory);
        InputStream is = EmbeddedServerHelper.class.getResourceAsStream(resource);
        String fileName = resource.substring(resource.lastIndexOf("/") + 1);
        File file = new File(directory + System.getProperty("file.separator") + fileName);
        OutputStream out = new FileOutputStream(file);
        byte buf[] = new byte[1024];
        int len;
        while ((len = is.read(buf)) > 0) {
            out.write(buf, 0, len);
        }
        out.close();
        is.close();
    }

    /**
     * Creates a directory
     * @param dir
     * @throws IOException
     */
    private static void mkdir(String dir) throws IOException
    {
        FileUtils.createDirectory(dir);
    }
}
