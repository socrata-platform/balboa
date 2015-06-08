package com.socrata.balboa.metrics.config;

import com.socrata.balboa.metrics.data.Period;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public abstract class Configuration extends Properties {

    /**
     * TODO: Can't figure out why we are mutex locking Configuration Queries.
     */

    public static Configuration instance;
    private List<Period> supportedPeriods;

    /**
     * Retrieves the synchronized singleton instance of this Balboa Configuration.
     *
     * @return The Balboa Configuration instance.
     * @throws IOException If unable to read the configuration file.
     */
    public static synchronized Configuration get() throws IOException {
        String environment = System.getProperty("socrata.env");
        if (environment == null) {
            environment = "development";
        }

        if (instance == null) {
            if (environment.equals("test")) {
                instance = new PropertiesFixtureConfiguration();
            } else {
                instance = new PropertiesConfiguration();
            }
        }

        return instance;
    }

    /**
     * Don't use this, it's only for mocking.
     */
     public synchronized void setSupportedTypes(List<Period> supportedPeriods) {
        this.supportedPeriods = supportedPeriods;
    }

    public synchronized List<Period> getSupportedPeriods() {
        if (supportedPeriods == null) {
            String[] types = getProperty("balboa.summaries").split(",");

            supportedPeriods = new ArrayList<Period>(types.length);
            for (String t : types) {
                supportedPeriods.add(Period.valueOf(t.toUpperCase()));
            }
        }

        return supportedPeriods;
    }

    /**
     * Finds list of values that are comma separated for a given list.
     *
     * @param key Property key.
     * @return The list of values for a particular key.
     */
    public synchronized List<String> getList(String key) {
        return getList(key, null);
    }

    /**
     * Given a key to find in the property file find the list of values for that key.  If the delimiter is
     * null, "," will be used as a default delimiter.
     *
     * @param key The key to find the value for.
     * @param delimiter The delimiter String REGEX that is used to extract values
     * @return List of String Values for a specific key
     */
    public synchronized List<String> getList(String key, String delimiter) {
        delimiter = delimiter == null ? "," : delimiter;
        return new ArrayList<>(Arrays.asList(getString(key).split(delimiter)));
    }

    /**
     * The File found with the configuration key.
     *
     * @param key The Configuration Key to get.
     * @return The file pointed to by the configuration key.
     */
    public synchronized File getFile(String key) {
        require(key);
        return Paths.get(getProperty(key)).toFile();
    }

    /**
     * The File found with the configuration key.
     *
     * @param key The Configuration Key to get.
     * @return The file pointed to by the configuration key.
     */
    public synchronized File getFile(String key, File defaultValue) {
        if (!containsKey(key))
            return defaultValue;
        else
            return getFile(key);
    }

    /**
     * Return a String Property for key.  Generalized method to be used
     *
     *
     * @param key The Key that references a String.
     * @return The String value
     * @throws IllegalArgumentException in case there is no property or it is not a String.
     */
    public synchronized String getString(String key) {
        require(key);
        return getProperty(key);
    }

    /**
     * Returns a String or a default property.
     *
     * @param key The key to look up.
     * @param defaultValue The default value if the key does not exist
     * @return The value associated to by the key or the default value if it does not exist
     */
    public synchronized String getString(String key, String defaultValue) {
        if (!containsKey(key))
            return defaultValue;
        else
            return getProperty(key);
    }

    /**
     * Returns integer property.
     *
     * @param key The key that points to Int value.
     * @return The integer
     * @throws java.lang.IllegalArgumentException If key does not exist.
     */
    public synchronized int getInt(String key) {
        require(key);
        return Integer.valueOf(getProperty(key));
    }

    public synchronized int getInt(String key, int defaultValue) {
        if (!containsKey(key))
            return defaultValue;
        else
            return getInt(key);
    }

    public synchronized long getLong(String key) {
        require(key);
        return Long.valueOf(getProperty(key));
    }

    public synchronized long getLong(String key, long defaultValue) {
        if (!containsKey(key))
            return defaultValue;
        else
            return getLong(key);
    }


    // Private Helper Methods.

    /**
     * Ensure a specific key is the properties.
     * @param key Key.
     * @return this if self.
     */
    private void require(String key) {
        if (!containsKey(key))
            throw new IllegalArgumentException("Missing Configuration \"" + key + "\"");
    }
}
