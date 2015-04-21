package com.socrata.balboa.metrics.config;

import com.socrata.balboa.metrics.data.Period;

import java.io.IOException;
import java.util.*;

public abstract class Configuration extends Properties {
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
