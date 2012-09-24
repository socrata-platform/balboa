package com.socrata.balboa.metrics.config;

import com.socrata.balboa.metrics.data.Period;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public abstract class Configuration extends Properties {
    public static Configuration instance;
    private List<Period> supportedPeriods;

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
    synchronized public void setSupportedTypes(List<Period> supportedPeriods) {
        this.supportedPeriods = supportedPeriods;
    }

    synchronized public List<Period> getSupportedPeriods() {
        if (supportedPeriods == null) {
            String[] types = getProperty("balboa.summaries").split(",");

            supportedPeriods = new ArrayList<Period>(types.length);
            for (String t : types) {
                supportedPeriods.add(Period.valueOf(t.toUpperCase()));
            }
        }

        return supportedPeriods;
    }

    public static class ConfigurationException extends RuntimeException {
        public ConfigurationException() {
            super();
        }

        public ConfigurationException(String s) {
            super(s);
        }

        public ConfigurationException(String s, Throwable throwable) {
            super(s, throwable);
        }

        public ConfigurationException(Throwable throwable) {
            super(throwable);
        }
    }
}
