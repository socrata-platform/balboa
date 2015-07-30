package com.socrata.balboa.common.config;

/**
 * Exception for MisConfigurations.
 */
public class ConfigurationException extends RuntimeException {

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
