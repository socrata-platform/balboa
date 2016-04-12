package com.socrata.balboa.metrics.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Attempts to load configuration from properties files.
 *
 * 1. Looks for a properties file on the classpath.
 * 2. Looks for a properties file specified by a system property.
 * 3. Looks for a properties file in a default location.
 *
 * The first properties file is always loaded, the second is loaded if it is
 * specified, and the third is load if the second isn't specified and the third
 * one exists.
 */
public class PropertiesConfiguration extends Configuration {
    private static final Logger logger = LoggerFactory.getLogger(PropertiesConfiguration.class);

    private static final String CLASSPATH_CONFIG_PROPERTIES = "config/config.properties";
    private static final String CONFIG_PROPERTY = "balboa.config";
    private static final File DEFAULT_PROPERTIES_FILE = new File("/etc/balboa.properties");

    public PropertiesConfiguration() throws IOException {
        // First load the base configuration file (config.properties) which is required in all environments.
        logger.info("Loading properties from '{}' from the classpath.", CLASSPATH_CONFIG_PROPERTIES);
        load(PropertiesConfiguration.class.getClassLoader().getResourceAsStream(CLASSPATH_CONFIG_PROPERTIES));

        String override = System.getProperty(CONFIG_PROPERTY);
        if (override != null) {
            File config = new File(override).getAbsoluteFile();
            logger.info("Loading properties from '{}' specified by '{}'.", config.getAbsolutePath(), CONFIG_PROPERTY);
            load(config);
        } else {
            logger.info("The property '{}' was not specified. Looking for default properties file '{}'.",
                        CONFIG_PROPERTY, DEFAULT_PROPERTIES_FILE.getAbsolutePath());
            if (!DEFAULT_PROPERTIES_FILE.exists()) {
                logger.warn("The file '{}' doesn't exist.", DEFAULT_PROPERTIES_FILE.getAbsolutePath());
            } else if (DEFAULT_PROPERTIES_FILE.isDirectory()) {
                logger.warn("The file '{}' is a directory and can't be read.",
                            DEFAULT_PROPERTIES_FILE.getAbsolutePath());
            } else if (!DEFAULT_PROPERTIES_FILE.canRead()) {
                logger.warn("The file '{}' isn't readable.", DEFAULT_PROPERTIES_FILE.getAbsolutePath());
            } else {
                load(DEFAULT_PROPERTIES_FILE);
            }
        }
    }

    void load(File config) throws IOException {
        if (config.exists()) {
            InputStream stream = new FileInputStream(config);
            load(stream);
        } else {
            throw new IOException(String.format("The file '%s' doesn't exist.", config.getAbsolutePath()));
        }
    }
}
