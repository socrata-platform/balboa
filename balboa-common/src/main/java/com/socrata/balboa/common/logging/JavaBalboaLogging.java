package com.socrata.balboa.common.logging;

import org.slf4j.Logger;

/**
 * Java Class binding for {@link com.socrata.balboa.common.logging.BalboaLogging}
 */
public class JavaBalboaLogging extends BalboaLoggingForJava {

    /**
     * Returns the Logger instance for argument class.
     *
     * @see {@link com.socrata.balboa.common.logging.BalboaLoggingForJava}
     *
     * @param clazz Class to create logger for.
     * @param <T> The type of the class for the logger.
     * @return The {@link org.slf4j.Logger} instance.
     */
    public static <T> Logger instance(Class<T> clazz) {
        return new BalboaLoggingForJava().forClass(clazz);
    }

}
