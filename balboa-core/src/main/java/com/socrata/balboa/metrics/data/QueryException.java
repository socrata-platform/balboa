package com.socrata.balboa.metrics.data;

/**
 * An exception occurring while creating the query
 */
public class QueryException extends RuntimeException {
    public QueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueryException(String message) {
        super(message);
    }
}
