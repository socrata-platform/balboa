package com.socrata.balboa.metrics.messaging;

import com.socrata.balboa.metrics.Summary;

import java.io.IOException;

/**
 * Receivers are how messages are persisted. They receive a summary and persist
 * it in whatever way they deem appropriate.
 */
public interface Receiver
{
    /** Called when a summary is received. */
    public void received(String entityId, Summary summary) throws IOException;
}
