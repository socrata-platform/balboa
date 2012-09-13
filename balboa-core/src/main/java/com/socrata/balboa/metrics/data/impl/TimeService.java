package com.socrata.balboa.metrics.data.impl;

/**
 * Returns the current time. Does not go backwards.
 */
public class TimeService {
    long lastTime = 0;

    /**
     * Returns the current time, or the last time returned if
     * the system clock is going backwards. Yes. This happened.
     * Yes. Cassandra apparently requires the timestamp to move
     * forwards, and I need a time-mock anyways... so...
     */
    public long currentTimeMillis() {
        long currentTime = System.currentTimeMillis();
        if (currentTime < lastTime) {
            return lastTime;
        } else {
            lastTime = currentTime;
            return currentTime;
        }

    }
}
