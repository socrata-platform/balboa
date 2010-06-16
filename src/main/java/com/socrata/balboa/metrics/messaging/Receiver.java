package com.socrata.balboa.metrics.messaging;

import com.socrata.balboa.metrics.Summary;

public interface Receiver
{
    public void received(String entityId, Summary summary);
}
