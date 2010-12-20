package com.socrata.balboa.metrics.data;

public interface EntityMeta
{
    boolean containsKey(String metric);
    String get(String metric);
}
