package com.socrata.balboa.agent;

public enum HttpOrMq {
    HTTP("HTTP"), MQ("MQ");

    private final String name;

    HttpOrMq(String s) {
        name = s;
    }

    public boolean equalsName(String otherName) {
        return otherName != null && name.equals(otherName);
    }

    public String toString() {
        return this.name;
    }
}
