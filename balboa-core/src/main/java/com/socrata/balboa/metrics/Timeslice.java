package com.socrata.balboa.metrics;

public class Timeslice {
    long start;
    long end;
    Metrics metrics;

    public Timeslice(long start, long end, Metrics metrics) {
        this.start = start;
        this.end = end;
        this.metrics = metrics;
    }

    public Timeslice() {}

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public void setMetrics(Metrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Timeslice timeslice = (Timeslice) o;

        if (end != timeslice.end) return false;
        if (start != timeslice.start) return false;
        if (metrics != null ? !metrics.equals(timeslice.metrics) : timeslice.metrics != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (start ^ (start >>> 32));
        result = 31 * result + (int) (end ^ (end >>> 32));
        result = 31 * result + (metrics != null ? metrics.hashCode() : 0);
        return result;
    }
}
