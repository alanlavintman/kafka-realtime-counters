package com.revizer.counters.v1.counters.metadata;

/**
 * Created by alanl on 11/11/14.
 */
public class AggregationCounterKey {
    private String counterKey;
    private String date;

    public AggregationCounterKey() {
    }

    public AggregationCounterKey(String counterKey, String date) {
        this.counterKey = counterKey;
        this.date = date;
    }

    public String getCounterKey() {
        return counterKey;
    }

    public void setCounterKey(String counterKey) {
        this.counterKey = counterKey;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AggregationCounterKey that = (AggregationCounterKey) o;

        if (!counterKey.equals(that.counterKey)) return false;
        if (!date.equals(that.date)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = counterKey.hashCode();
        result = 31 * result + date.hashCode();
        return result;
    }
}