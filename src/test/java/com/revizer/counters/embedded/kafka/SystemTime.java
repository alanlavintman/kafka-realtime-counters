package com.revizer.counters.embedded.kafka;

import kafka.utils.Time;

/**
 * Created by alanl on 12/4/14.
 */
public class SystemTime implements Time {

    public long milliseconds() {
        return System.currentTimeMillis();
    }

    public long nanoseconds() {
        return System.nanoTime();
    }

    public void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // Ignore
        }
    }

}
