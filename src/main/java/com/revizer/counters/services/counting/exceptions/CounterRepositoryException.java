package com.revizer.counters.services.counting.exceptions;

/**
 * Created by alanl on 11/13/14.
 */
public class CounterRepositoryException extends Exception {

    public CounterRepositoryException() {
    }

    public CounterRepositoryException(String message) {
        super(message);
    }

    public CounterRepositoryException(String message, Throwable cause) {
        super(message, cause);
    }

    public CounterRepositoryException(Throwable cause) {
        super(cause);
    }

    public CounterRepositoryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
