package com.revizer.counters.services.streaming.exceptions;

/**
 * Created by alanl on 11/11/14.
 */
public class StreamServiceListenerException extends Exception {

    public StreamServiceListenerException(String message) {
        super(message);
    }

    public StreamServiceListenerException(String message, Exception e) {
        super(message, e);
    }

    public StreamServiceListenerException(Exception e) {
        super(e);
    }

}