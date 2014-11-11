package com.revizer.counters.services.counting.exceptions;

/**
 * Created by alanl on 11/10/14.
 */
public class ComposeKeyException extends Exception {

    public ComposeKeyException(String message) {
        super(message);
    }

    public ComposeKeyException(String message, Exception e) {
        super(message, e);
    }

    public ComposeKeyException(Exception e) {
        super(e);
    }

}
