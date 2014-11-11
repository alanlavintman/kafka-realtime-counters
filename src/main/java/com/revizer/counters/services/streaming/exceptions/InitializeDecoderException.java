package com.revizer.counters.services.streaming.exceptions;

/**
 * Created by alanl on 11/10/14.
 */
public class InitializeDecoderException extends RuntimeException {

    private static final long serialVersionUID = 4758535014429852589L;

    public InitializeDecoderException(String message) {
        super(message);
    }

    public InitializeDecoderException(String message, Exception e) {
        super(message, e);
    }

    public InitializeDecoderException(Exception e) {
        super(e);
    }

}
