package com.revizer.counters.services.streaming.exceptions;

/**
 * Created by alanl on 11/11/14.
 */
public class MessageDecoderException extends Exception {

    public MessageDecoderException(String message) {
        super(message);
    }

    public MessageDecoderException(String message, Exception e) {
        super(message, e);
    }

    public MessageDecoderException(Exception e) {
        super(e);
    }

}
