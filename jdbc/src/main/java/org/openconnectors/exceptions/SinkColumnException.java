package org.openconnectors.exceptions;

public class SinkColumnException extends RuntimeException {
    public SinkColumnException(String message) {
        super(message);
    }

    public SinkColumnException(String message, Throwable cause) {
        super(message, cause);
    }
}
