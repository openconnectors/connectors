package org.openconnectors.exceptions;

public class SinkRecordException extends RuntimeException {
    public SinkRecordException(String message) {
        super(message);
    }

    public SinkRecordException(String message, Throwable cause) {
        super(message, cause);
    }
}
