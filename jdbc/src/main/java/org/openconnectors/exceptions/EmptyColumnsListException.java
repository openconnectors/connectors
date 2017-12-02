package org.openconnectors.exceptions;

public class EmptyColumnsListException extends RuntimeException {
    public EmptyColumnsListException(String message) {
        super(message);
    }

    public EmptyColumnsListException(String message, Throwable cause) {
        super(message, cause);
    }
}
