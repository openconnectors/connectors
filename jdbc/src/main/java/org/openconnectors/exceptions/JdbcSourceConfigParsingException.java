package org.openconnectors.exceptions;

public class JdbcSourceConfigParsingException extends RuntimeException {

    public JdbcSourceConfigParsingException(String message) {
        super(message);
    }

    public JdbcSourceConfigParsingException(String message, Throwable cause) {
        super(message, cause);
    }
}
