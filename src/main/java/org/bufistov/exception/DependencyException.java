package org.bufistov.exception;

public class DependencyException extends RuntimeException {
    public DependencyException(String message, Throwable cause) {
        super(message, cause);
    }
}
