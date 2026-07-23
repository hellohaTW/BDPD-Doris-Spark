package com.yourteam.ingestion.config;

/**
 * Thrown when a job config cannot be read, parsed, or fails required-field validation.
 * Carries a human-readable message naming the offending field(s).
 */
public class ConfigException extends RuntimeException {

    public ConfigException(String message) {
        super(message);
    }

    public ConfigException(String message, Throwable cause) {
        super(message, cause);
    }
}
