package com.yourteam.ingestion.config;

/**
 * A root config object that can validate itself. Implemented by every top-level config type
 * ({@link JobConfig} for the streaming job, {@link MigrationConfig} for the one-shot migration)
 * so {@link ConfigLoader} can parse and validate any of them through one generic entry point.
 */
public interface Validatable {

    /**
     * Fails fast if any required section or field is missing/blank.
     *
     * @throws ConfigException listing all missing/blank required fields
     */
    void validate();
}
