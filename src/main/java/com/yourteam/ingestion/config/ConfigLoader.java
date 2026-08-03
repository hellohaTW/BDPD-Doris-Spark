package com.yourteam.ingestion.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * Loads a config from a YAML file (or stream). YAML keys are snake_case; a
 * {@link PropertyNamingStrategies#SNAKE_CASE} mapper maps them to the camelCase config
 * fields, so no per-field annotations are needed. Unknown keys are ignored (forward-compat).
 * The parsed config is validated before it is returned.
 *
 * <p>The type-parameterised {@link #load(Path, Class)} handles any {@link Validatable} root
 * ({@link JobConfig} for the streaming job, {@link MigrationConfig} for the one-shot migration);
 * the no-type overloads keep the original {@link JobConfig} shorthand.
 */
public final class ConfigLoader {

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory())
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private ConfigLoader() {
    }

    /** Loads and validates a {@link JobConfig} from a file path. */
    public static JobConfig load(Path path) {
        return load(path, JobConfig.class);
    }

    /** Loads and validates a {@link JobConfig} from a stream (does not close it). */
    public static JobConfig load(InputStream in) {
        return load(in, JobConfig.class);
    }

    /** Loads and validates a config of the given type from a file path. */
    public static <T extends Validatable> T load(Path path, Class<T> type) {
        try (InputStream in = Files.newInputStream(path)) {
            return load(in, type);
        } catch (IOException e) {
            throw new ConfigException("Cannot read config file: " + path + " — " + e.getMessage(), e);
        }
    }

    /** Loads and validates a config of the given type from a stream (does not close it). */
    public static <T extends Validatable> T load(InputStream in, Class<T> type) {
        T config;
        try {
            config = MAPPER.readValue(in, type);
        } catch (IOException e) {
            throw new ConfigException("Cannot parse YAML config: " + e.getMessage(), e);
        }
        if (config == null) {
            throw new ConfigException("Config is empty");
        }
        config.validate();
        return config;
    }
}
