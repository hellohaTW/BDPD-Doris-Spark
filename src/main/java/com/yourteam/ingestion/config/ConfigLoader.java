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
 * Loads a {@link JobConfig} from a YAML file (or stream). YAML keys are snake_case; a
 * {@link PropertyNamingStrategies#SNAKE_CASE} mapper maps them to the camelCase config
 * fields, so no per-field annotations are needed. Unknown keys are ignored (forward-compat).
 * The parsed config is validated before it is returned.
 */
public final class ConfigLoader {

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory())
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private ConfigLoader() {
    }

    /** Loads and validates a config from a file path. */
    public static JobConfig load(Path path) {
        try (InputStream in = Files.newInputStream(path)) {
            return load(in);
        } catch (IOException e) {
            throw new ConfigException("Cannot read config file: " + path + " — " + e.getMessage(), e);
        }
    }

    /** Loads and validates a config from a stream (does not close it). */
    public static JobConfig load(InputStream in) {
        JobConfig config;
        try {
            config = MAPPER.readValue(in, JobConfig.class);
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
