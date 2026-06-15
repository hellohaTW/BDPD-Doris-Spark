package com.yourteam.ingestion.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

/**
 * Loads a {@link JobConfig} from a YAML file (or stream). YAML keys are snake_case; a
 * {@link PropertyNamingStrategies#SNAKE_CASE} mapper maps them to the camelCase config
 * fields, so no per-field annotations are needed. Unknown keys are ignored (forward-compat).
 * The parsed config is validated before it is returned.
 *
 * <p>The location may be a plain local path (read via NIO) or a Hadoop-FileSystem URI such as
 * {@code s3a://bucket/job-config.yaml} or {@code hdfs:///path/job-config.yaml} (read via the
 * matching Hadoop {@link FileSystem}). For {@code s3a://}, the cluster needs {@code hadoop-aws} and
 * S3 credentials on the classpath/conf — usually already present where the checkpoint also lives.
 */
public final class ConfigLoader {

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory())
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private ConfigLoader() {
    }

    /**
     * Loads and validates a config from a location string: a plain local path, or a Hadoop URI
     * with a scheme (e.g. {@code s3a://}, {@code hdfs://}, {@code file://}).
     */
    public static JobConfig load(String location) {
        return hasUriScheme(location) ? loadFromHadoop(location) : load(Paths.get(location));
    }

    /** Loads and validates a config from a local file path. */
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

    /** True if {@code location} starts with a URI scheme like {@code s3a://} / {@code hdfs://}. */
    private static boolean hasUriScheme(String location) {
        int idx = location.indexOf("://");
        return idx > 0 && location.substring(0, idx).matches("[a-zA-Z][a-zA-Z0-9+.-]*");
    }

    /** Reads the config through the Hadoop FileSystem resolved from the URI scheme. */
    private static JobConfig loadFromHadoop(String location) {
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(location);
        try {
            FileSystem fs = path.getFileSystem(new Configuration());
            try (FSDataInputStream in = fs.open(path)) {
                return load(in);
            }
        } catch (IOException e) {
            throw new ConfigException(
                    "Cannot read config from " + location + " — " + e.getMessage(), e);
        }
    }
}
