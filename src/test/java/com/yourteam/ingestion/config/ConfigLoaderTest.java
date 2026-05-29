package com.yourteam.ingestion.config;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigLoaderTest {

    private static InputStream yaml(String s) {
        return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void loadsAllFieldsFromValidYaml() {
        JobConfig cfg;
        try (InputStream in = getClass().getResourceAsStream("/valid-job.yaml")) {
            cfg = ConfigLoader.load(in);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        assertEquals("localhost:9092", cfg.getKafka().getBootstrapServers());
        assertEquals("orders", cfg.getKafka().getTopic());
        assertEquals("earliest", cfg.getKafka().getStartingOffsets());
        assertEquals(Long.valueOf(1000L), cfg.getKafka().getMaxOffsetsPerTrigger());

        assertEquals("127.0.0.1:8030", cfg.getDoris().getFenodes());
        assertEquals("ods.orders_raw", cfg.getDoris().tableIdentifier());
        assertEquals("root", cfg.getDoris().getUser());
        assertEquals("DORIS_PASSWORD", cfg.getDoris().getPasswordEnv());
        assertEquals("10000", cfg.getDoris().getOptions().get("sink.batch.size"));
        assertEquals(2, cfg.getDoris().getOptions().size());

        assertEquals("/tmp/ckpt/orders", cfg.getSpark().getCheckpointLocation());
        assertEquals("append", cfg.getSpark().getOutputMode());
        assertEquals("30 seconds", cfg.getSpark().getTrigger());
        assertEquals("4", cfg.getSpark().getExtraConf().get("spark.sql.shuffle.partitions"));
    }

    @Test
    void appliesDefaultsAndEmptyMapsWhenOptionalFieldsOmitted() {
        String minimal =
                "kafka:\n"
                        + "  bootstrap_servers: \"b:9092\"\n"
                        + "  topic: \"t\"\n"
                        + "doris:\n"
                        + "  fenodes: \"fe:8030\"\n"
                        + "  database: \"db\"\n"
                        + "  table: \"tbl\"\n"
                        + "  user: \"u\"\n"
                        + "  password_env: \"PW\"\n"
                        + "spark:\n"
                        + "  checkpoint_location: \"/tmp/c\"\n";

        JobConfig cfg = ConfigLoader.load(yaml(minimal));

        // Defaults kick in for omitted optionals.
        assertEquals("latest", cfg.getKafka().getStartingOffsets());
        assertNull(cfg.getKafka().getMaxOffsetsPerTrigger());
        assertEquals("append", cfg.getSpark().getOutputMode());
        assertNull(cfg.getSpark().getTrigger());

        // Omitted maps are empty, never null.
        assertEquals(Collections.emptyMap(), cfg.getDoris().getOptions());
        assertEquals(Collections.emptyMap(), cfg.getSpark().getExtraConf());
    }

    @Test
    void failsFastListingEveryMissingRequiredField() {
        // Missing kafka.topic, doris.fenodes, and the whole spark section.
        String bad =
                "kafka:\n"
                        + "  bootstrap_servers: \"b:9092\"\n"
                        + "doris:\n"
                        + "  database: \"db\"\n"
                        + "  table: \"tbl\"\n"
                        + "  user: \"u\"\n"
                        + "  password_env: \"PW\"\n";

        ConfigException ex = assertThrows(ConfigException.class, () -> ConfigLoader.load(yaml(bad)));
        String msg = ex.getMessage();
        assertTrue(msg.contains("kafka.topic"), msg);
        assertTrue(msg.contains("doris.fenodes"), msg);
        assertTrue(msg.contains("spark"), msg);
    }

    @Test
    void resolvesPasswordFromEnvVarNamedByConfig() {
        JobConfig cfg = ConfigLoader.load(yaml(
                "kafka: {bootstrap_servers: b, topic: t}\n"
                        + "doris: {fenodes: fe, database: db, table: tbl, user: u, password_env: DORIS_PW}\n"
                        + "spark: {checkpoint_location: /tmp/c}\n"));

        Map<String, String> env = new HashMap<>();
        env.put("DORIS_PW", "s3cret");
        assertEquals("s3cret", cfg.getDoris().resolvePassword(env));

        // Missing env var -> ConfigException naming the var.
        ConfigException ex = assertThrows(ConfigException.class,
                () -> cfg.getDoris().resolvePassword(Collections.emptyMap()));
        assertTrue(ex.getMessage().contains("DORIS_PW"), ex.getMessage());
    }

    @Test
    void rejectsMalformedYaml() {
        assertThrows(ConfigException.class, () -> ConfigLoader.load(yaml("kafka: [this is not a map")));
    }
}
