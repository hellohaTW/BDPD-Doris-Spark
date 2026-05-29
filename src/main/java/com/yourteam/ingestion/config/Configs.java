package com.yourteam.ingestion.config;

/** Small shared helpers for config validation. */
final class Configs {

    private Configs() {
    }

    static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }
}
