package com.yourteam.ingestion.config;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Doris sink settings.
 *
 * <p>The password is never stored in YAML. {@link #passwordEnv} names the environment
 * variable that holds it; resolve it at launch with {@link #resolvePassword()}.
 */
@Value
@Builder
@Jacksonized
public class DorisConfig {

    /** Required. Doris FE http nodes, e.g. {@code 127.0.0.1:8030}. */
    String fenodes;

    /** Required. Target database. */
    String database;

    /** Required. Target table (shares the fixed 7-column layout). */
    String table;

    /** Required. Doris user. */
    String user;

    /**
     * Optional. Name of the env var holding the Doris password (NOT the password itself). If it is
     * omitted, or the named env var is not present, the password defaults to empty ({@code ""}) —
     * which is what a Doris user with no password expects.
     */
    String passwordEnv;

    /** Optional. Extra {@code doris.*} writer options passed through verbatim. */
    @Builder.Default
    Map<String, String> options = Collections.emptyMap();

    /** {@code database.table}, as the Doris connector expects it. */
    public String tableIdentifier() {
        return database + "." + table;
    }

    /** Reads the password from the process environment named by {@link #passwordEnv}. */
    public String resolvePassword() {
        return resolvePassword(System.getenv());
    }

    /**
     * Resolves the password from the supplied environment. Defaults to empty ({@code ""}) when no
     * {@code password_env} is configured or the named env var is absent — it never throws, so a
     * passwordless Doris user works out of the box.
     */
    public String resolvePassword(Map<String, String> env) {
        if (Configs.isBlank(passwordEnv)) {
            return "";
        }
        String password = env.get(passwordEnv);
        return password != null ? password : "";
    }

    void collectMissing(List<String> missing) {
        if (Configs.isBlank(fenodes)) {
            missing.add("doris.fenodes");
        }
        if (Configs.isBlank(database)) {
            missing.add("doris.database");
        }
        if (Configs.isBlank(table)) {
            missing.add("doris.table");
        }
        if (Configs.isBlank(user)) {
            missing.add("doris.user");
        }
        // password_env is optional — an omitted/absent password defaults to empty (see resolvePassword).
    }
}
