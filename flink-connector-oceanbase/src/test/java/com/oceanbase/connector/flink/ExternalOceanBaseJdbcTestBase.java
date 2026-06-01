/*
 * Copyright 2024 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oceanbase.connector.flink;

import org.junit.jupiter.api.Assumptions;

/**
 * {@link OceanBaseTestBase} wired from environment variables (no Testcontainers).
 *
 * <p>Subclasses define {@link #envVarNames()} with at least {@code jdbcUrl} and {@code username}
 * keys.
 */
public abstract class ExternalOceanBaseJdbcTestBase extends OceanBaseTestBase {

    protected static String jdbcUrl;
    protected static String jdbcUser;
    protected static String jdbcPassword;
    protected static String schemaName;

    protected static void loadJdbcEnv(EnvVarNames names) {
        if (!isEnvPresent(names)) {
            return;
        }
        jdbcUrl = System.getenv(names.jdbcUrl);
        jdbcUser = System.getenv(names.username);
        jdbcPassword = System.getenv(names.password) != null ? System.getenv(names.password) : "";
        schemaName = firstNonBlank(System.getenv(names.schema), "test");
    }

    protected static void assumeJdbcEnv(EnvVarNames names) {
        Assumptions.assumeTrue(
                isEnvPresent(names),
                () -> "skip: set non-blank " + names.jdbcUrl + " and " + names.username);
    }

    protected static boolean isEnvPresent(EnvVarNames names) {
        String url = System.getenv(names.jdbcUrl);
        String user = System.getenv(names.username);
        return url != null && !url.trim().isEmpty() && user != null && !user.trim().isEmpty();
    }

    protected abstract EnvVarNames envVarNames();

    @Override
    public String getHost() {
        return "";
    }

    @Override
    public int getPort() {
        return 0;
    }

    @Override
    public int getRpcPort() {
        return 0;
    }

    @Override
    public String getJdbcUrl() {
        return jdbcUrl;
    }

    @Override
    public String getClusterName() {
        return "";
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public String getSysUsername() {
        return "";
    }

    @Override
    public String getSysPassword() {
        return "";
    }

    @Override
    public String getUsername() {
        return jdbcUser;
    }

    @Override
    public String getPassword() {
        return jdbcPassword;
    }

    protected static String firstNonBlank(String preferred, String fallback) {
        if (preferred != null && !preferred.trim().isEmpty()) {
            return preferred;
        }
        return fallback;
    }

    /** Environment variable names for external JDBC integration tests. */
    protected static final class EnvVarNames {
        final String jdbcUrl;
        final String username;
        final String password;
        final String schema;

        EnvVarNames(String jdbcUrl, String username, String password, String schema) {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            this.schema = schema;
        }
    }
}
