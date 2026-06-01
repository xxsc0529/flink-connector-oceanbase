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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Version-upsert integration test against an external OceanBase / MySQL instance.
 *
 * <p>See {@code docs/sink/ob-sink-version-upsert-usage.md} for environment variables and examples.
 */
public class OceanBaseVersionUpsertExternalITCase extends ExternalOceanBaseJdbcTestBase {

    private static final EnvVarNames ENV =
            new EnvVarNames(
                    "OCEANBASE_VERSION_UPSERT_IT_JDBC_URL",
                    "OCEANBASE_VERSION_UPSERT_IT_USERNAME",
                    "OCEANBASE_VERSION_UPSERT_IT_PASSWORD",
                    "OCEANBASE_VERSION_UPSERT_IT_SCHEMA");

    @Override
    protected EnvVarNames envVarNames() {
        return ENV;
    }

    @BeforeAll
    static void loadEnv() {
        loadJdbcEnv(ENV);
    }

    @AfterAll
    static void dropTableIfConfigured() throws SQLException {
        if (jdbcUrl == null) {
            return;
        }
        try (Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
                Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + VersionUpsertITSupport.TABLE);
        }
    }

    @BeforeEach
    void assumeEnv() {
        assumeJdbcEnv(ENV);
    }

    @Test
    public void testVersionColumnUpsert() throws Exception {
        VersionUpsertITSupport.runVersionColumnUpsert(this);
    }

    @Test
    public void testDefaultUpsertWithoutVersionCompare() throws Exception {
        VersionUpsertITSupport.runDefaultUpsert(this);
    }
}
