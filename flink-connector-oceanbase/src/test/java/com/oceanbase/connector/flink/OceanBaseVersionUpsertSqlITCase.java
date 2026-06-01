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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

/**
 * Version-upsert integration test with Testcontainers (same OB CE setup as {@link
 * OceanBaseMySQLConnectorITCase}).
 */
public class OceanBaseVersionUpsertSqlITCase extends OceanBaseMySQLTestBase {

    private static final Logger LOG =
            LoggerFactory.getLogger(OceanBaseVersionUpsertSqlITCase.class);

    @BeforeAll
    public static void setup() {
        CONTAINER.withLogConsumer(new Slf4jLogConsumer(LOG)).start();
    }

    @AfterAll
    public static void tearDown() {
        CONTAINER.stop();
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
