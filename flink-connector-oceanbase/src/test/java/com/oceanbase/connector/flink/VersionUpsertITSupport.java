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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

/** Shared Flink SQL steps for version-column upsert integration tests. */
final class VersionUpsertITSupport {

    static final String TABLE_DDL = "sql/mysql/version_upsert.sql";
    static final String TABLE = "t_version_upsert";

    private VersionUpsertITSupport() {}

    static void runVersionColumnUpsert(OceanBaseTestBase testBase) throws Exception {
        testBase.initialize(TABLE_DDL);
        StreamTableEnvironment tEnv = newTableEnv();
        createSinkTable(testBase, tEnv, true);
        insertAndAssert(testBase, tEnv, "(1, 'v1', 1)", "1,v1,1");
        insertAndAssert(testBase, tEnv, "(1, 'v2', 2)", "1,v2,2");
        insertAndAssert(testBase, tEnv, "(1, 'stale', 1)", "1,v2,2");
        testBase.dropTables(TABLE);
    }

    static void runDefaultUpsert(OceanBaseTestBase testBase) throws Exception {
        testBase.initialize(TABLE_DDL);
        StreamTableEnvironment tEnv = newTableEnv();
        createSinkTable(testBase, tEnv, false);
        insertAndAssert(testBase, tEnv, "(1, 'v1', 1)", "1,v1,1");
        insertAndAssert(testBase, tEnv, "(1, 'v2', 2)", "1,v2,2");
        insertAndAssert(testBase, tEnv, "(1, 'stale', 1)", "1,stale,1");
        testBase.dropTables(TABLE);
    }

    private static StreamTableEnvironment newTableEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        return StreamTableEnvironment.create(
                env, EnvironmentSettings.newInstance().inStreamingMode().build());
    }

    private static void createSinkTable(
            OceanBaseTestBase testBase, StreamTableEnvironment tEnv, boolean versionColumnUpsert)
            throws Exception {
        String versionOption =
                versionColumnUpsert ? "  'sink.upsert.version-column'='version'," : "";
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE target ("
                        + " `id` INT NOT NULL,"
                        + " data STRING,"
                        + " version INT,"
                        + " PRIMARY KEY (`id`) NOT ENFORCED"
                        + ") with ("
                        + "  'connector'='oceanbase',"
                        + "  'table-name'='"
                        + TABLE
                        + "',"
                        + versionOption
                        + testBase.getOptionsString()
                        + ");");
    }

    private static void insertAndAssert(
            OceanBaseTestBase testBase,
            StreamTableEnvironment tEnv,
            String values,
            String expectedRow)
            throws Exception {
        tEnv.executeSql("INSERT INTO target VALUES " + values).await();
        testBase.waitingAndAssertTableCount(TABLE, 1);
        assertEquals(Collections.singletonList(expectedRow), testBase.queryTable(TABLE));
    }
}
