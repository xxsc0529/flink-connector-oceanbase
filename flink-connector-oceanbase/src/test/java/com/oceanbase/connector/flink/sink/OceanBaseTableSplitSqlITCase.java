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

package com.oceanbase.connector.flink.sink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * SQL-level integration test for table-name split sink routing against a real OceanBase instance.
 */
class OceanBaseTableSplitSqlITCase {

    static final class ItEnv {
        static final String JDBC_URL = "OCEANBASE_FILE_COMPLETION_IT_JDBC_URL";
        static final String USERNAME = "OCEANBASE_FILE_COMPLETION_IT_USERNAME";
        static final String PASSWORD = "OCEANBASE_FILE_COMPLETION_IT_PASSWORD";
        static final String SCHEMA = "OCEANBASE_FILE_COMPLETION_IT_SCHEMA";
        static final String TABLE_BASE = "OCEANBASE_TABLE_SPLIT_IT_TABLE_BASE";

        private ItEnv() {}
    }

    private static String jdbcUrl;
    private static String jdbcUser;
    private static String jdbcPassword;
    private static String obSchema;
    private static String baseTable;
    private static String tableCn;
    private static String tableUs;

    private static MockProducer<String, String> mockProducer;

    @BeforeAll
    static void createTables() throws SQLException {
        if (!itEnvPresent()) {
            return;
        }
        jdbcUrl = System.getenv(ItEnv.JDBC_URL);
        jdbcUser = System.getenv(ItEnv.USERNAME);
        jdbcPassword = System.getenv(ItEnv.PASSWORD) != null ? System.getenv(ItEnv.PASSWORD) : "";
        obSchema = firstNonBlank(System.getenv(ItEnv.SCHEMA), "test");
        baseTable = firstNonBlank(System.getenv(ItEnv.TABLE_BASE), "t_user_info_split_it");
        tableCn = baseTable + "_cn";
        tableUs = baseTable + "_us";
        String ddl = "(id INT PRIMARY KEY, name VARCHAR(64), country VARCHAR(16))";
        try (Connection conn = jdbc();
                Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + tableCn);
            st.execute("DROP TABLE IF EXISTS " + tableUs);
            st.execute("DROP TABLE IF EXISTS " + baseTable);
            st.execute("CREATE TABLE " + baseTable + ddl);
            st.execute("CREATE TABLE " + tableCn + ddl);
            st.execute("CREATE TABLE " + tableUs + ddl);
        }
    }

    @AfterAll
    static void dropTables() throws SQLException {
        if (jdbcUrl == null || baseTable == null) {
            return;
        }
        try (Connection conn = jdbc();
                Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + tableCn);
            st.execute("DROP TABLE IF EXISTS " + tableUs);
            st.execute("DROP TABLE IF EXISTS " + baseTable);
        }
    }

    @BeforeEach
    void assumeEnvAndTruncate() throws SQLException {
        Assumptions.assumeTrue(
                itEnvPresent(),
                () -> "skip: set non-blank " + ItEnv.JDBC_URL + " and " + ItEnv.USERNAME);
        truncateAll();
    }

    @AfterEach
    void tearDownKafkaMock() {
        KafkaFileCompletionNotifier.producerFactoryForTest = null;
        if (mockProducer != null) {
            mockProducer.close();
            mockProducer = null;
        }
    }

    @Test
    void suffixSplitRoutesToShardTables() throws Exception {
        runInsert(
                " (1, CAST('alice' AS STRING), CAST('China' AS STRING), CAST('cn' AS STRING)),"
                        + " (2, CAST('bob' AS STRING), CAST('USA' AS STRING), CAST('us' AS STRING))");

        assertEquals(Collections.singletonList("1|alice|China"), queryTable(tableCn));
        assertEquals(Collections.singletonList("2|bob|USA"), queryTable(tableUs));
        assertEquals(Collections.emptyList(), queryTable(baseTable));
    }

    @Test
    void emptySplitWritesToBaseTable() throws Exception {
        runInsert(" (10, CAST('no-region' AS STRING), CAST('N/A' AS STRING), CAST('' AS STRING))");

        assertEquals(Collections.singletonList("10|no-region|N/A"), queryTable(baseTable));
        assertEquals(Collections.emptyList(), queryTable(tableCn));
        assertEquals(Collections.emptyList(), queryTable(tableUs));
    }

    @Test
    void nullSplitWritesToBaseTable() throws Exception {
        runInsert(" (11, CAST('unknown' AS STRING), CAST('N/A' AS STRING), CAST(NULL AS STRING))");

        assertEquals(Collections.singletonList("11|unknown|N/A"), queryTable(baseTable));
        assertEquals(Collections.emptyList(), queryTable(tableCn));
        assertEquals(Collections.emptyList(), queryTable(tableUs));
    }

    @Test
    void mixedBatchInOneJob() throws Exception {
        runInsert(
                " (1, CAST('alice' AS STRING), CAST('China' AS STRING), CAST('cn' AS STRING)),"
                        + " (2, CAST('bob' AS STRING), CAST('USA' AS STRING), CAST('us' AS STRING)),"
                        + " (3, CAST('local' AS STRING), CAST('N/A' AS STRING), CAST('' AS STRING)),"
                        + " (4, CAST('anon' AS STRING), CAST('N/A' AS STRING), CAST(NULL AS STRING))");

        assertEquals(Collections.singletonList("1|alice|China"), queryTable(tableCn));
        assertEquals(Collections.singletonList("2|bob|USA"), queryTable(tableUs));
        assertEquals(Arrays.asList("3|local|N/A", "4|anon|N/A"), queryTable(baseTable));
    }

    @Test
    void splitWithFileCompletionRoutesToShardAndSendsKafka() throws Exception {
        mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        KafkaFileCompletionNotifier.producerFactoryForTest = props -> mockProducer;

        StreamTableEnvironment tEnv = newTableEnv();
        tEnv.executeSql(buildSinkDdlWithFileCompletion());
        tEnv.executeSql(
                        "INSERT INTO ob_split_sink VALUES "
                                + " (20, CAST('cn-row' AS STRING), CAST('China' AS STRING), CAST('cn' AS STRING),"
                                + "  false, CAST(NULL AS STRING)),"
                                + " (21, CAST('cn-done' AS STRING), CAST('China' AS STRING), CAST('cn' AS STRING),"
                                + "  true, CAST('oss://bucket/cn_file.parquet#done' AS STRING))")
                .await();

        assertEquals(Arrays.asList("20|cn-row|China", "21|cn-done|China"), queryTable(tableCn));
        assertEquals(Collections.emptyList(), queryTable(baseTable));
        assertEquals(Collections.emptyList(), queryTable(tableUs));

        List<ProducerRecord<String, String>> sent = mockProducer.history();
        assertEquals(1, sent.size());
        assertEquals("oss-split-file-events", sent.get(0).topic());
        assertEquals("oss://bucket/cn_file.parquet#done", sent.get(0).value());
    }

    private static void runInsert(String valuesClause) throws Exception {
        StreamTableEnvironment tEnv = newTableEnv();
        tEnv.executeSql(buildSinkDdl());
        tEnv.executeSql("INSERT INTO ob_split_sink VALUES " + valuesClause).await();
    }

    private static String buildSinkDdl() {
        return "CREATE TABLE ob_split_sink ("
                + " id INT,"
                + " name STRING,"
                + " country STRING,"
                + " split_column STRING,"
                + " PRIMARY KEY (id) NOT ENFORCED"
                + ") WITH ("
                + " 'connector'='oceanbase',"
                + " 'url'='"
                + jdbcUrl
                + "',"
                + " 'username'='"
                + jdbcUser
                + "',"
                + " 'password'='"
                + jdbcPassword
                + "',"
                + " 'schema-name'='"
                + obSchema
                + "',"
                + " 'table-name'='"
                + baseTable
                + "',"
                + " 'table-name.split-column'='split_column',"
                + " 'table-name.split-affix'='suffix',"
                + " 'table-name.split-concat'='_',"
                + " 'sync-write'='true',"
                + " 'memstore-check.enabled'='false'"
                + ")";
    }

    private static String buildSinkDdlWithFileCompletion() {
        return "CREATE TABLE ob_split_sink ("
                + " id INT,"
                + " name STRING,"
                + " country STRING,"
                + " split_column STRING,"
                + " is_eof BOOLEAN,"
                + " kafka_msg STRING,"
                + " PRIMARY KEY (id) NOT ENFORCED"
                + ") WITH ("
                + " 'connector'='oceanbase',"
                + " 'url'='"
                + jdbcUrl
                + "',"
                + " 'username'='"
                + jdbcUser
                + "',"
                + " 'password'='"
                + jdbcPassword
                + "',"
                + " 'schema-name'='"
                + obSchema
                + "',"
                + " 'table-name'='"
                + baseTable
                + "',"
                + " 'table-name.split-column'='split_column',"
                + " 'table-name.split-affix'='suffix',"
                + " 'table-name.split-concat'='_',"
                + " 'sync-write'='true',"
                + " 'memstore-check.enabled'='false',"
                + " 'file-completion.kafka.notification-enabled'='true',"
                + " 'file-completion.flag-column'='is_eof',"
                + " 'file-completion.message-column'='kafka_msg',"
                + " 'file-completion.kafka.topic'='oss-split-file-events',"
                + " 'file-completion.kafka.properties.bootstrap.servers'='dummy:9092'"
                + ")";
    }

    private static StreamTableEnvironment newTableEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        return StreamTableEnvironment.create(
                env, EnvironmentSettings.newInstance().inStreamingMode().build());
    }

    private static void truncateAll() throws SQLException {
        try (Connection conn = jdbc();
                Statement st = conn.createStatement()) {
            st.execute("TRUNCATE TABLE " + baseTable);
            st.execute("TRUNCATE TABLE " + tableCn);
            st.execute("TRUNCATE TABLE " + tableUs);
        }
    }

    private static List<String> queryTable(String table) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Connection conn = jdbc();
                Statement st = conn.createStatement();
                ResultSet rs =
                        st.executeQuery(
                                "SELECT id, name, country FROM " + table + " ORDER BY id")) {
            while (rs.next()) {
                out.add(rs.getInt(1) + "|" + rs.getString(2) + "|" + rs.getString(3));
            }
        }
        return out;
    }

    private static Connection jdbc() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
    }

    private static boolean itEnvPresent() {
        String url = System.getenv(ItEnv.JDBC_URL);
        String user = System.getenv(ItEnv.USERNAME);
        return url != null && !url.trim().isEmpty() && user != null && !user.trim().isEmpty();
    }

    private static String firstNonBlank(String preferred, String fallback) {
        if (preferred != null && !preferred.trim().isEmpty()) {
            return preferred;
        }
        return fallback;
    }
}
