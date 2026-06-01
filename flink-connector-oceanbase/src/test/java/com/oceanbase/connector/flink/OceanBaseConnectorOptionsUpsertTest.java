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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class OceanBaseConnectorOptionsUpsertTest {

    @Test
    void rejectsBothUpsertOptions() {
        Map<String, String> config = new HashMap<>();
        config.put("url", "jdbc:mysql://localhost:2881/test");
        config.put("username", "u");
        config.put("password", "p");
        config.put("schema-name", "test");
        config.put("table-name", "t");
        config.put(OceanBaseConnectorOptions.SINK_UPSERT_VERSION_COLUMN.key(), "version");
        config.put(
                OceanBaseConnectorOptions.SINK_DUPLICATE_KEY_UPDATE_CLAUSE.key(),
                "`data`=VALUES(`data`)");

        OceanBaseConnectorOptions options = new OceanBaseConnectorOptions(config);
        Assertions.assertThrows(IllegalArgumentException.class, options::validateUpsertOptions);
    }
}
