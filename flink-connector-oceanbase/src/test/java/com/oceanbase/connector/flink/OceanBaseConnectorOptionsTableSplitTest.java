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

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OceanBaseConnectorOptionsTableSplitTest {

    private static Map<String, String> base() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("url", "jdbc:mysql://127.0.0.1:1/test");
        m.put("username", "u");
        m.put("password", "p");
        m.put("schema-name", "test");
        m.put("table-name", "user_info");
        return m;
    }

    @Test
    void splitDisabledWhenColumnUnset() {
        OceanBaseConnectorOptions opts = new OceanBaseConnectorOptions(base());
        assertFalse(opts.isTableNameSplitEnabled());
        assertDoesNotThrow(opts::validateTableNameSplitOptions);
    }

    @Test
    void validateFailsWhenSplitColumnSetWithoutAffix() {
        Map<String, String> m = base();
        m.put("table-name.split-column", "split_column");
        OceanBaseConnectorOptions opts = new OceanBaseConnectorOptions(m);
        assertTrue(opts.isTableNameSplitEnabled());
        assertThrows(IllegalArgumentException.class, opts::validateTableNameSplitOptions);
    }

    @Test
    void validateFailsWhenAffixSetWithoutSplitColumn() {
        Map<String, String> m = base();
        m.put("table-name.split-affix", "suffix");
        OceanBaseConnectorOptions opts = new OceanBaseConnectorOptions(m);
        assertThrows(IllegalArgumentException.class, opts::validateTableNameSplitOptions);
    }

    @Test
    void validatePassesWhenSplitOptionsComplete() {
        Map<String, String> m = base();
        m.put("table-name.split-column", "split_column");
        m.put("table-name.split-affix", "suffix");
        OceanBaseConnectorOptions opts = new OceanBaseConnectorOptions(m);
        assertDoesNotThrow(opts::validateTableNameSplitOptions);
        assertEquals(TableNameSplitAffix.SUFFIX, opts.getTableNameSplitAffix());
        assertEquals("_", opts.getTableNameSplitConcat());
    }

    @Test
    void validateFailsWhenSplitAndPartitionEnabled() {
        Map<String, String> m = base();
        m.put("table-name.split-column", "split_column");
        m.put("table-name.split-affix", "suffix");
        m.put("partition.enabled", "true");
        OceanBaseConnectorOptions opts = new OceanBaseConnectorOptions(m);
        IllegalArgumentException ex =
                assertThrows(IllegalArgumentException.class, opts::validateTableNameSplitOptions);
        assertTrue(ex.getMessage().contains("partition.enabled"));
    }

    @Test
    void invalidAffixThrows() {
        Map<String, String> m = base();
        m.put("table-name.split-column", "split_column");
        m.put("table-name.split-affix", "middle");
        OceanBaseConnectorOptions opts = new OceanBaseConnectorOptions(m);
        assertThrows(IllegalArgumentException.class, opts::getTableNameSplitAffix);
    }
}
