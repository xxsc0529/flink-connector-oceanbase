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

import com.oceanbase.connector.flink.TableNameSplitAffix;

/** Resolves a physical OceanBase table name from a split column value. */
public final class TableNameSplitResolver {

    private TableNameSplitResolver() {}

    /**
     * When {@code splitValue} is null or blank after trim, returns {@code baseTableName}. Otherwise
     * joins base and value per {@code affix} and {@code concat}.
     */
    public static String resolvePhysicalTableName(
            String splitValue, String baseTableName, TableNameSplitAffix affix, String concat) {
        if (splitValue == null || splitValue.trim().isEmpty()) {
            return baseTableName;
        }
        String v = splitValue.trim();
        return affix == TableNameSplitAffix.SUFFIX
                ? baseTableName + concat + v
                : v + concat + baseTableName;
    }
}
