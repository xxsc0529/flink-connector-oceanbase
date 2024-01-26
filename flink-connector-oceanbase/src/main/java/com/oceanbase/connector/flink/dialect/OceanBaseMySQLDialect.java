/*
 * Copyright (c) 2023 OceanBase.
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

package com.oceanbase.connector.flink.dialect;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.stream.Collectors;

public class OceanBaseMySQLDialect implements OceanBaseDialect {

    private static final long serialVersionUID = 1L;

    @Override
    public String quoteIdentifier(@Nonnull String identifier) {
        return "`" + identifier.replaceAll("`", "``") + "`";
    }

    @Override
    public String getUpsertStatement(
            @Nonnull String schemaName,
            @Nonnull String tableName,
            @Nonnull List<String> fieldNames,
            @Nonnull List<String> uniqueKeyFields) {
        String updateClause =
                fieldNames.stream()
                        .filter(f -> !uniqueKeyFields.contains(f))
                        .map(f -> quoteIdentifier(f) + "=VALUES(" + quoteIdentifier(f) + ")")
                        .collect(Collectors.joining(", "));
        return getInsertIntoStatement(schemaName, tableName, fieldNames)
                + " ON DUPLICATE KEY UPDATE "
                + updateClause;
    }

    @Override
    public String getSysDatabase() {
        return "oceanbase";
    }

    @Override
    public String getSelectOBVersionStatement() {
        return "SELECT OB_VERSION()";
    }
}
