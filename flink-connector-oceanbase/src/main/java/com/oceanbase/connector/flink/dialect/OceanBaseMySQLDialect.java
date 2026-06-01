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

package com.oceanbase.connector.flink.dialect;

import org.apache.flink.util.function.SerializableFunction;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;

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
            @Nonnull List<String> uniqueKeyFields,
            @Nullable SerializableFunction<String, String> placeholderFunc) {
        return getUpsertStatement(
                schemaName, tableName, fieldNames, uniqueKeyFields, placeholderFunc, null);
    }

    @Override
    public String getUpsertStatement(
            @Nonnull String schemaName,
            @Nonnull String tableName,
            @Nonnull List<String> fieldNames,
            @Nonnull List<String> uniqueKeyFields,
            @Nullable SerializableFunction<String, String> placeholderFunc,
            @Nullable UpsertOptions upsertOptions) {
        String updateClause =
                MySQLDuplicateKeyUpdateClauseBuilder.resolve(
                        fieldNames, uniqueKeyFields, this::quoteIdentifier, upsertOptions);
        String insertIntoStatement =
                getInsertIntoStatement(schemaName, tableName, fieldNames, placeholderFunc);
        if (!MySQLDuplicateKeyUpdateClauseBuilder.isEmpty(updateClause)) {
            return insertIntoStatement + " ON DUPLICATE KEY UPDATE " + updateClause;
        } else {
            return StringUtils.replace(insertIntoStatement, "INSERT", "INSERT IGNORE", 1);
        }
    }

    @Override
    public String getSysDatabase() {
        return "oceanbase";
    }

    @Override
    public String getQueryTenantNameStatement() {
        return "SHOW TENANT";
    }

    @Override
    public String getListSchemaStatement() {
        return "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA`";
    }

    @Override
    public String getListTableStatement(String schemaName) {
        return "SELECT TABLE_NAME FROM information_schema.`TABLES` WHERE TABLE_SCHEMA = '"
                + schemaName
                + "'";
    }
}
