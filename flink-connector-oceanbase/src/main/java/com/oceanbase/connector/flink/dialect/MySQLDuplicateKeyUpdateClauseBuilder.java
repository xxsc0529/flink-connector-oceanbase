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

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Builds {@code ON DUPLICATE KEY UPDATE} assignment lists for MySQL-compatible mode. */
final class MySQLDuplicateKeyUpdateClauseBuilder {

    private MySQLDuplicateKeyUpdateClauseBuilder() {}

    static String resolve(
            @Nonnull List<String> fieldNames,
            @Nonnull List<String> uniqueKeyFields,
            @Nonnull Function<String, String> quoteIdentifier,
            @Nullable UpsertOptions upsertOptions) {
        if (upsertOptions == null || !upsertOptions.isConfigured()) {
            return buildDefault(fieldNames, uniqueKeyFields, quoteIdentifier);
        }
        if (upsertOptions.getDuplicateKeyUpdateClause() != null) {
            return upsertOptions.getDuplicateKeyUpdateClause();
        }
        return buildVersionCompare(
                fieldNames,
                uniqueKeyFields,
                quoteIdentifier,
                upsertOptions.getVersionCompareColumn());
    }

    static String buildDefault(
            @Nonnull List<String> fieldNames,
            @Nonnull List<String> uniqueKeyFields,
            @Nonnull Function<String, String> quoteIdentifier) {
        return buildAssignments(fieldNames, uniqueKeyFields, quoteIdentifier, null);
    }

    /**
     * Generates conditional updates such as {@code col=IF(VALUES(ver)>ver, VALUES(col), col)} for
     * every non-key column.
     */
    static String buildVersionCompare(
            @Nonnull List<String> fieldNames,
            @Nonnull List<String> uniqueKeyFields,
            @Nonnull Function<String, String> quoteIdentifier,
            @Nonnull String versionColumn) {
        if (!fieldNames.contains(versionColumn)) {
            throw new IllegalArgumentException(
                    "Version compare column '"
                            + versionColumn
                            + "' is not among sink columns: "
                            + fieldNames);
        }
        String quotedVersion = quoteIdentifier.apply(versionColumn);
        String compareExpr = "VALUES(" + quotedVersion + ")>" + quotedVersion;
        return buildAssignments(fieldNames, uniqueKeyFields, quoteIdentifier, compareExpr);
    }

    private static String buildAssignments(
            @Nonnull List<String> fieldNames,
            @Nonnull List<String> uniqueKeyFields,
            @Nonnull Function<String, String> quoteIdentifier,
            @Nullable String compareExpr) {
        Set<String> uniqueKeys = new HashSet<>(uniqueKeyFields);
        return fieldNames.stream()
                .filter(f -> !uniqueKeys.contains(f))
                .map(
                        f -> {
                            String quoted = quoteIdentifier.apply(f);
                            if (compareExpr == null) {
                                return quoted + "=VALUES(" + quoted + ")";
                            }
                            return quoted
                                    + "=IF("
                                    + compareExpr
                                    + ",VALUES("
                                    + quoted
                                    + "),"
                                    + quoted
                                    + ")";
                        })
                .collect(Collectors.joining(", "));
    }

    static boolean isEmpty(@Nullable String updateClause) {
        return StringUtils.isEmpty(updateClause);
    }
}
