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

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;

import javax.annotation.Nullable;

import java.io.Serializable;

/** Controls how MySQL-mode {@code ON DUPLICATE KEY UPDATE} clauses are generated. */
public class UpsertOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final UpsertOptions NONE = new UpsertOptions(null, null);

    @Nullable private final String versionCompareColumn;

    /** Full UPDATE clause (without the {@code ON DUPLICATE KEY UPDATE} prefix). */
    @Nullable private final String duplicateKeyUpdateClause;

    private UpsertOptions(
            @Nullable String versionCompareColumn, @Nullable String duplicateKeyUpdateClause) {
        this.versionCompareColumn = versionCompareColumn;
        this.duplicateKeyUpdateClause = duplicateKeyUpdateClause;
    }

    public static UpsertOptions none() {
        return NONE;
    }

    /** Builds version-compare upsert options for the given physical column name. */
    public static UpsertOptions withVersionColumn(String versionColumn) {
        return new UpsertOptions(versionColumn, null);
    }

    /**
     * Resolves upsert options from connector configuration. Custom {@code
     * sink.duplicate-key-update-clause} wins over {@code sink.upsert.version-column} when only one
     * should be set ({@link OceanBaseConnectorOptions#validateUpsertOptions()} rejects both).
     */
    public static UpsertOptions from(OceanBaseConnectorOptions options) {
        String custom = options.getSinkDuplicateKeyUpdateClause();
        if (custom != null) {
            return new UpsertOptions(null, custom);
        }
        String versionColumn = options.getSinkUpsertVersionColumn();
        if (versionColumn != null) {
            return withVersionColumn(versionColumn);
        }
        return NONE;
    }

    @Nullable
    public String getVersionCompareColumn() {
        return versionCompareColumn;
    }

    @Nullable
    public String getDuplicateKeyUpdateClause() {
        return duplicateKeyUpdateClause;
    }

    public boolean isConfigured() {
        return versionCompareColumn != null || duplicateKeyUpdateClause != null;
    }
}
