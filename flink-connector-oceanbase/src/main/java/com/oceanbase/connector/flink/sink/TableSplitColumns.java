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

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Schema helpers for the table-name split routing column. */
public final class TableSplitColumns {

    private TableSplitColumns() {}

    public static ResolvedSchema stripFromSchema(ResolvedSchema physicalFull, String splitColumn) {
        List<Column> kept = new ArrayList<>();
        for (Column column : physicalFull.getColumns()) {
            if (column.isPhysical() && !column.getName().equals(splitColumn)) {
                kept.add(column);
            }
        }
        UniqueConstraint pk =
                physicalFull
                        .getPrimaryKey()
                        .map(
                                uc -> {
                                    List<String> pkCols =
                                            uc.getColumns().stream()
                                                    .filter(name -> !name.equals(splitColumn))
                                                    .collect(Collectors.toList());
                                    if (pkCols.isEmpty()) {
                                        throw new IllegalArgumentException(
                                                "Primary key cannot consist only of table-name split column ("
                                                        + splitColumn
                                                        + ").");
                                    }
                                    return UniqueConstraint.primaryKey(uc.getName(), pkCols);
                                })
                        .orElse(null);
        return new ResolvedSchema(kept, physicalFull.getWatermarkSpecs(), pk);
    }

    public static int resolvePhysicalColumnIndex(ResolvedSchema physicalFull, String splitColumn) {
        List<Column> columns = physicalFull.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            if (column.isPhysical() && column.getName().equals(splitColumn)) {
                return i;
            }
        }
        throw new IllegalArgumentException(
                "Physical column '"
                        + splitColumn
                        + "' not found in sink schema. Available: "
                        + physicalFull.getColumnNames());
    }

    public static void assertColumnValid(ResolvedSchema physicalFull, String splitColumn) {
        int idx = resolvePhysicalColumnIndex(physicalFull, splitColumn);
        LogicalTypeRoot actual =
                physicalFull.getColumnDataTypes().get(idx).getLogicalType().getTypeRoot();
        if (actual != LogicalTypeRoot.VARCHAR && actual != LogicalTypeRoot.CHAR) {
            throw new IllegalArgumentException(
                    "Column '"
                            + splitColumn
                            + "' must be CHAR or VARCHAR for table-name split, but was "
                            + actual
                            + ".");
        }
    }
}
