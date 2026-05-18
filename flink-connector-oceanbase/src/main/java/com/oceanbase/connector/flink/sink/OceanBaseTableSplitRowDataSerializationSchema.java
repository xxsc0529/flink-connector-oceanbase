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
import com.oceanbase.connector.flink.table.OceanBaseRowDataSerializationSchema;
import com.oceanbase.connector.flink.table.Record;
import com.oceanbase.connector.flink.table.RecordSerializationSchema;
import com.oceanbase.connector.flink.table.TableId;
import com.oceanbase.connector.flink.table.TableInfo;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Routes each row to a physical table derived from {@code table-name.split-column}. Blank split
 * values write to the configured {@code table-name}.
 */
public class OceanBaseTableSplitRowDataSerializationSchema
        implements RecordSerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final TableId baseTableId;
    private final TableInfo obTableInfoTemplate;
    private final int splitPos;
    private final TableNameSplitAffix affix;
    private final String concat;
    private final RowData.FieldGetter splitGetter;
    private final boolean fileCompletionEnabled;
    private final int flagPos;
    private final int messagePos;
    private final RowData.FieldGetter[] fileCompletionObFieldGetters;
    private transient Map<String, RecordSerializationSchema<RowData>> innerByTable;

    public OceanBaseTableSplitRowDataSerializationSchema(
            TableId baseTableId,
            ResolvedSchema physicalFull,
            String splitColumn,
            TableNameSplitAffix affix,
            String concat,
            ResolvedSchema obPhysical) {
        this(baseTableId, physicalFull, splitColumn, affix, concat, obPhysical, false, null, null);
    }

    public OceanBaseTableSplitRowDataSerializationSchema(
            TableId baseTableId,
            ResolvedSchema physicalFull,
            String splitColumn,
            TableNameSplitAffix affix,
            String concat,
            ResolvedSchema obPhysical,
            String flagColumn,
            String messageColumn) {
        this(
                baseTableId,
                physicalFull,
                splitColumn,
                affix,
                concat,
                obPhysical,
                true,
                flagColumn,
                messageColumn);
    }

    private OceanBaseTableSplitRowDataSerializationSchema(
            TableId baseTableId,
            ResolvedSchema physicalFull,
            String splitColumn,
            TableNameSplitAffix affix,
            String concat,
            ResolvedSchema obPhysical,
            boolean fileCompletionEnabled,
            String flagColumn,
            String messageColumn) {
        this.baseTableId = baseTableId;
        this.obTableInfoTemplate = new TableInfo(baseTableId, obPhysical);
        this.splitPos = TableSplitColumns.resolvePhysicalColumnIndex(physicalFull, splitColumn);
        this.affix = affix;
        this.concat = concat;
        LogicalType splitType = physicalFull.getColumnDataTypes().get(splitPos).getLogicalType();
        this.splitGetter = RowData.createFieldGetter(splitType, splitPos);
        this.fileCompletionEnabled = fileCompletionEnabled;
        if (fileCompletionEnabled) {
            this.flagPos =
                    FileCompletionColumns.resolvePhysicalColumnIndex(physicalFull, flagColumn);
            this.messagePos =
                    FileCompletionColumns.resolvePhysicalColumnIndex(physicalFull, messageColumn);
            this.fileCompletionObFieldGetters =
                    OceanBaseFileCompletionRowDataSerializationSchema.buildObFieldGetters(
                            obTableInfoTemplate,
                            physicalFull,
                            flagColumn,
                            messageColumn,
                            splitColumn);
        } else {
            this.flagPos = -1;
            this.messagePos = -1;
            this.fileCompletionObFieldGetters = null;
        }
    }

    @Override
    public Record serialize(RowData rowData) {
        String splitValue = readSplitValue(rowData);
        String targetTable =
                TableNameSplitResolver.resolvePhysicalTableName(
                        splitValue, baseTableId.getTableName(), affix, concat);
        RecordSerializationSchema<RowData> inner =
                innerByTable().computeIfAbsent(targetTable, this::createInnerSerializer);
        return inner.serialize(rowData);
    }

    private Map<String, RecordSerializationSchema<RowData>> innerByTable() {
        if (innerByTable == null) {
            innerByTable = new ConcurrentHashMap<>();
        }
        return innerByTable;
    }

    private RecordSerializationSchema<RowData> createInnerSerializer(String targetTable) {
        TableInfo tableInfo = tableInfoFor(targetTable);
        if (fileCompletionEnabled) {
            return new OceanBaseFileCompletionRowDataSerializationSchema(
                    tableInfo, flagPos, messagePos, fileCompletionObFieldGetters);
        }
        return new OceanBaseRowDataSerializationSchema(tableInfo);
    }

    private TableInfo tableInfoFor(String targetTable) {
        return new TableInfo(
                baseTableId.withTableName(targetTable),
                obTableInfoTemplate.getKey(),
                obTableInfoTemplate.getFieldNames(),
                obTableInfoTemplate.getDataTypes(),
                obTableInfoTemplate.getPlaceholderFunc());
    }

    private String readSplitValue(RowData rowData) {
        if (rowData.isNullAt(splitPos)) {
            return null;
        }
        Object field = splitGetter.getFieldOrNull(rowData);
        return field == null ? null : field.toString();
    }
}
