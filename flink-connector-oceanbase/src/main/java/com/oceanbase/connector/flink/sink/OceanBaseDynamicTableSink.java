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

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import com.oceanbase.connector.flink.connection.OceanBaseConnectionProvider;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.OceanBaseRowDataSerializationSchema;
import com.oceanbase.connector.flink.table.RecordSerializationSchema;
import com.oceanbase.connector.flink.table.TableId;
import com.oceanbase.connector.flink.table.TableInfo;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;

public class OceanBaseDynamicTableSink extends AbstractDynamicTableSink {

    private final OceanBaseConnectorOptions connectorOptions;

    public OceanBaseDynamicTableSink(
            ResolvedSchema physicalSchema, OceanBaseConnectorOptions connectorOptions) {
        super(physicalSchema);
        this.connectorOptions = connectorOptions;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        OceanBaseConnectionProvider connectionProvider =
                new OceanBaseConnectionProvider(connectorOptions);
        TableId tableId =
                new TableId(
                        connectionProvider.getDialect()::getFullTableName,
                        connectorOptions.getSchemaName(),
                        connectorOptions.getTableName());
        OceanBaseRecordFlusher recordFlusher =
                new OceanBaseRecordFlusher(connectorOptions, connectionProvider);

        final RecordSerializationSchema<RowData> serializer;
        final FileCompletionNotifier notifier;
        if (connectorOptions.isTableNameSplitEnabled()) {
            serializer = buildTableSplitSerializer(tableId);
            notifier = buildFileCompletionNotifier();
        } else if (connectorOptions.isFileCompletionKafkaEnabled()) {
            serializer = buildFileCompletionSerializer(tableId);
            notifier = new KafkaFileCompletionNotifier(connectorOptions);
        } else if (connectorOptions.hasFileCompletionColumnMapping()) {
            serializer = buildStrippedObSerializer(tableId);
            notifier = FileCompletionNotifier.noop();
        } else {
            serializer =
                    new OceanBaseRowDataSerializationSchema(new TableInfo(tableId, physicalSchema));
            notifier = FileCompletionNotifier.noop();
        }

        return new SinkProvider(
                typeSerializer ->
                        new OceanBaseSink<>(
                                connectorOptions,
                                typeSerializer,
                                serializer,
                                DataChangeRecord.KeyExtractor.simple(),
                                recordFlusher,
                                notifier),
                connectorOptions.getSinkParallelism());
    }

    private RecordSerializationSchema<RowData> buildTableSplitSerializer(TableId tableId) {
        String splitColumn = connectorOptions.getTableNameSplitColumn();
        ResolvedSchema obPhysical = TableSplitColumns.stripFromSchema(physicalSchema, splitColumn);
        if (connectorOptions.hasFileCompletionColumnMapping()) {
            obPhysical =
                    FileCompletionColumns.stripFromSchema(
                            obPhysical,
                            connectorOptions.getFileCompletionFlagColumn(),
                            connectorOptions.getFileCompletionMessageColumn());
        }
        if (connectorOptions.isFileCompletionKafkaEnabled()) {
            return new OceanBaseTableSplitRowDataSerializationSchema(
                    tableId,
                    physicalSchema,
                    splitColumn,
                    connectorOptions.getTableNameSplitAffix(),
                    connectorOptions.getTableNameSplitConcat(),
                    obPhysical,
                    connectorOptions.getFileCompletionFlagColumn(),
                    connectorOptions.getFileCompletionMessageColumn());
        }
        return new OceanBaseTableSplitRowDataSerializationSchema(
                tableId,
                physicalSchema,
                splitColumn,
                connectorOptions.getTableNameSplitAffix(),
                connectorOptions.getTableNameSplitConcat(),
                obPhysical);
    }

    private RecordSerializationSchema<RowData> buildStrippedObSerializer(TableId tableId) {
        String flagColumn = connectorOptions.getFileCompletionFlagColumn();
        String messageColumn = connectorOptions.getFileCompletionMessageColumn();
        ResolvedSchema obPhysical =
                FileCompletionColumns.stripFromSchema(physicalSchema, flagColumn, messageColumn);
        return new OceanBaseRowDataSerializationSchema(new TableInfo(tableId, obPhysical));
    }

    private RecordSerializationSchema<RowData> buildFileCompletionSerializer(TableId tableId) {
        String flagColumn = connectorOptions.getFileCompletionFlagColumn();
        String messageColumn = connectorOptions.getFileCompletionMessageColumn();
        ResolvedSchema obPhysical =
                FileCompletionColumns.stripFromSchema(physicalSchema, flagColumn, messageColumn);
        return new OceanBaseFileCompletionRowDataSerializationSchema(
                new TableInfo(tableId, obPhysical), physicalSchema, flagColumn, messageColumn);
    }

    private FileCompletionNotifier buildFileCompletionNotifier() {
        if (connectorOptions.isFileCompletionKafkaEnabled()) {
            return new KafkaFileCompletionNotifier(connectorOptions);
        }
        return FileCompletionNotifier.noop();
    }

    @Override
    public DynamicTableSink copy() {
        return new OceanBaseDynamicTableSink(physicalSchema, connectorOptions);
    }

    @Override
    public String asSummaryString() {
        return "OceanBase";
    }
}
