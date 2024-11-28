/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.paimon.sink;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventDispatcher;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalog;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonHadoopConfiguration;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.paimon.data.PaimonTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorException;
import org.apache.seatunnel.connectors.seatunnel.paimon.security.PaimonSecurityContext;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.bucket.PaimonBucketAssigner;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.commit.PaimonCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.state.PaimonSinkState;
import org.apache.seatunnel.connectors.seatunnel.paimon.utils.JobContextUtil;
import org.apache.seatunnel.connectors.seatunnel.paimon.utils.RowConverter;

import org.apache.commons.lang3.StringUtils;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableCommit;
import org.apache.paimon.table.sink.TableWrite;
import org.apache.paimon.table.sink.WriteBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.Preconditions;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.disk.IOManagerImpl.splitPaths;
import static org.apache.seatunnel.connectors.seatunnel.paimon.sink.schema.UpdatedDataFields.canConvert;

@Slf4j
public class PaimonSinkWriter
        implements SinkWriter<SeaTunnelRow, PaimonCommitInfo, PaimonSinkState>,
                SupportMultiTableSinkWriter<Void>,
                SupportSchemaEvolutionSinkWriter {

    private String commitUser = UUID.randomUUID().toString();

    private FileStoreTable paimonFileStoretable;

    private WriteBuilder tableWriteBuilder;

    private TableWrite tableWrite;

    private final List<CommitMessage> committables = new ArrayList<>();

    private SeaTunnelRowType seaTunnelRowType;

    private final SinkWriter.Context context;

    private final JobContext jobContext;

    private org.apache.seatunnel.api.table.catalog.TableSchema sourceTableSchema;

    private TableSchema sinkPaimonTableSchema;

    private PaimonBucketAssigner bucketAssigner;

    private final boolean dynamicBucket;

    private final PaimonCatalog paimonCatalog;

    private final TablePath paimonTablePath;

    private final PaimonSinkConfig paimonSinkConfig;

    private final TableSchemaChangeEventDispatcher TABLE_SCHEMACHANGER =
            new TableSchemaChangeEventDispatcher();

    public PaimonSinkWriter(
            Context context,
            ReadonlyConfig readonlyConfig,
            CatalogTable catalogTable,
            Table paimonFileStoretable,
            JobContext jobContext,
            PaimonSinkConfig paimonSinkConfig,
            PaimonHadoopConfiguration paimonHadoopConfiguration) {
        this.sourceTableSchema = catalogTable.getTableSchema();
        this.seaTunnelRowType = this.sourceTableSchema.toPhysicalRowDataType();
        this.paimonTablePath = catalogTable.getTablePath();
        this.paimonCatalog = PaimonCatalog.loadPaimonCatalog(readonlyConfig);
        this.paimonCatalog.open();
        this.paimonFileStoretable = (FileStoreTable) paimonFileStoretable;
        CoreOptions.ChangelogProducer changelogProducer =
                this.paimonFileStoretable.coreOptions().changelogProducer();
        if (Objects.nonNull(paimonSinkConfig.getChangelogProducer())
                && changelogProducer != paimonSinkConfig.getChangelogProducer()) {
            log.warn(
                    "configured the props named 'changelog-producer' which is not compatible with the options in table , so it will use the table's 'changelog-producer'");
        }
        this.paimonSinkConfig = paimonSinkConfig;
        this.sinkPaimonTableSchema = this.paimonFileStoretable.schema();
        this.context = context;
        this.jobContext = jobContext;
        this.newTableWrite();
        BucketMode bucketMode = this.paimonFileStoretable.bucketMode();
        this.dynamicBucket =
                BucketMode.DYNAMIC == bucketMode || BucketMode.GLOBAL_DYNAMIC == bucketMode;
        int bucket = ((FileStoreTable) paimonFileStoretable).coreOptions().bucket();
        if (bucket == -1 && BucketMode.UNAWARE == bucketMode) {
            log.warn("Append only table currently do not support dynamic bucket");
        }
        if (dynamicBucket) {
            this.bucketAssigner =
                    new PaimonBucketAssigner(
                            paimonFileStoretable,
                            this.context.getNumberOfParallelSubtasks(),
                            this.context.getIndexOfSubtask());
        }
        PaimonSecurityContext.shouldEnableKerberos(paimonHadoopConfiguration);
    }

    public PaimonSinkWriter(
            Context context,
            ReadonlyConfig readonlyConfig,
            CatalogTable catalogTable,
            Table paimonFileStoretable,
            List<PaimonSinkState> states,
            JobContext jobContext,
            PaimonSinkConfig paimonSinkConfig,
            PaimonHadoopConfiguration paimonHadoopConfiguration) {
        this(
                context,
                readonlyConfig,
                catalogTable,
                paimonFileStoretable,
                jobContext,
                paimonSinkConfig,
                paimonHadoopConfiguration);
        if (Objects.isNull(states) || states.isEmpty()) {
            return;
        }
        this.commitUser = states.get(0).getCommitUser();
        long checkpointId = states.get(0).getCheckpointId();
        try (TableCommit tableCommit = tableWriteBuilder.newCommit()) {
            List<CommitMessage> commitables =
                    states.stream()
                            .map(PaimonSinkState::getCommittables)
                            .flatMap(List::stream)
                            .collect(Collectors.toList());
            log.info("Trying to recommit states {}", commitables);
            if (JobContextUtil.isBatchJob(jobContext)) {
                log.debug("Trying to recommit states batch mode");
                ((BatchTableCommit) tableCommit).commit(commitables);
            } else {
                log.debug("Trying to recommit states streaming mode");
                ((StreamTableCommit) tableCommit).commit(checkpointId, commitables);
            }
        } catch (Exception e) {
            throw new PaimonConnectorException(
                    PaimonConnectorErrorCode.TABLE_WRITE_COMMIT_FAILED, e);
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        InternalRow rowData =
                RowConverter.reconvert(element, seaTunnelRowType, sinkPaimonTableSchema);
        try {
            PaimonSecurityContext.runSecured(
                    () -> {
                        if (dynamicBucket) {
                            int bucket = bucketAssigner.assign(rowData);
                            tableWrite.write(rowData, bucket);
                        } else {
                            tableWrite.write(rowData);
                        }
                        return null;
                    });
        } catch (Exception e) {
            throw new PaimonConnectorException(
                    PaimonConnectorErrorCode.TABLE_WRITE_RECORD_FAILED,
                    "This record " + element + " failed to be written",
                    e);
        }
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) throws IOException {
        if (event instanceof AlterTableColumnsEvent) {
            for (AlterTableColumnEvent columnEvent : ((AlterTableColumnsEvent) event).getEvents()) {
                applySingleSchemaChangeEvent(columnEvent);
            }
        } else if (event instanceof AlterTableColumnEvent) {
            applySingleSchemaChangeEvent(event);
        } else {
            throw new UnsupportedOperationException("Unsupported alter table event: " + event);
        }
        reOpenTableWrite(event);
    }

    private void reOpenTableWrite(SchemaChangeEvent event) {
        this.sourceTableSchema = TABLE_SCHEMACHANGER.reset(sourceTableSchema).apply(event);
        this.seaTunnelRowType = this.sourceTableSchema.toPhysicalRowDataType();
        this.paimonFileStoretable = (FileStoreTable) paimonCatalog.getPaimonTable(paimonTablePath);
        this.sinkPaimonTableSchema = this.paimonFileStoretable.schema();
        this.newTableWrite();
    }

    private void newTableWrite() {
        this.tableWriteBuilder =
                JobContextUtil.isBatchJob(jobContext)
                        ? this.paimonFileStoretable.newBatchWriteBuilder()
                        : this.paimonFileStoretable.newStreamWriteBuilder();
        TableWrite oldTableWrite = this.tableWrite;
        this.tableWrite =
                tableWriteBuilder
                        .newWrite()
                        .withIOManager(
                                IOManager.create(
                                        splitPaths(paimonSinkConfig.getChangelogTmpPath())));
        tableWriteClose(oldTableWrite);
    }

    private void applySingleSchemaChangeEvent(SchemaChangeEvent event) {
        Identifier identifier =
                Identifier.create(
                        paimonTablePath.getDatabaseName(), paimonTablePath.getTableName());
        if (event instanceof AlterTableAddColumnEvent) {
            AlterTableAddColumnEvent alterTableAddColumnEvent = (AlterTableAddColumnEvent) event;
            Column column = alterTableAddColumnEvent.getColumn();
            String afterColumnName = alterTableAddColumnEvent.getAfterColumn();
            SchemaChange.Move move =
                    StringUtils.isBlank(afterColumnName)
                            ? null
                            : SchemaChange.Move.after(column.getName(), afterColumnName);
            BasicTypeDefine<DataType> reconvertColumn = PaimonTypeMapper.INSTANCE.reconvert(column);
            SchemaChange schemaChange =
                    SchemaChange.addColumn(
                            column.getName(),
                            reconvertColumn.getNativeType(),
                            column.getComment(),
                            move);
            paimonCatalog.alterTable(identifier, schemaChange, false);
        } else if (event instanceof AlterTableDropColumnEvent) {
            String columnName = ((AlterTableDropColumnEvent) event).getColumn();
            paimonCatalog.alterTable(identifier, SchemaChange.dropColumn(columnName), true);
        } else if (event instanceof AlterTableModifyColumnEvent) {
            Column column = ((AlterTableModifyColumnEvent) event).getColumn();
            String afterColumn = ((AlterTableModifyColumnEvent) event).getAfterColumn();
            updateColumn(column, column.getName(), identifier, afterColumn);
        } else if (event instanceof AlterTableChangeColumnEvent) {
            Column column = ((AlterTableChangeColumnEvent) event).getColumn();
            String afterColumn = ((AlterTableChangeColumnEvent) event).getAfterColumn();
            String oldColumn = ((AlterTableChangeColumnEvent) event).getOldColumn();
            updateColumn(column, oldColumn, identifier, afterColumn);
            if (!column.getName().equals(oldColumn)) {
                paimonCatalog.alterTable(
                        identifier, SchemaChange.renameColumn(oldColumn, column.getName()), false);
            }
        } else {
            throw new UnsupportedOperationException("Unsupported alter table event: " + event);
        }
    }

    private void updateColumn(
            Column newColumn, String oldColumnName, Identifier identifier, String afterTheColumn) {
        BasicTypeDefine<DataType> reconvertColumn = PaimonTypeMapper.INSTANCE.reconvert(newColumn);
        int idx = sinkPaimonTableSchema.fieldNames().indexOf(oldColumnName);
        Preconditions.checkState(
                idx >= 0,
                "Field name " + oldColumnName + " does not exist in table. This is unexpected.");
        DataType newDataType = reconvertColumn.getNativeType();
        DataField dataField = sinkPaimonTableSchema.fields().get(idx);
        DataType oldDataType = dataField.type();
        switch (canConvert(oldDataType, newDataType)) {
            case CONVERT:
                paimonCatalog.alterTable(
                        identifier,
                        SchemaChange.updateColumnType(oldColumnName, newDataType),
                        false);
                break;
            case IGNORE:
                log.warn(
                        "old: {{}-{}} and new: {{}-{}} belongs to the same type family, but old type has higher precision than new type. Ignore this convert request.",
                        dataField.name(),
                        oldDataType,
                        reconvertColumn.getName(),
                        newDataType);
                break;
            case EXCEPTION:
                throw new UnsupportedOperationException(
                        String.format(
                                "Cannot convert field %s from type %s to %s of Paimon table %s.",
                                oldColumnName, oldDataType, newDataType, identifier.getFullName()));
        }
        if (StringUtils.isNotBlank(afterTheColumn)) {
            paimonCatalog.alterTable(
                    identifier,
                    SchemaChange.updateColumnPosition(
                            SchemaChange.Move.after(oldColumnName, afterTheColumn)),
                    false);
        }
        String comment = newColumn.getComment();
        if (StringUtils.isNotBlank(comment)) {
            paimonCatalog.alterTable(
                    identifier, SchemaChange.updateColumnComment(oldColumnName, comment), false);
        }
        paimonCatalog.alterTable(
                identifier,
                SchemaChange.updateColumnNullability(oldColumnName, newColumn.isNullable()),
                false);
    }

    @Override
    public Optional<PaimonCommitInfo> prepareCommit() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<PaimonCommitInfo> prepareCommit(long checkpointId) throws IOException {
        try {
            List<CommitMessage> fileCommittables;
            if (JobContextUtil.isBatchJob(jobContext)) {
                fileCommittables = ((BatchTableWrite) tableWrite).prepareCommit();
            } else {
                fileCommittables =
                        ((StreamTableWrite) tableWrite)
                                .prepareCommit(waitCompaction(), checkpointId);
            }
            committables.addAll(fileCommittables);
            return Optional.of(new PaimonCommitInfo(fileCommittables, checkpointId));
        } catch (Exception e) {
            throw new PaimonConnectorException(
                    PaimonConnectorErrorCode.TABLE_PRE_COMMIT_FAILED,
                    "Paimon pre-commit failed.",
                    e);
        }
    }

    @Override
    public List<PaimonSinkState> snapshotState(long checkpointId) throws IOException {
        PaimonSinkState paimonSinkState =
                new PaimonSinkState(new ArrayList<>(committables), commitUser, checkpointId);
        committables.clear();
        return Collections.singletonList(paimonSinkState);
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        try {
            tableWriteClose(this.tableWrite);
        } finally {
            committables.clear();
            if (Objects.nonNull(paimonCatalog)) {
                paimonCatalog.close();
            }
        }
    }

    private void tableWriteClose(TableWrite tableWrite) {
        if (Objects.nonNull(tableWrite)) {
            try {
                tableWrite.close();
            } catch (Exception e) {
                log.error("Failed to close table writer in paimon sink writer.", e);
                throw new SeaTunnelException(e);
            }
        }
    }

    private boolean waitCompaction() {
        CoreOptions.ChangelogProducer changelogProducer =
                this.paimonFileStoretable.coreOptions().changelogProducer();
        return changelogProducer == CoreOptions.ChangelogProducer.LOOKUP
                || changelogProducer == CoreOptions.ChangelogProducer.FULL_COMPACTION;
    }
}
