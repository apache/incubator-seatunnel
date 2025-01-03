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
package org.apache.seatunnel.transform.common;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventDispatcher;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventHandler;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelFlatMapTransform;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractCatalogSupportFlatMapTransform
        extends AbstractSeaTunnelTransform<SeaTunnelRow, List<SeaTunnelRow>>
        implements SeaTunnelFlatMapTransform<SeaTunnelRow> {

    private final ReadonlyConfig config;
    private final TableSchemaChangeEventHandler tableSchemaChangeEventHandler;

    public AbstractCatalogSupportFlatMapTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.config = config;
        this.tableSchemaChangeEventHandler = new TableSchemaChangeEventDispatcher();
    }

    public AbstractCatalogSupportFlatMapTransform(
            @NonNull CatalogTable inputCatalogTable,
            ErrorHandleWay rowErrorHandleWay,
            ReadonlyConfig config) {
        super(inputCatalogTable, rowErrorHandleWay);
        this.config = config;
        this.tableSchemaChangeEventHandler = new TableSchemaChangeEventDispatcher();
    }

    @Override
    public List<SeaTunnelRow> flatMap(SeaTunnelRow row) {
        return transform(row);
    }

    @Override
    public TableSchema transformTableSchema() {
        return convertTableSchema(inputCatalogTable.getTableSchema());
    }

    @Override
    public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent event) {
        TableSchema newTableSchema =
                tableSchemaChangeEventHandler
                        .reset(inputCatalogTable.getTableSchema())
                        .apply(event);
        this.inputCatalogTable =
                CatalogTable.of(
                        inputCatalogTable.getTableId(),
                        newTableSchema,
                        inputCatalogTable.getOptions(),
                        inputCatalogTable.getPartitionKeys(),
                        inputCatalogTable.getComment());

        if (event instanceof AlterTableColumnsEvent) {
            AlterTableColumnsEvent alterTableColumnsEvent = (AlterTableColumnsEvent) event;
            AlterTableColumnsEvent newEvent =
                    new AlterTableColumnsEvent(
                            event.tableIdentifier(),
                            alterTableColumnsEvent.getEvents().stream()
                                    .map(this::convertName)
                                    .collect(Collectors.toList()));

            newEvent.setJobId(event.getJobId());
            newEvent.setStatement(((AlterTableColumnsEvent) event).getStatement());
            newEvent.setSourceDialectName(((AlterTableColumnsEvent) event).getSourceDialectName());
            if (event.getChangeAfter() != null) {
                newEvent.setChangeAfter(
                        CatalogTable.of(
                                event.getChangeAfter().getTableId(), event.getChangeAfter()));
            }
            return newEvent;
        }
        if (event instanceof AlterTableColumnEvent) {
            return convertName((AlterTableColumnEvent) event);
        }
        return event;
    }

    @VisibleForTesting
    public AlterTableColumnEvent convertName(AlterTableColumnEvent event) {
        AlterTableColumnEvent newEvent = event;
        switch (event.getEventType()) {
            case SCHEMA_CHANGE_ADD_COLUMN:
                AlterTableAddColumnEvent addColumnEvent = (AlterTableAddColumnEvent) event;
                newEvent =
                        new AlterTableAddColumnEvent(
                                event.tableIdentifier(),
                                convertName(addColumnEvent.getColumn()),
                                addColumnEvent.isFirst(),
                                convertName(addColumnEvent.getAfterColumn()));
                break;
            case SCHEMA_CHANGE_DROP_COLUMN:
                AlterTableDropColumnEvent dropColumnEvent = (AlterTableDropColumnEvent) event;
                newEvent =
                        new AlterTableDropColumnEvent(
                                event.tableIdentifier(), convertName(dropColumnEvent.getColumn()));
                break;
            case SCHEMA_CHANGE_MODIFY_COLUMN:
                AlterTableModifyColumnEvent modifyColumnEvent = (AlterTableModifyColumnEvent) event;
                newEvent =
                        new AlterTableModifyColumnEvent(
                                event.tableIdentifier(),
                                convertName(modifyColumnEvent.getColumn()),
                                modifyColumnEvent.isFirst(),
                                convertName(modifyColumnEvent.getAfterColumn()));
                break;
            case SCHEMA_CHANGE_CHANGE_COLUMN:
                AlterTableChangeColumnEvent changeColumnEvent = (AlterTableChangeColumnEvent) event;
                boolean nameChanged =
                        !changeColumnEvent
                                .getOldColumn()
                                .equals(changeColumnEvent.getColumn().getName());
                if (nameChanged) {
                    log.warn(
                            "FieldRenameTransform does not support changing column name, "
                                    + "old column name: {}, new column name: {}",
                            changeColumnEvent.getOldColumn(),
                            changeColumnEvent.getColumn().getName());
                    return changeColumnEvent;
                }

                newEvent =
                        new AlterTableChangeColumnEvent(
                                event.tableIdentifier(),
                                convertName(changeColumnEvent.getOldColumn()),
                                convertName(changeColumnEvent.getColumn()),
                                changeColumnEvent.isFirst(),
                                convertName(changeColumnEvent.getAfterColumn()));
                break;
            default:
                log.warn("Unsupported event: {}", event);
                return event;
        }

        newEvent.setJobId(event.getJobId());
        newEvent.setStatement(event.getStatement());
        newEvent.setSourceDialectName(event.getSourceDialectName());
        if (event.getChangeAfter() != null) {
            CatalogTable newChangeAfter =
                    CatalogTable.of(
                            event.getChangeAfter().getTableId(),
                            convertTableSchema(event.getChangeAfter().getTableSchema()),
                            event.getChangeAfter().getOptions(),
                            event.getChangeAfter().getPartitionKeys(),
                            event.getChangeAfter().getComment());
            newEvent.setChangeAfter(newChangeAfter);
        }
        return newEvent;
    }

    @VisibleForTesting
    public String convertName(String name) {
        return name;
    }

    private Column convertName(Column column) {
        return column.rename(convertName(column.getName()));
    }

    protected abstract TableSchema convertTableSchema(TableSchema tableSchema);
}
