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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.exception.MongodbConnectorException;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT;

@AutoService(Factory.class)
public class MongodbIncrementalSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return MongodbIncrementalSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return MongodbSourceOptions.getBaseRule()
                .required(
                        MongodbSourceOptions.HOSTS,
                        MongodbSourceOptions.DATABASE,
                        MongodbSourceOptions.COLLECTION,
                        TableSchemaOptions.SCHEMA)
                .optional(
                        MongodbSourceOptions.USERNAME,
                        MongodbSourceOptions.PASSWORD,
                        MongodbSourceOptions.CONNECTION_OPTIONS,
                        MongodbSourceOptions.BATCH_SIZE,
                        MongodbSourceOptions.POLL_MAX_BATCH_SIZE,
                        MongodbSourceOptions.POLL_AWAIT_TIME_MILLIS,
                        MongodbSourceOptions.HEARTBEAT_INTERVAL_MILLIS,
                        MongodbSourceOptions.INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB,
                        MongodbSourceOptions.STARTUP_MODE,
                        MongodbSourceOptions.STOP_MODE)
                .conditional(
                        MongodbSourceOptions.STARTUP_MODE,
                        StartupMode.TIMESTAMP,
                        SourceOptions.STARTUP_TIMESTAMP)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return MongodbIncrementalSource.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> {
            List<CatalogTable> configCatalog =
                    CatalogTableUtil.getCatalogTables(
                            context.getOptions(), context.getClassLoader());
            List<String> collections = context.getOptions().get(MongodbSourceOptions.COLLECTION);
            if (collections.size() != configCatalog.size()) {
                throw new MongodbConnectorException(
                        ILLEGAL_ARGUMENT,
                        "The number of collections must be equal to the number of schema tables");
            }
            List<CatalogTable> catalogTables =
                    IntStream.range(0, configCatalog.size())
                            .mapToObj(
                                    i -> {
                                        CatalogTable catalogTable = configCatalog.get(i);
                                        String fullName = collections.get(i);
                                        TableIdentifier tableIdentifier =
                                                TableIdentifier.of(
                                                        catalogTable.getCatalogName(),
                                                        TablePath.of(fullName));
                                        return CatalogTable.of(tableIdentifier, catalogTable);
                                    })
                            .collect(Collectors.toList());
            return (SeaTunnelSource<T, SplitT, StateT>)
                    new MongodbIncrementalSource<>(context.getOptions(), catalogTables);
        };
    }
}
