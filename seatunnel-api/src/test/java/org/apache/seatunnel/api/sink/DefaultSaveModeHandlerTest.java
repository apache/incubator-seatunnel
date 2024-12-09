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

package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.InMemoryCatalog;
import org.apache.seatunnel.api.table.catalog.InMemoryCatalogFactory;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultSaveModeHandlerTest {

    private SeaTunnelRowType rowType;
    private InMemoryCatalogFactory catalogFactory;

    @BeforeEach
    public void setup() {
        String[] fieldNames = new String[] {"id", "name", "description", "weight"};
        SeaTunnelDataType<?>[] dataTypes =
                new SeaTunnelDataType[] {
                    BasicType.LONG_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE
                };
        rowType = new SeaTunnelRowType(fieldNames, dataTypes);
        catalogFactory = new InMemoryCatalogFactory();
    }

    @Test
    public void shouldTruncateExistingTable() {
        // SchemaSaveMode is CREATE_SCHEMA_WHEN_NOT_EXIST and DataSaveMode is DROP_DATA and table
        // exist, truncateTable needs to be executed
        CatalogTable catalogTable = createCatalogTable("table1");
        Catalog catalog = catalogFactory.createCatalog("test", null);
        DefaultSaveModeHandler handler =
                createHandler(
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                        DataSaveMode.DROP_DATA,
                        catalog,
                        catalogTable);

        handler.handleSchemaSaveMode();
        handler.handleDataSaveMode();

        InMemoryCatalog inMemoryCatalog = (InMemoryCatalog) catalog;
        assertTrue(inMemoryCatalog.isRunTruncateTable(), "Should truncate data for existing table");
    }

    @Test
    public void shouldNotTruncateNewlyCreatedTable() {
        // SchemaSaveMode is CREATE_SCHEMA_WHEN_NOT_EXIST and DataSaveMode is DROP_DATA and table
        // not exist, truncateTable no needs to be executed
        CatalogTable catalogTable = createCatalogTable("notExistsTable");
        Catalog catalog = catalogFactory.createCatalog("test", null);
        DefaultSaveModeHandler handler =
                createHandler(
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                        DataSaveMode.DROP_DATA,
                        catalog,
                        catalogTable);

        handler.handleSchemaSaveMode();
        handler.handleDataSaveMode();

        InMemoryCatalog inMemoryCatalog = (InMemoryCatalog) catalog;
        assertFalse(
                inMemoryCatalog.isRunTruncateTable(),
                "Should not truncate data for newly created table");
    }

    @Test
    public void shouldNotTruncateRecreatedTable() {
        // SchemaSaveMode is RECREATE_SCHEMA and DataSaveMode is DROP_DATA , truncateTable no needs
        // to be executed
        CatalogTable catalogTable = createCatalogTable("notExistsTable");
        Catalog catalog = catalogFactory.createCatalog("test", null);
        DefaultSaveModeHandler handler =
                createHandler(
                        SchemaSaveMode.RECREATE_SCHEMA,
                        DataSaveMode.DROP_DATA,
                        catalog,
                        catalogTable);

        handler.handleSchemaSaveMode();
        handler.handleDataSaveMode();

        InMemoryCatalog inMemoryCatalog = (InMemoryCatalog) catalog;
        assertFalse(
                inMemoryCatalog.isRunTruncateTable(),
                "Should not truncate data for recreated table");
    }

    private CatalogTable createCatalogTable(String tableName) {
        return CatalogTableUtil.getCatalogTable("", "st", "public", tableName, rowType);
    }

    private DefaultSaveModeHandler createHandler(
            SchemaSaveMode schemaSaveMode,
            DataSaveMode dataSaveMode,
            Catalog catalog,
            CatalogTable catalogTable) {
        return new DefaultSaveModeHandler(
                schemaSaveMode, dataSaveMode, catalog, catalogTable, null);
    }
}
