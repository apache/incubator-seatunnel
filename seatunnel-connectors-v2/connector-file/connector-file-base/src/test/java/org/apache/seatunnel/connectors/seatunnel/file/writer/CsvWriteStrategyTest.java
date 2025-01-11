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

package org.apache.seatunnel.connectors.seatunnel.file.writer;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.CsvWriteStrategy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CsvWriteStrategyTest {

    private CsvWriteStrategy csvWriteStrategy;

    @BeforeEach
    public void setUp() {
        Map<String, Object> writeConfig = new HashMap<>();
        writeConfig.put("tmp_path", "file:///tmp/seatunnel/parquet/int96");
        writeConfig.put("path", "file:///tmp/seatunnel/parquet/int96");
        writeConfig.put("parquet_avro_write_timestamp_as_int96", "true");
        writeConfig.put("parquet_avro_write_fixed_as_int96", Collections.singletonList("f3_bytes"));
        writeConfig.put("file_format_type", FileFormat.CSV.name());

        SeaTunnelRowType writeRowType =
                new SeaTunnelRowType(
                        new String[] {"f1", "f2", "f3", "f4"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE),
                            new SeaTunnelRowType(
                                    new String[] {"f1", "f2", "f3", "f4"},
                                    new SeaTunnelDataType[] {
                                        BasicType.STRING_TYPE,
                                        BasicType.STRING_TYPE,
                                        new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE),
                                        BasicType.LONG_TYPE
                                    })
                        });
        FileSinkConfig writeSinkConfig =
                new FileSinkConfig(ConfigFactory.parseMap(writeConfig), writeRowType);
        csvWriteStrategy = new CsvWriteStrategy(writeSinkConfig);
    }

    @Test
    public void addQuotesToCsvFields_BasicNonStringField_NoQuotes() {
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "f1", BasicType.STRING_TYPE, 22, false, null, "non-nested"))
                        .column(
                                PhysicalColumn.of(
                                        "f2", BasicType.STRING_TYPE, 22, false, null, "nested"))
                        .column(
                                PhysicalColumn.of(
                                        "f3",
                                        new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE),
                                        22,
                                        false,
                                        null,
                                        ""))
                        .column(
                                PhysicalColumn.of(
                                        "f4",
                                        new SeaTunnelRowType(
                                                new String[] {"f1", "f2", "f3", "f4"},
                                                new SeaTunnelDataType[] {
                                                    BasicType.STRING_TYPE,
                                                    BasicType.STRING_TYPE,
                                                    new MapType<>(
                                                            BasicType.STRING_TYPE,
                                                            BasicType.STRING_TYPE),
                                                    BasicType.LONG_TYPE
                                                }),
                                        22,
                                        false,
                                        null,
                                        ""))
                        .build();
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", "seatunnel", "source"),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "table");
        csvWriteStrategy.setCatalogTable(catalogTable);
        HashMap<String, String> objects = new HashMap<>();
        objects.put("f1", "Hi World");
        objects.put("f2", "This is a \"quoted\" string");
        objects.put("f3", "This is a \"quoted\" string");

        SeaTunnelRow f4 =
                new SeaTunnelRow(
                        new Object[] {"Hi World", "This is a \"quoted\" string", objects, 30L});
        SeaTunnelRow row =
                new SeaTunnelRow(
                        new Object[] {"Hi World", "This is a \"quoted\" string", objects, f4});
        csvWriteStrategy.addQuotesToCsvFields(row);
        assertEquals("\"Hi World\"", row.getField(0));
        assertEquals("\"This is a \"\"quoted\"\" string\"", row.getField(1));
        // map structure strings temporarily use raw escaping
        Assertions.assertNotNull(((HashMap<?, ?>) row.getField(2)).get("f1"));
        Assertions.assertNotNull(((HashMap<?, ?>) row.getField(2)).get("f2"));
        Assertions.assertNotNull(((HashMap<?, ?>) row.getField(2)).get("f3"));
    }
}
