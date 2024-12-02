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

package org.apache.seatunnel.connectors.cdc.debezium.row;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationConverterFactory;
import org.apache.seatunnel.connectors.cdc.debezium.MetadataConverter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;

public class SeaTunnelRowDebeziumDeserializationConvertersTest {

    @Test
    void testDefaultValueNotUsed() throws Exception {
        SeaTunnelRowDebeziumDeserializationConverters converters =
                new SeaTunnelRowDebeziumDeserializationConverters(
                        new SeaTunnelRowType(
                                new String[] {"id", "name"},
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.STRING_TYPE
                                }),
                        new MetadataConverter[] {},
                        ZoneId.systemDefault(),
                        DebeziumDeserializationConverterFactory.DEFAULT);
        Schema schema =
                SchemaBuilder.struct()
                        .field("id", SchemaBuilder.int32().build())
                        .field("name", SchemaBuilder.string().defaultValue("UL"))
                        .build();
        Struct value = new Struct(schema);
        // the value of `name` is null, so do not put value for it
        value.put("id", 1);
        SourceRecord record =
                new SourceRecord(
                        new HashMap<>(),
                        new HashMap<>(),
                        "topicName",
                        null,
                        SchemaBuilder.int32().build(),
                        1,
                        schema,
                        value,
                        null,
                        new ArrayList<>());

        SeaTunnelRow row = converters.convert(record, value, schema);
        Assertions.assertEquals(row.getField(0), 1);
        Assertions.assertNull(row.getField(1));
    }
}
