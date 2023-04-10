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

package org.apache.seatunnel.connectors.seatunnel.hudi;

import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.writer.HudiOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.org.apache.avro.Schema;
import org.apache.hudi.org.apache.avro.generic.GenericData;
import org.apache.hudi.org.apache.avro.generic.GenericRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.table.type.BasicType.BOOLEAN_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.FLOAT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.INT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.LONG_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;

public class HudiTest {

    private static final String tablePath = "D:\\tmp\\hudi";
    private static final String tableName = "hudi";

    protected static final String DEFAULT_PARTITION_PATH = "default";
    public static final String DEFAULT_PARTITION_PATH_SEPARATOR = "/";
    protected static final String NULL_RECORDKEY_PLACEHOLDER = "__null__";
    protected static final String EMPTY_RECORDKEY_PLACEHOLDER = "__empty__";

    private static final String recordKeyFields = "int";

    private static final String partitionFields = "timestamp3";

    private static final SeaTunnelRowType seaTunnelRowType =
            new SeaTunnelRowType(
                    new String[] {
                        "bool",
                        "int",
                        "longValue",
                        "float",
                        "name",
                        "date",
                        "time",
                        "timestamp3",
                        "map"
                    },
                    new SeaTunnelDataType[] {
                        BOOLEAN_TYPE,
                        INT_TYPE,
                        LONG_TYPE,
                        FLOAT_TYPE,
                        STRING_TYPE,
                        LocalTimeType.LOCAL_DATE_TYPE,
                        LocalTimeType.LOCAL_TIME_TYPE,
                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                        new MapType(STRING_TYPE, LONG_TYPE),
                    });

    private static final HudiOutputFormat hudiOutputFormat = new HudiOutputFormat();

    private String getSchema() {
        return hudiOutputFormat.convertSchema(seaTunnelRowType);
    }

    @Test
    void testSchema() {
        System.out.println(getSchema());
        Assertions.assertEquals(
                "{\"type\":\"record\",\"name\":\"seaTunnelRow\",\"fields\":[{\"name\":\"bool\",\"type\":\"boolean\"},{\"name\":\"int\",\"type\":\"int\"},{\"name\":\"longValue\",\"type\":\"long\"},{\"name\":\"float\",\"type\":\"float\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"date\",\"type\":{\"type\":\"array\",\"items\":\"int\"}},{\"name\":\"time\",\"type\":{\"type\":\"array\",\"items\":\"int\"}},{\"name\":\"timestamp3\",\"type\":{\"type\":\"array\",\"items\":\"long\"}},{\"name\":\"map\",\"type\":{\"type\":\"map\",\"values\":\"long\"}}]}",
                getSchema());
    }

    @Test
    void testWriteData() throws IOException {
        HoodieTableMetaClient.withPropertyBuilder()
                .setTableType(HoodieTableType.COPY_ON_WRITE)
                .setTableName(tableName)
                .setPayloadClassName(HoodieAvroPayload.class.getName())
                .initTable(new Configuration(), tablePath);

        HoodieWriteConfig cfg =
                HoodieWriteConfig.newBuilder()
                        .withPath(tablePath)
                        .withSchema(getSchema())
                        .withParallelism(2, 2)
                        .withDeleteParallelism(2)
                        .forTable(tableName)
                        .withIndexConfig(
                                HoodieIndexConfig.newBuilder()
                                        .withIndexType(HoodieIndex.IndexType.INMEMORY)
                                        .build())
                        .withArchivalConfig(
                                HoodieArchivalConfig.newBuilder()
                                        .archiveCommitsWith(11, 25)
                                        .build())
                        .build();

        try (HoodieJavaWriteClient<HoodieAvroPayload> javaWriteClient =
                new HoodieJavaWriteClient<>(
                        new HoodieJavaEngineContext(new Configuration()), cfg)) {
            SeaTunnelRow expected = new SeaTunnelRow(12);
            expected.setField(0, true);
            expected.setField(1, 45536);
            expected.setField(2, 1238123899121L);
            expected.setField(3, 33.333F);
            expected.setField(4, "asdlkjasjkdla998y1122");
            expected.setField(5, LocalDate.parse("1990-10-14"));
            expected.setField(6, LocalTime.parse("12:12:43"));
            expected.setField(7, Timestamp.valueOf("1990-10-14 12:12:43.123"));
            Map<String, Long> map = new HashMap<>();
            map.put("element", 123L);
            expected.setField(9, map);
            String instantTime = javaWriteClient.startCommit();
            List<HoodieRecord<HoodieAvroPayload>> hoodieRecords = new ArrayList<>();
            hoodieRecords.add(convertRow(expected));
            List<WriteStatus> insert = javaWriteClient.insert(hoodieRecords, instantTime);
            javaWriteClient.commit(instantTime, insert);
        }
    }

    private HoodieRecord<HoodieAvroPayload> convertRow(SeaTunnelRow element) {
        GenericRecord rec =
                new GenericData.Record(
                        new Schema.Parser()
                                .parse(hudiOutputFormat.convertSchema(seaTunnelRowType)));
        for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
            rec.put(seaTunnelRowType.getFieldNames()[i], element.getField(i));
        }

        return new HoodieAvroRecord<>(
                getHoodieKey(element, seaTunnelRowType), new HoodieAvroPayload(Option.of(rec)));
    }

    private HoodieKey getHoodieKey(SeaTunnelRow element, SeaTunnelRowType seaTunnelRowType) {
        String partitionPath = getRecordPartitionPath(element, seaTunnelRowType);
        String rowKey = getRecordKey(element, seaTunnelRowType);
        return new HoodieKey(rowKey, partitionPath);
    }

    private String getRecordKey(SeaTunnelRow element, SeaTunnelRowType seaTunnelRowType) {
        boolean keyIsNullEmpty = true;
        StringBuilder recordKey = new StringBuilder();
        for (String recordKeyField : recordKeyFields.split(",")) {
            String recordKeyValue =
                    getNestedFieldValAsString(element, seaTunnelRowType, recordKeyField);
            recordKeyField = recordKeyField.toLowerCase();
            if (recordKeyValue == null) {
                recordKey.append(recordKeyField + ":" + NULL_RECORDKEY_PLACEHOLDER + ",");
            } else if (recordKeyValue.isEmpty()) {
                recordKey.append(recordKeyField + ":" + EMPTY_RECORDKEY_PLACEHOLDER + ",");
            } else {
                recordKey.append(recordKeyField + ":" + recordKeyValue + ",");
                keyIsNullEmpty = false;
            }
        }
        recordKey.deleteCharAt(recordKey.length() - 1);
        if (keyIsNullEmpty) {
            throw new HoodieKeyException(
                    "recordKey values: \""
                            + recordKey
                            + "\" for fields: "
                            + recordKeyFields
                            + " cannot be entirely null or empty.");
        }
        return recordKey.toString();
    }

    private String getRecordPartitionPath(SeaTunnelRow element, SeaTunnelRowType seaTunnelRowType) {

        StringBuilder partitionPath = new StringBuilder();
        String[] avroPartitionPathFields = partitionFields.split(",");
        for (String partitionPathField : avroPartitionPathFields) {
            String fieldVal =
                    getNestedFieldValAsString(element, seaTunnelRowType, partitionPathField);
            if (fieldVal == null || fieldVal.isEmpty()) {
                partitionPath.append(partitionPathField + "=" + DEFAULT_PARTITION_PATH);
            } else {
                partitionPath.append(partitionPathField + "=" + fieldVal);
            }
            partitionPath.append(DEFAULT_PARTITION_PATH_SEPARATOR);
        }
        partitionPath.deleteCharAt(partitionPath.length() - 1);
        return partitionPath.toString();
    }

    private String getNestedFieldValAsString(
            SeaTunnelRow element, SeaTunnelRowType seaTunnelRowType, String fieldName) {
        Object value = null;

        if (Arrays.stream(seaTunnelRowType.getFieldNames())
                .collect(Collectors.toList())
                .contains(fieldName)) {
            value = element.getField(seaTunnelRowType.indexOf(fieldName));
        }
        return StringUtils.objToString(value);
    }
}
