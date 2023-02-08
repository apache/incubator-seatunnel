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

package org.apache.seatunnel.connectors.seatunnel.fake.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.fake.config.FakeConfig;
import org.apache.seatunnel.connectors.seatunnel.fake.exception.FakeConnectorException;
import org.apache.seatunnel.connectors.seatunnel.fake.utils.FakeDataRandomUtils;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FakeDataGenerator {
    private final SeaTunnelSchema schema;
    private final FakeConfig fakeConfig;
    private final JsonDeserializationSchema jsonDeserializationSchema;
    private final FakeDataRandomUtils fakeDataRandomUtils;

    public FakeDataGenerator(SeaTunnelSchema schema, FakeConfig fakeConfig) {
        this.schema = schema;
        this.fakeConfig = fakeConfig;
        this.jsonDeserializationSchema = fakeConfig.getFakeRows() == null ?
            null :
            new JsonDeserializationSchema(
                false, false, schema.getSeaTunnelRowType());
        this.fakeDataRandomUtils = new FakeDataRandomUtils(fakeConfig);
    }

    private SeaTunnelRow convertRow(FakeConfig.RowData rowData) {
        try {
            SeaTunnelRow seaTunnelRow = jsonDeserializationSchema.deserialize(rowData.getFieldsJson());
            if (rowData.getKind() != null) {
                seaTunnelRow.setRowKind(RowKind.valueOf(rowData.getKind()));
            }
            return seaTunnelRow;
        } catch (IOException e) {
            throw new FakeConnectorException(CommonErrorCode.JSON_OPERATION_FAILED, e);
        }
    }

    private SeaTunnelRow randomRow() {
        SeaTunnelRowType seaTunnelRowType = schema.getSeaTunnelRowType();
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        List<Object> randomRow = new ArrayList<>(fieldNames.length);
        for (SeaTunnelDataType<?> fieldType : fieldTypes) {
            randomRow.add(randomColumnValue(fieldType));
        }
        return new SeaTunnelRow(randomRow.toArray());
    }

    /**
     * @param rowNum The number of pieces of data to be generated by the current task
     * @param output Data collection and distribution
     **/
    public void collectFakedRows(int rowNum, Collector<SeaTunnelRow> output) {
        // Use manual configuration data preferentially
        if (fakeConfig.getFakeRows() != null) {
            for (FakeConfig.RowData rowData : fakeConfig.getFakeRows()) {
                output.collect(convertRow(rowData));
            }
        } else {
            for (int i = 0; i < rowNum; i++) {
                output.collect(randomRow());
            }
        }
    }

    @SuppressWarnings("magicnumber")
    private Object randomColumnValue(SeaTunnelDataType<?> fieldType) {
        switch (fieldType.getSqlType()) {
            case ARRAY:
                ArrayType<?, ?> arrayType = (ArrayType<?, ?>) fieldType;
                BasicType<?> elementType = arrayType.getElementType();
                int length = fakeConfig.getArraySize();
                Object array = Array.newInstance(elementType.getTypeClass(), length);
                for (int i = 0; i < length; i++) {
                    Object value = randomColumnValue(elementType);
                    Array.set(array, i, value);
                }
                return array;
            case MAP:
                MapType<?, ?> mapType = (MapType<?, ?>) fieldType;
                SeaTunnelDataType<?> keyType = mapType.getKeyType();
                SeaTunnelDataType<?> valueType = mapType.getValueType();
                HashMap<Object, Object> objectMap = new HashMap<>();
                int mapSize = fakeConfig.getMapSize();
                for (int i = 0; i < mapSize; i++) {
                    Object key = randomColumnValue(keyType);
                    Object value = randomColumnValue(valueType);
                    objectMap.put(key, value);
                }
                return objectMap;
            case STRING:
                return fakeDataRandomUtils.randomString();
            case BOOLEAN:
                return fakeDataRandomUtils.randomBoolean();
            case TINYINT:
                return fakeDataRandomUtils.randomTinyint();
            case SMALLINT:
                return fakeDataRandomUtils.randomSmallint();
            case INT:
                return fakeDataRandomUtils.randomInt();
            case BIGINT:
                return fakeDataRandomUtils.randomBigint();
            case FLOAT:
                return fakeDataRandomUtils.randomFloat();
            case DOUBLE:
                return fakeDataRandomUtils.randomDouble();
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                return fakeDataRandomUtils.randomBigDecimal(decimalType.getPrecision(), decimalType.getScale());
            case NULL:
                return null;
            case BYTES:
                return fakeDataRandomUtils.randomBytes();
            case DATE:
                return fakeDataRandomUtils.randomLocalDate();
            case TIME:
                return fakeDataRandomUtils.randomLocalTime();
            case TIMESTAMP:
                return fakeDataRandomUtils.randomLocalDateTime();
            case ROW:
                SeaTunnelDataType<?>[] fieldTypes = ((SeaTunnelRowType) fieldType).getFieldTypes();
                Object[] objects = new Object[fieldTypes.length];
                for (int i = 0; i < fieldTypes.length; i++) {
                    Object object = randomColumnValue(fieldTypes[i]);
                    objects[i] = object;
                }
                return new SeaTunnelRow(objects);
            default:
                // never got in there
                throw new FakeConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "SeaTunnel Fake source connector not support this data type");
        }
    }
}
