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

package org.apache.seatunnel.connectors.seatunnel.file.sink.writer;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import lombok.NonNull;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

public class CsvWriteStrategy extends TextWriteStrategy {

    public CsvWriteStrategy(FileSinkConfig fileSinkConfig) {
        super(fileSinkConfig);
    }

    @Override
    public void write(@NonNull SeaTunnelRow seaTunnelRow) {
        String filePath = getOrCreateFilePathBeingWritten(seaTunnelRow);
        try {
            addQuotesToCsvFields(seaTunnelRow);
            super.write(seaTunnelRow);
        } catch (Exception e) {
            throw CommonError.fileOperationFailed("CsvFile", "write", filePath, e);
        }
    }

    @VisibleForTesting
    public void addQuotesToCsvFields(SeaTunnelRow seaTunnelRow) {
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            SeaTunnelDataType<?> fieldType = seaTunnelRowType.getFieldType(i);
            Object fieldValue = seaTunnelRow.getField(i);
            Object newFieldValue = addQuotes(fieldValue, fieldType);
            seaTunnelRow.setField(i, newFieldValue);
        }
    }

    private Object addQuotes(Object fieldValue, SeaTunnelDataType<?> fieldType) {
        if (fieldType instanceof BasicType) {
            if (fieldType.getSqlType() == SqlType.STRING) {
                return addQuotesUsingCSVFormat(fieldValue.toString());
            }
        } else if (fieldType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) fieldType;
            SeaTunnelDataType<?> elementType = arrayType.getElementType();
            if (fieldValue instanceof Object[]) {
                Object[] values = (Object[]) fieldValue;
                Object[] newValues = new Object[values.length];
                for (int i = 0; i < values.length; i++) {
                    newValues[i] = addQuotes(values[i], elementType);
                }
                return newValues;
            }
        } else if (fieldType instanceof MapType) {
            MapType mapType = (MapType) fieldType;
            SeaTunnelDataType<?> keyType = mapType.getKeyType();
            SeaTunnelDataType<?> valueType = mapType.getValueType();
            if (fieldValue instanceof Map) {
                Map<?, ?> values = (Map<?, ?>) fieldValue;
                Map<Object, Object> newValues = new HashMap<>();
                for (Map.Entry<?, ?> entry : values.entrySet()) {
                    Object newKey = addQuotes(entry.getKey(), keyType);
                    Object newValue = addQuotes(entry.getValue(), valueType);
                    newValues.put(newKey, newValue);
                }
                return newValues;
            }
        } else if (fieldType instanceof SeaTunnelRowType) {
            SeaTunnelRowType rowType = (SeaTunnelRowType) fieldType;
            SeaTunnelRow row = (SeaTunnelRow) fieldValue;
            String[] fieldNames = rowType.getFieldNames();
            for (int i = 0; i < fieldNames.length; i++) {
                SeaTunnelDataType<?> subFieldType = rowType.getFieldType(i);
                Object subFieldValue = row.getField(i);
                row.setField(i, addQuotes(subFieldValue, subFieldType));
            }
            return row;
        }
        return fieldValue;
    }

    private String addQuotesUsingCSVFormat(String fieldValue) {
        CSVFormat format =
                CSVFormat.DEFAULT
                        .builder()
                        .setQuoteMode(org.apache.commons.csv.QuoteMode.ALL)
                        .setRecordSeparator("")
                        .build();
        StringWriter stringWriter = new StringWriter();
        try (CSVPrinter printer = new CSVPrinter(stringWriter, format)) {
            printer.printRecord(fieldValue);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return stringWriter.toString();
    }
}
