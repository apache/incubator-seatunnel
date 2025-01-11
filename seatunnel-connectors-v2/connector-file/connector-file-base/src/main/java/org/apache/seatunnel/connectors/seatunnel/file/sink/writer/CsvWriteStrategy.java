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

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import lombok.NonNull;

import java.io.StringWriter;

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
                return addQuotesUsingCsvFormat(fieldValue.toString());
            }
        }
        return fieldValue;
    }

    private String addQuotesUsingCsvFormat(String fieldValue) {
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
