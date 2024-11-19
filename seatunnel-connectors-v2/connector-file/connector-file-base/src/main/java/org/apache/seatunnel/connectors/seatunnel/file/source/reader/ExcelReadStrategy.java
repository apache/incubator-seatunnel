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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;


import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.excel.ExcelReaderListener;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import com.alibaba.excel.EasyExcel;
import lombok.SneakyThrows;

import java.io.InputStream;
import java.util.Map;

public class ExcelReadStrategy extends AbstractReadStrategy {


    private int[] indexes;



    @SneakyThrows
    @Override
    public void read(String path, String tableId, Collector<SeaTunnelRow> output) {
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        resolveArchiveCompressedInputStream(path, tableId, output, partitionsMap, FileFormat.EXCEL);
    }

    @Override
    protected void readProcess(
            String path,
            String tableId,
            Collector<SeaTunnelRow> output,
            InputStream inputStream,
            Map<String, String> partitionsMap,
            String currentFileName) {

        if (skipHeaderNumber > Integer.MAX_VALUE || skipHeaderNumber < Integer.MIN_VALUE) {
            throw new FileConnectorException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    "Skip the number of rows exceeds the maximum or minimum limit of Sheet");
        }

        EasyExcel.read(inputStream, new ExcelReaderListener(
                        tableId,
                        output,
                        inputStream,
                        partitionsMap,
                        pluginConfig,
                        seaTunnelRowType))
                .sheet(pluginConfig.getString(BaseSourceConfigOptions.SHEET_NAME.key()))
                .headRowNumber((int) skipHeaderNumber)
                .doReadSync();
    }

    @Override
    public void setCatalogTable(CatalogTable catalogTable) {
        SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();
        if (isNullOrEmpty(rowType.getFieldNames()) || isNullOrEmpty(rowType.getFieldTypes())) {
            throw new FileConnectorException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    "Schema information is not set or incorrect Schema settings");
        }
        SeaTunnelRowType userDefinedRowTypeWithPartition =
                mergePartitionTypes(fileNames.get(0), rowType);
        // column projection
        if (pluginConfig.hasPath(BaseSourceConfigOptions.READ_COLUMNS.key())) {
            // get the read column index from user-defined row type
            indexes = new int[readColumns.size()];
            String[] fields = new String[readColumns.size()];
            SeaTunnelDataType<?>[] types = new SeaTunnelDataType[readColumns.size()];
            for (int i = 0; i < indexes.length; i++) {
                indexes[i] = rowType.indexOf(readColumns.get(i));
                fields[i] = rowType.getFieldName(indexes[i]);
                types[i] = rowType.getFieldType(indexes[i]);
            }
            this.seaTunnelRowType = new SeaTunnelRowType(fields, types);
            this.seaTunnelRowTypeWithPartition =
                    mergePartitionTypes(fileNames.get(0), this.seaTunnelRowType);
        } else {
            this.seaTunnelRowType = rowType;
            this.seaTunnelRowTypeWithPartition = userDefinedRowTypeWithPartition;
        }
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) throws FileConnectorException {
        throw new FileConnectorException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                "User must defined schema for json file type");
    }

    private <T> boolean isNullOrEmpty(T[] arr) {
        return arr == null || arr.length == 0;
    }
}
