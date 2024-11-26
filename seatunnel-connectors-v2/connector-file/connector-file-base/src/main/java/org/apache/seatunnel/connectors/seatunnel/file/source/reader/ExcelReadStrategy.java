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

// import com.alibaba.excel.metadata.Cell;
// import com.alibaba.excel.metadata.data.ReadCellData;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.excel.ExcelReaderListener;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import org.apache.poi.ss.usermodel.DateUtil;

import org.dhatim.fastexcel.reader.Cell;
import org.dhatim.fastexcel.reader.CellType;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.read.builder.ExcelReaderBuilder;
import lombok.SneakyThrows;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;
import java.util.Objects;

public class ExcelReadStrategy extends AbstractReadStrategy {

    private int[] indexes;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private SeaTunnelDataType<?>[] fieldTypes;

    private DateTimeFormatter dateFormatter;
    private DateTimeFormatter dateTimeFormatter;
    private DateTimeFormatter timeFormatter;

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
            String currentFileName)
            throws IOException {

        if (skipHeaderNumber > Integer.MAX_VALUE || skipHeaderNumber < Integer.MIN_VALUE) {
            throw new FileConnectorException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    "Skip the number of rows exceeds the maximum or minimum limit of Sheet");
        }

        //        fieldTypes = seaTunnelRowType.getFieldTypes();
        //
        //        ReadableWorkbook wb = new ReadableWorkbook(inputStream);
        //        Sheet sheet;
        //        if (pluginConfig.hasPath(BaseSourceConfigOptions.SHEET_NAME.key())) {
        //            sheet =
        //
        // wb.findSheet(pluginConfig.getString(BaseSourceConfigOptions.SHEET_NAME.key()))
        //                            .get();
        //        } else {
        //            sheet = wb.getFirstSheet();
        //        }
        //        Stream<Row> rowStream = sheet.openStream();
        //
        //        //        long count = rowStream.count();
        //
        //        if (skipHeaderNumber > Integer.MAX_VALUE || skipHeaderNumber < Integer.MIN_VALUE
        //        //                || count <= skipHeaderNumber
        //        ) {
        //            throw new FileConnectorException(
        //                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
        //                    "Skip the number of rows exceeds the maximum or minimum limit of
        // Sheet");
        //        }
        //
        //        if (pluginConfig.hasPath(BaseSourceConfigOptions.DATE_FORMAT.key())) {
        //            String dateFormatString =
        //                    pluginConfig.getString(BaseSourceConfigOptions.DATE_FORMAT.key());
        //            dateFormatter = DateTimeFormatter.ofPattern(dateFormatString);
        //        }
        //        if (pluginConfig.hasPath(BaseSourceConfigOptions.DATETIME_FORMAT.key())) {
        //            String datetimeFormatString =
        //                    pluginConfig.getString(BaseSourceConfigOptions.DATETIME_FORMAT.key());
        //            dateTimeFormatter = DateTimeFormatter.ofPattern(datetimeFormatString);
        //        }
        //        if (pluginConfig.hasPath(BaseSourceConfigOptions.TIME_FORMAT.key())) {
        //            String timeFormatString =
        //                    pluginConfig.getString(BaseSourceConfigOptions.TIME_FORMAT.key());
        //            timeFormatter = DateTimeFormatter.ofPattern(timeFormatString);
        //        }
        //
        //        Iterator<Row> iterator = rowStream.iterator();
        //        int rowCounter = 0;
        //        while (iterator.hasNext()) {
        //            if (rowCounter++ < skipHeaderNumber) {
        //                iterator.next();
        //                continue;
        //            }
        //            Row row = iterator.next();
        //            SeaTunnelRow seaTunnelRow = new SeaTunnelRow(row.getCellCount());
        //            int cellCounter = 0;
        //            Iterator<org.dhatim.fastexcel.reader.Cell> cellIterator =
        // row.stream().iterator();
        //            while (cellIterator.hasNext()) {
        //                org.dhatim.fastexcel.reader.Cell cell = cellIterator.next();
        //                Object cellCovert = convert(cell.getText(), cell,
        // fieldTypes[cellCounter]);
        //                seaTunnelRow.setField(cellCounter, cellCovert);
        //                cellCounter++;
        //            }
        //            seaTunnelRow.setTableId(tableId);
        //            output.collect(seaTunnelRow);
        //        }

        ExcelReaderBuilder read =
                EasyExcel.read(
                        inputStream,
                        new ExcelReaderListener(tableId, output, pluginConfig, seaTunnelRowType));
        if (pluginConfig.hasPath(BaseSourceConfigOptions.SHEET_NAME.key())) {
            read.sheet(pluginConfig.getString(BaseSourceConfigOptions.SHEET_NAME.key()))
                    .headRowNumber((int) skipHeaderNumber)
                    .doReadSync();
        } else {
            read.sheet(0).headRowNumber((int) skipHeaderNumber).doReadSync();
        }
    }

    @SneakyThrows(JsonProcessingException.class)
    private Object convert(Object field, Cell cellRaw, SeaTunnelDataType<?> fieldType) {
        if (field == null) {
            return "";
        }
        SqlType sqlType = fieldType.getSqlType();

        switch (sqlType) {
            case MAP:
            case ARRAY:
                return objectMapper.readValue((String) field, fieldType.getTypeClass());
            case STRING:
                return String.valueOf(field);
            case DOUBLE:
                return Double.parseDouble(field.toString());
            case BOOLEAN:
                return Boolean.parseBoolean(field.toString());
            case FLOAT:
                return (float) Double.parseDouble(field.toString());
            case BIGINT:
                return (long) Double.parseDouble(field.toString());
            case INT:
                return (int) Double.parseDouble(field.toString());
            case TINYINT:
                return (byte) Double.parseDouble(field.toString());
            case SMALLINT:
                return (short) Double.parseDouble(field.toString());
            case DECIMAL:
                return BigDecimal.valueOf(Double.parseDouble(field.toString()));
            case DATE:
                if (field instanceof LocalDateTime) {
                    return ((LocalDateTime) field).toLocalDate();
                } else if (pluginConfig.hasPath(BaseSourceConfigOptions.DATE_FORMAT.key())) {
                    return LocalDate.parse((String) field, dateFormatter);
                } else if (cellRaw != null && cellRaw.getType() == CellType.NUMBER) {
                    double v = Double.parseDouble(cellRaw.getText());
                    Date javaDate = DateUtil.getJavaDate(v);
                    return javaDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                } else {
                    return LocalDate.parse(
                            (String) field, DateUtils.matchDateFormatter((String) field));
                }
            case TIME:
                if (pluginConfig.hasPath(BaseSourceConfigOptions.TIME_FORMAT.key())) {
                    return LocalTime.parse((String) field, timeFormatter);
                } else if (cellRaw != null && cellRaw.getType() == CellType.NUMBER) {
                    double v = Double.parseDouble(cellRaw.getText());
                    Date javaDate = DateUtil.getJavaDate(v);
                    LocalTime localTime =
                            LocalTime.of(
                                    javaDate.getHours(),
                                    javaDate.getMinutes(),
                                    javaDate.getSeconds());
                    return localTime;
                } else {
                    return LocalTime.parse(
                            (String) field, TimeUtils.matchTimeFormatter((String) field));
                }
            case TIMESTAMP:
                if (pluginConfig.hasPath(BaseSourceConfigOptions.DATETIME_FORMAT.key())) {
                    return LocalDateTime.parse((String) field, dateTimeFormatter);
                } else if (cellRaw != null && cellRaw.getType() == CellType.NUMBER) {
                    double v = Double.parseDouble(cellRaw.getText());
                    Date date = DateUtil.getJavaDate(v);
                    return LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
                } else {
                    return LocalDateTime.parse(
                            (String) field,
                            Objects.requireNonNull(
                                    DateTimeUtils.matchDateTimeFormatter((String) field)));
                }
            case NULL:
                return "";
            case BYTES:
                return field.toString().getBytes(StandardCharsets.UTF_8);
            case ROW:
                String delimiter =
                        ReadonlyConfig.fromConfig(pluginConfig)
                                .get(BaseSourceConfigOptions.FIELD_DELIMITER);
                String[] context = field.toString().split(delimiter);
                SeaTunnelRowType ft = (SeaTunnelRowType) fieldType;
                int length = context.length;
                SeaTunnelRow seaTunnelRow = new SeaTunnelRow(length);
                for (int j = 0; j < length; j++) {
                    seaTunnelRow.setField(j, convert(context[j], null, ft.getFieldType(j)));
                }
                return seaTunnelRow;
            default:
                throw new FileConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        "User defined schema validation failed");
        }
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

    private static LocalTime convertExcelTimeToJavaLocalTime(double excelTime) {
        // Excel中的一天是1.0，所以将小数部分转换为小时
        double hours = excelTime - (int) excelTime;
        // 将小时的小数部分转换为分钟
        double minutes = (hours - (int) hours) * 60;
        // 将分钟的小数部分转换为秒
        double seconds = (minutes - (int) minutes) * 60;

        // 使用整数部分创建LocalTime对象
        return LocalTime.of((int) excelTime, (int) hours, (int) minutes, (int) Math.round(seconds));
    }
}
