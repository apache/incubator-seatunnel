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

package org.apache.seatunnel.connectors.seatunnel.file.excel;

import com.alibaba.excel.enums.CellDataTypeEnum;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import org.apache.poi.ss.usermodel.DateUtil;

import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;
import com.alibaba.excel.exception.ExcelDataConvertException;
import com.alibaba.excel.metadata.Cell;
import com.alibaba.excel.metadata.data.ReadCellData;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class ExcelReaderListener extends AnalysisEventListener<Map<Integer, Object>>
        implements Serializable, Closeable {
    private final String tableId;
    private final Collector<SeaTunnelRow> output;
    private int cellCount;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private DateTimeFormatter dateFormatter;
    private DateTimeFormatter dateTimeFormatter;
    private DateTimeFormatter timeFormatter;

    protected Config pluginConfig;

    protected SeaTunnelRowType seaTunnelRowType;

    private SeaTunnelDataType<?>[] fieldTypes;

    Map<Integer, String> customHeaders = new HashMap<>();

    public ExcelReaderListener(
            String tableId,
            Collector<SeaTunnelRow> output,
            Config pluginConfig,
            SeaTunnelRowType seaTunnelRowType) {
        this.tableId = tableId;
        this.output = output;
        this.pluginConfig = pluginConfig;
        this.seaTunnelRowType = seaTunnelRowType;

        fieldTypes = seaTunnelRowType.getFieldTypes();

        if (pluginConfig.hasPath(BaseSourceConfigOptions.DATE_FORMAT.key())) {
            String dateFormatString =
                    pluginConfig.getString(BaseSourceConfigOptions.DATE_FORMAT.key());
            dateFormatter = DateTimeFormatter.ofPattern(dateFormatString);
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.DATETIME_FORMAT.key())) {
            String datetimeFormatString =
                    pluginConfig.getString(BaseSourceConfigOptions.DATETIME_FORMAT.key());
            dateTimeFormatter = DateTimeFormatter.ofPattern(datetimeFormatString);
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.TIME_FORMAT.key())) {
            String timeFormatString =
                    pluginConfig.getString(BaseSourceConfigOptions.TIME_FORMAT.key());
            timeFormatter = DateTimeFormatter.ofPattern(timeFormatString);
        }
    }

    @Override
    public void invokeHead(Map<Integer, ReadCellData<?>> headMap, AnalysisContext context) {
        for (int i = 0; i < headMap.size(); i++) {
            String header = headMap.get(i).getStringValue();
            if (!"null".equals(header)) {
                customHeaders.put(i, header);
            }
        }
    }

    @Override
    public void invoke(Map<Integer, Object> data, AnalysisContext context) {
        cellCount = data.size();
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fieldTypes.length);
        Map<Integer, Cell> cellMap = context.readRowHolder().getCellMap();
        int i = 0;
        for (; i < fieldTypes.length; i++) {
            if (cellMap.get(i) == null){
                seaTunnelRow.setField(i, null);
            }else {
                Object cell = convert(data.get(i), cellMap.get(i), fieldTypes[i]);
                seaTunnelRow.setField(i, cell);
            }
        }
        seaTunnelRow.setTableId(tableId);
        output.collect(seaTunnelRow);
    }

    @Override
    public void doAfterAllAnalysed(AnalysisContext context) {
        log.info("excel parsing completed");
    }

    @Override
    public void onException(Exception exception, AnalysisContext context) {
        log.error("cell parsing exception :{}", exception.getMessage());
        if (exception instanceof ExcelDataConvertException) {
            ExcelDataConvertException excelDataConvertException =
                    (ExcelDataConvertException) exception;
            log.error(
                    "row:{},cell:{},data:{}",
                    excelDataConvertException.getRowIndex(),
                    excelDataConvertException.getColumnIndex(),
                    excelDataConvertException.getCellData());
        }
    }

    private String getCellValue(ReadCellData cellData) {

        if (cellData.getStringValue() != null) {
            return cellData.getStringValue();
        }else if(cellData.getNumberValue() != null        ) {
            return cellData.getNumberValue().toString();
        }else if (cellData.getOriginalNumberValue() != null) {
            return cellData.getOriginalNumberValue().toString();
        }else if ( cellData.getBooleanValue() != null){
            return cellData.getBooleanValue().toString();
        }
        else if ( cellData.getType()== CellDataTypeEnum.EMPTY){
            return "";
        }
        return null;
    }

    @SneakyThrows(JsonProcessingException.class)
    private Object convert(Object field, Cell cellRaw, SeaTunnelDataType<?> fieldType) {

        ReadCellData cellData = (ReadCellData) cellRaw;
        SqlType sqlType = fieldType.getSqlType();

        String cellValue = getCellValue(cellData);
        if (cellValue == null || (cellValue.equals("") && sqlType != SqlType.STRING)) {
            return null;
        }

        switch (sqlType) {
            case MAP:
            case ARRAY:
                return objectMapper.readValue(cellValue, fieldType.getTypeClass());
            case STRING:
                return cellValue;
            case DOUBLE:
                return Double.parseDouble(cellValue);
            case BOOLEAN:
                return Boolean.parseBoolean(cellValue);
            case FLOAT:
                return (float) Double.parseDouble(cellValue);
            case BIGINT:
                return (long) Double.parseDouble(cellValue);
            case INT:
                return (int) Double.parseDouble(cellValue);
            case TINYINT:
                return (byte) Double.parseDouble(field.toString());
            case SMALLINT:
                return (short) Double.parseDouble(cellValue);
            case DECIMAL:
                return BigDecimal.valueOf(Double.parseDouble(field.toString()));
            case DATE:
                if (field instanceof LocalDateTime) {
                    return ((LocalDateTime) field).toLocalDate();
                } else if (pluginConfig.hasPath(BaseSourceConfigOptions.DATE_FORMAT.key())) {
                    return LocalDate.parse((String) field, dateFormatter);
                } else if (cellData != null && cellData.getOriginalNumberValue() != null) {
                    BigDecimal originalNumberValue = cellData.getOriginalNumberValue();
                    Date javaDate = DateUtil.getJavaDate(originalNumberValue.doubleValue());
                    return javaDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                } else {
                    return LocalDate.parse(
                            (String) field, DateUtils.matchDateFormatter((String) field));
                }
            case TIME:
                if (field instanceof LocalDateTime) {
                    return ((LocalDateTime) field).toLocalTime();
                } else if (pluginConfig.hasPath(BaseSourceConfigOptions.TIME_FORMAT.key())) {
                    return LocalTime.parse((String) field, timeFormatter);
                } else {
                    return LocalTime.parse(
                            (String) field, TimeUtils.matchTimeFormatter((String) field));
                }
            case TIMESTAMP:
                if (field instanceof LocalDateTime) {
                    return field;
                } else if (pluginConfig.hasPath(BaseSourceConfigOptions.DATETIME_FORMAT.key())) {
                    return LocalDateTime.parse((String) field, dateTimeFormatter);
                } else if (cellData != null && cellData.getOriginalNumberValue() != null) {
                    Date date =
                            DateUtil.getJavaDate(cellData.getOriginalNumberValue().doubleValue());
                    return LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
                } else {
                    return LocalDateTime.parse(
                            (String) field,
                            Objects.requireNonNull(
                                    DateTimeUtils.matchDateTimeFormatter((String) field)));
                }
            case NULL:
                return null;
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
    public void close() throws IOException {}
}
