package org.apache.seatunnel.connectors.seatunnel.file.excel;

import com.alibaba.excel.metadata.Cell;
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

import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;
import com.alibaba.excel.exception.ExcelDataConvertException;
import com.alibaba.excel.metadata.data.ReadCellData;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.common.utils.DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS;

@Slf4j
public class ExcelReaderListener extends AnalysisEventListener<Map<Integer, Object>> {
    private final String tableId;
    private final Collector<SeaTunnelRow> output;
    private final InputStream inputStream;
    private final Map<String, String> partitionsMap;
    private int cellCount;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DateUtils.Formatter dateFormat = DateUtils.Formatter.YYYY_MM_DD;
    private final DateTimeUtils.Formatter datetimeFormat = YYYY_MM_DD_HH_MM_SS;
    private final TimeUtils.Formatter timeFormat = TimeUtils.Formatter.HH_MM_SS;

    protected Config pluginConfig;

    protected SeaTunnelRowType seaTunnelRowType;

    private SeaTunnelDataType<?>[] fieldTypes;

    Map<Integer, String> customHeaders = new HashMap<>();

    public ExcelReaderListener(
            String tableId,
            Collector<SeaTunnelRow> output,
            InputStream inputStream,
            Map<String, String> partitionsMap,
            Config pluginConfig,
            SeaTunnelRowType seaTunnelRowType) {
        this.tableId = tableId;
        this.output = output;
        this.inputStream = inputStream;
        this.partitionsMap = partitionsMap;
        this.pluginConfig = pluginConfig;
        this.seaTunnelRowType = seaTunnelRowType;

        fieldTypes = seaTunnelRowType.getFieldTypes();
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
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(cellCount);
        Map<Integer, Cell> cellMap = context.readRowHolder().getCellMap();

        for (int i = 0; i < cellCount; i++) {
            Object cell = convert(data.get(i),cellMap.get(i), fieldTypes[i]);
            seaTunnelRow.setField(i, cell);
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

    @SneakyThrows
    private Object convert(Object field, Cell  cellRaw, SeaTunnelDataType<?> fieldType) {
        if (field == null) {
            return "";
        }
        SqlType sqlType = fieldType.getSqlType();
        DateTimeFormatter dateTimeFormatter ;
        ReadCellData cellData = (ReadCellData) cellRaw;

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
                }



                dateTimeFormatter = DateUtils.matchDateFormatter((String) field);
                return LocalDate.parse(
                        (String) field,dateTimeFormatter);

            case TIME:
                if (field instanceof LocalDateTime) {
                    return ((LocalDateTime) field).toLocalTime();
                }
                return LocalTime.parse(
                        (String) field, DateTimeFormatter.ofPattern(timeFormat.getValue()));
            case TIMESTAMP:
                if (field instanceof LocalDateTime) {
                    return field;
                }


                String format = cellData.getDataFormatData().getFormat();

//                dateTimeFormatter = DateTimeUtils.matchDateTimeFormatter((String) field);

                return LocalDate.parse(
                        (String) field,DateTimeFormatter.ofPattern(format));


//                return LocalDateTime.parse(
//                        (String) field, DateTimeFormatter.ofPattern("yyyy-M-d HH:mm"));
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
                    seaTunnelRow.setField(j, convert(context[j],null, ft.getFieldType(j)));
                }
                return seaTunnelRow;
            default:
                throw new FileConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        "User defined schema validation failed");
        }
    }
}
