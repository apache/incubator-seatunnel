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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.CopyBatchStatementExecutor;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;

import org.postgresql.PGConnection;

import com.google.auto.service.AutoService;
import lombok.Getter;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@AutoService(CopyBatchStatementExecutor.class)
public class PostgresCopyBatchStatementExecutor implements CopyBatchStatementExecutor {

    private static final String COMMA = ",";
    private static final String DOUBLE_QUOTE = Character.valueOf('"').toString();
    private static final char LF = '\n';
    private static final String EMPTY = "";

    protected String copySql;
    @Getter protected TableSchema tableSchema;
    private Connection connection;
    CSVFormat csvFormat =
            CSVFormat.DEFAULT
                    .builder()
                    .setDelimiter(COMMA)
                    .setEscape(null)
                    .setIgnoreEmptyLines(false)
                    .setQuote(null)
                    .setRecordSeparator(LF)
                    .setNullString(EMPTY)
                    .setQuoteMode(QuoteMode.ALL_NON_NULL)
                    .build();
    CSVPrinter csvPrinter;

    @Getter private boolean flushed = true;

    @Override
    public void init(TablePath tablePath, TableSchema tableSchema) {
        JdbcDialect dialect = new PostgresDialect();
        this.tableSchema = tableSchema;
        String tableName = dialect.extractTableName(tablePath);
        String columns =
                Arrays.stream(tableSchema.getFieldNames())
                        .map(dialect::quoteIdentifier)
                        .collect(Collectors.joining(",", "(", ")"));
        this.copySql = String.format("COPY %s %s FROM STDIN WITH CSV", tableName, columns);
    }

    @Override
    public void prepareStatements(Connection connection) {
        try {
            this.connection = connection;
            this.csvPrinter = new CSVPrinter(new StringBuilder(), csvFormat);
        } catch (IOException e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.NO_SUPPORT_OPERATION_FAILED,
                    "unable to open CopyManager Operation in this JDBC writer.",
                    e);
        }
    }

    @Override
    public void addToBatch(SeaTunnelRow record) throws SQLException {
        try {
            this.csvPrinter.printRecord(toExtract(record));
            flushed = false;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected List<Object> toExtract(SeaTunnelRow record) {
        SeaTunnelRowType rowType = tableSchema.toPhysicalRowDataType();
        List<Object> csvRecord = new ArrayList<>();
        for (int fieldIndex = 0; fieldIndex < rowType.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = rowType.getFieldType(fieldIndex);
            Object fieldValue = record.getField(fieldIndex);
            if (fieldValue == null) {
                csvRecord.add(null);
                continue;
            }
            switch (seaTunnelDataType.getSqlType()) {
                case BOOLEAN:
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                case DECIMAL:
                    csvRecord.add(record.getField(fieldIndex));
                    break;
                case DATE:
                    LocalDate localDate = (LocalDate) record.getField(fieldIndex);
                    csvRecord.add(java.sql.Date.valueOf(localDate));
                    break;
                case TIME:
                    LocalTime localTime = (LocalTime) record.getField(fieldIndex);
                    csvRecord.add(java.sql.Time.valueOf(localTime));
                    break;
                case TIMESTAMP:
                    LocalDateTime localDateTime = (LocalDateTime) record.getField(fieldIndex);
                    csvRecord.add(java.sql.Timestamp.valueOf(localDateTime));
                    break;
                case BYTES:
                    StringBuilder hexString = new StringBuilder("\\x");
                    for (byte b : (byte[]) record.getField(fieldIndex)) {
                        hexString.append(String.format("%02x", b));
                    }
                    csvRecord.add(hexString.toString());
                    break;
                case STRING:
                    Object val = record.getField(fieldIndex);
                    if (val != null) {
                        String strVal = val.toString();
                        boolean containsQuote = strVal.contains(DOUBLE_QUOTE);
                        if (strVal.contains(COMMA) || containsQuote) {
                            strVal =
                                    containsQuote
                                            ? strVal.replace(
                                                    DOUBLE_QUOTE, DOUBLE_QUOTE + DOUBLE_QUOTE)
                                            : strVal;
                            strVal = DOUBLE_QUOTE + strVal + DOUBLE_QUOTE;
                        }
                        csvRecord.add(strVal);
                    } else {
                        csvRecord.add(null);
                    }
                    break;
                case NULL:
                    csvRecord.add(null);
                    break;
                case MAP:
                case ARRAY:
                case ROW:
                default:
                    throw new JdbcConnectorException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                            "Unexpected value: " + seaTunnelDataType);
            }
        }
        return csvRecord;
    }

    private long doCopy(String sql, Reader reader) throws SQLException, IOException {
        PGConnection pgConnection = connection.unwrap(PGConnection.class);
        return pgConnection.getCopyAPI().copyIn(sql, reader);
    }

    @Override
    public void executeBatch() throws SQLException {
        try {
            this.csvPrinter.flush();
            doCopy(copySql, new StringReader(this.csvPrinter.getOut().toString()));
        } catch (SQLException | IOException e) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED, "Sql command: " + copySql);
        } finally {
            try {
                this.csvPrinter.close();
                this.csvPrinter = new CSVPrinter(new StringBuilder(), csvFormat);
                flushed = true;
            } catch (Exception ignore) {
            }
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        try {
            this.csvPrinter.close();
            this.csvPrinter = null;
        } catch (Exception ignore) {
        }
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.POSTGRESQL;
    }
}
