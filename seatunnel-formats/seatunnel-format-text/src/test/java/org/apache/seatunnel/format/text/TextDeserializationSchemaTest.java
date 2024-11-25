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

package org.apache.seatunnel.format.text;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class TextDeserializationSchemaTest {

    @Test
    public void testSplitLineWithCSV() {
        // prepare test data
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"city", "order_no", "weight"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                        });

        TextDeserializationSchema schema =
                TextDeserializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter(",")
                        .build();

        // Test normal CSV line
        String normalLine = "Beijing,123456,100";
        Map<Integer, String> normalResult =
                schema.splitLineBySeaTunnelRowType(normalLine, rowType, 0);
        Assertions.assertEquals("Beijing", normalResult.get(0));
        Assertions.assertEquals("123456", normalResult.get(1));
        Assertions.assertEquals("100", normalResult.get(2));

        // Test CSV line with quotes and commas
        String quotedLine = "Shanghai,\"123,456,789\",200";
        Map<Integer, String> quotedResult =
                schema.splitLineBySeaTunnelRowType(quotedLine, rowType, 0);
        Assertions.assertEquals("Shanghai", quotedResult.get(0));
        Assertions.assertEquals("123,456,789", quotedResult.get(1));
        Assertions.assertEquals("200", quotedResult.get(2));

        // Test CSV line with empty fields
        String emptyFieldLine = "Guangzhou,,300";
        Map<Integer, String> emptyResult =
                schema.splitLineBySeaTunnelRowType(emptyFieldLine, rowType, 0);
        Assertions.assertEquals("Guangzhou", emptyResult.get(0));
        Assertions.assertEquals("", emptyResult.get(1));
        Assertions.assertEquals("300", emptyResult.get(2));

        // Test line with insufficient fields
        String insufficientLine = "Shenzhen,400";
        Map<Integer, String> insufficientResult =
                schema.splitLineBySeaTunnelRowType(insufficientLine, rowType, 0);
        Assertions.assertEquals("Shenzhen", insufficientResult.get(0));
        Assertions.assertEquals("400", insufficientResult.get(1));
        Assertions.assertNull(insufficientResult.get(2));

        // Test line with quotes inside quotes
        String doubleQuotedLine = "Tianjin,\"123\"\"456\",500";
        Map<Integer, String> doubleQuotedResult =
                schema.splitLineBySeaTunnelRowType(doubleQuotedLine, rowType, 0);
        Assertions.assertEquals("Tianjin", doubleQuotedResult.get(0));
        Assertions.assertEquals("123\"456", doubleQuotedResult.get(1));
        Assertions.assertEquals("500", doubleQuotedResult.get(2));
    }

    @Test
    public void testSplitLineWithCSVFile() throws Exception {
        // prepare test data
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"city", "order_no", "weight"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                        });

        TextDeserializationSchema schema =
                TextDeserializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter(",")
                        .build();

        // Read the CSV file
        List<String> lines = Files.readAllLines(Paths.get("src/test/resources/testdata.csv"));

        // Skip the header line
        lines = lines.subList(1, lines.size());

        for (String line : lines) {
            Map<Integer, String> result = schema.splitLineBySeaTunnelRowType(line, rowType, 0);
            // Verify the result is not null and contains all expected keys
            Assertions.assertNotNull(result);
            Assertions.assertEquals(3, result.size(), "Should contain 3 fields");
            Assertions.assertTrue(result.containsKey(0), "Should contain city field");
            Assertions.assertTrue(result.containsKey(1), "Should contain order_no field");
            Assertions.assertTrue(result.containsKey(2), "Should contain weight field");

            // Verify the values are not null
            Assertions.assertNotNull(result.get(0), "City should not be null");
            Assertions.assertNotNull(result.get(1), "Order number should not be null");
            Assertions.assertNotNull(result.get(2), "Weight should not be null");

            // Verify weight is a valid integer
            Assertions.assertDoesNotThrow(
                    () -> Integer.parseInt(result.get(2)), "Weight should be a valid integer");
        }
    }

    @Test
    public void testSplitLineWithNonCSV() {
        // Test non-CSV format (using other delimiters)
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"city", "info", "date"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
                        });

        TextDeserializationSchema schema =
                TextDeserializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter("|")
                        .build();

        String line = "Beijing|123|2024-01-01";
        Map<Integer, String> result = schema.splitLineBySeaTunnelRowType(line, rowType, 0);
        Assertions.assertEquals("Beijing", result.get(0));
        Assertions.assertEquals("123", result.get(1));
        Assertions.assertEquals("2024-01-01", result.get(2));
    }

    @Test
    public void testSplitLineWithInvalidInput() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"field1", "field2", "field3"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
                        });

        TextDeserializationSchema schema =
                TextDeserializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter(",")
                        .build();
        // Test empty input
        String emptyLine = "";
        Map<Integer, String> emptyResult =
                schema.splitLineBySeaTunnelRowType(emptyLine, rowType, 0);
        Assertions.assertNotNull(emptyResult, "Empty result should not be null");
        Assertions.assertFalse(
                emptyResult.isEmpty()
                        || (emptyResult.get(0) != null && emptyResult.get(0).isEmpty()));
    }
}
