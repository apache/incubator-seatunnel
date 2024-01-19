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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@Slf4j
public class SQLUtils {

    public static Long countForSubquery(Connection connection, String subQuerySQL)
            throws SQLException {
        String sqlQuery = String.format("SELECT COUNT(*) FROM (%s) T", subQuerySQL);
        log.info("Split Chunk, countForSubquery: {}", sqlQuery);
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery(sqlQuery)) {
                if (resultSet.next()) {
                    return resultSet.getLong(1);
                }
                throw new SQLException(
                        String.format("No result returned after running query [%s]", sqlQuery));
            }
        }
    }

    public static Long countForTable(Connection connection, String tablePath) throws SQLException {
        String sqlQuery = String.format("SELECT COUNT(*) FROM %s", tablePath);
        log.info("Split Chunk, countForTable: {}", sqlQuery);
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery(sqlQuery)) {
                if (resultSet.next()) {
                    return resultSet.getLong(1);
                }
                throw new SQLException(
                        String.format("No result returned after running query [%s]", sqlQuery));
            }
        }
    }
}
