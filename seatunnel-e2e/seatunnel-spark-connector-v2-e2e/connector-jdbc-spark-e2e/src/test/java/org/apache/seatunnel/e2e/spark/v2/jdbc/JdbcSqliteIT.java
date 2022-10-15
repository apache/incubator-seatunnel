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

package org.apache.seatunnel.e2e.spark.v2.jdbc;

import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.core.starter.config.ConfigBuilder;
import org.apache.seatunnel.e2e.spark.SparkContainer;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Slf4j
public class JdbcSqliteIT extends SparkContainer {
    private String tmpdir;
    private Config config;
    private static final List<List<Object>> TEST_DATASET = generateTestDataset();
    private static final String THIRD_PARTY_PLUGINS_URL = "https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.39.3.0/sqlite-jdbc-3.39.3.0.jar";

    private Connection jdbcConnection;

    private void initTestDb() throws Exception {
        tmpdir = System.getProperty("java.io.tmpdir");
        Class.forName("org.sqlite.JDBC");
        jdbcConnection = DriverManager.getConnection("jdbc:sqlite:" + tmpdir + "test.db", "", "");
        initializeJdbcTable();
        batchInsertData();
    }

    private void initializeJdbcTable() throws Exception {
        URI resource = Objects.requireNonNull(JdbcSqliteIT.class.getResource("/jdbc/init_sql/sqlite_init.conf")).toURI();
        config = new ConfigBuilder(Paths.get(resource)).getConfig();
        CheckConfigUtil.checkAllExists(this.config, "source_table", "sink_table", "type_source_table",
                "type_sink_table", "insert_type_source_table_sql", "check_type_sink_table_sql");

        try {
            Statement statement = jdbcConnection.createStatement();
            statement.execute(config.getString("source_table"));
            statement.execute(config.getString("sink_table"));
            statement.execute(config.getString("type_source_table"));
            statement.execute(config.getString("type_sink_table"));
            statement.execute(config.getString("insert_type_source_table_sql"));
        } catch (SQLException e) {
            throw new RuntimeException("Initializing Sqlite table failed!", e);
        }
    }

    private void batchInsertData() throws SQLException {
        String sql = "insert into source(age, name) values(?, ?)";

        try {
            jdbcConnection.setAutoCommit(false);
            try (PreparedStatement preparedStatement = jdbcConnection.prepareStatement(sql)) {
                for (List row : TEST_DATASET) {
                    preparedStatement.setInt(1, (Integer) row.get(0));
                    preparedStatement.setString(2, (String) row.get(1));
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            }
            jdbcConnection.commit();
        } catch (SQLException e) {
            jdbcConnection.rollback();
            throw e;
        }
    }

    private static List<List<Object>> generateTestDataset() {
        List<List<Object>> rows = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            rows.add(Arrays.asList(i, String.format("test_%s", i)));
        }
        return rows;
    }

    @Test
    public void testJdbcSqliteSourceAndSinkDataType() throws Exception {
        Container.ExecResult execResult = executeSeaTunnelSparkJob("/jdbc/jdbc_sqlite_source_and_sink_datatype.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        master.copyFileFromContainer(Paths.get(SEATUNNEL_HOME, "data", "test.db").toString(), new File(tmpdir + "test.db").toPath().toString());
        checkSinkDataTypeTable();
    }

    private void checkSinkDataTypeTable() throws Exception {
        URI resource = Objects.requireNonNull(JdbcSqliteIT.class.getResource("/jdbc/init_sql/sqlite_init.conf")).toURI();
        config = new ConfigBuilder(Paths.get(resource)).getConfig();
        CheckConfigUtil.checkAllExists(this.config, "source_table", "sink_table", "type_source_table",
                "type_sink_table", "insert_type_source_table_sql", "check_type_sink_table_sql");

        Statement statement = jdbcConnection.createStatement();
        ResultSet resultSet = statement.executeQuery(config.getString("check_type_sink_table_sql"));
        resultSet.next();
        Assertions.assertEquals(resultSet.getInt(1), 2);
    }

    @Test
    public void testJdbcSqliteSourceAndSink() throws IOException, InterruptedException, SQLException {
        Container.ExecResult execResult = executeSeaTunnelSparkJob("/jdbc/jdbc_sqlite_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        master.copyFileFromContainer(Paths.get(SEATUNNEL_HOME, "data", "test.db").toString(), new File(tmpdir + "test.db").toPath().toString());
        // query result
        String sql = "select age, name from sink order by age asc";
        List<List> result = new ArrayList<>();
        try (Statement statement = jdbcConnection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                result.add(Arrays.asList(
                        resultSet.getInt(1),
                        resultSet.getString(2)));
            }
        }
        Assertions.assertIterableEquals(TEST_DATASET, result);
    }

    @AfterEach
    public void closeResource() throws SQLException, IOException {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
        // remove the temp test.db file
        Files.delete(new File(tmpdir + "test.db").toPath());
    }

    @Override
    protected void executeExtraCommands(GenericContainer<?> container) throws IOException, InterruptedException {
        Container.ExecResult extraCommands = container.execInContainer("bash", "-c", "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O " + THIRD_PARTY_PLUGINS_URL);
        Assertions.assertEquals(0, extraCommands.getExitCode());

        Container.ExecResult mkdirCommands = container.execInContainer("bash", "-c", "mkdir -p " + SEATUNNEL_HOME + "/data");
        Assertions.assertEquals(0, mkdirCommands.getExitCode());

        try {
            initTestDb();
            // copy db file to container, dist file path in container is /tmp/seatunnel/data/test.db
            Path path = new File(tmpdir + "test.db").toPath();
            byte[] bytes = Files.readAllBytes(path);
            container.copyFileToContainer(Transferable.of(bytes), Paths.get(SEATUNNEL_HOME, "data", "test.db").toString());
        } catch (Exception e) {
            e.printStackTrace();
            Files.delete(new File(tmpdir + "test.db").toPath());
        }
    }

}
