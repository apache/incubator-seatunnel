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

package org.apache.seatunnel.connectors.seatunnel.neo4j.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSinkQueryInfo;
import org.apache.seatunnel.connectors.seatunnel.neo4j.internal.SeatunnelRowNeo4jValue;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Query;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class Neo4jSinkWriter implements SinkWriter<SeaTunnelRow, Void, Void> {

    private final Neo4jSinkQueryInfo neo4jSinkQueryInfo;
    private final transient Driver driver;
    private final transient Session session;

    private final SeaTunnelRowType seaTunnelRowType;
    private final List<SeatunnelRowNeo4jValue> writeBuffer;
    private final Integer maxBatchSize;

    public Neo4jSinkWriter(
            Neo4jSinkQueryInfo neo4jSinkQueryInfo, SeaTunnelRowType seaTunnelRowType) {
        this.neo4jSinkQueryInfo = neo4jSinkQueryInfo;
        this.driver = this.neo4jSinkQueryInfo.getDriverBuilder().build();
        this.session =
                driver.session(
                        SessionConfig.forDatabase(
                                neo4jSinkQueryInfo.getDriverBuilder().getDatabase()));
        this.seaTunnelRowType = seaTunnelRowType;
        this.maxBatchSize = Optional.ofNullable(neo4jSinkQueryInfo.getMaxBatchSize()).orElse(0);
        this.writeBuffer = new ArrayList<>(maxBatchSize);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        if (neo4jSinkQueryInfo.batchMode()) {
            writeByBatchSize(element);
        } else {
            writeOneByOne(element);
        }
    }

    private void writeOneByOne(SeaTunnelRow element) {
        final Map<String, Object> queryParamPosition =
                neo4jSinkQueryInfo.getQueryParamPosition().entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        e -> element.getField((Integer) e.getValue())));
        final Query query = new Query(neo4jSinkQueryInfo.getQuery(), queryParamPosition);
        writeByQuery(query);
    }

    private void writeByBatchSize(SeaTunnelRow element) {
        writeBuffer.add(new SeatunnelRowNeo4jValue(seaTunnelRowType, element));
        tryWriteByBatchSize();
    }

    private void tryWriteByBatchSize() {
        while (!writeBuffer.isEmpty() && writeBuffer.size() >= maxBatchSize) {
            Query query = batchQuery();
            writeByQuery(query);
            writeBuffer.clear();
        }
    }

    private Query batchQuery() {
        Value batchValues = Values.parameters(writeBuffer);
        return new Query(neo4jSinkQueryInfo.getQuery(), batchValues);
    }

    private void writeByQuery(Query query) {
        session.writeTransaction(
                tx -> {
                    tx.run(query);
                    return null;
                });
    }

    @Override
    public Optional<Void> prepareCommit() throws IOException {
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        tryWriteByBatchSize();
        session.close();
        driver.close();
    }
}
