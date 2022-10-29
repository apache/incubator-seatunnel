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

package org.apache.seatunnel.connectors.seatunnel.cassandra.source;

import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraConfig.CQL;
import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraConfig.HOST;
import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraConfig.KEYSPACE;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.cassandra.client.CassandraClient;
import org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraConfig;
import org.apache.seatunnel.connectors.seatunnel.cassandra.state.CassandraSourceState;
import org.apache.seatunnel.connectors.seatunnel.cassandra.util.TypeConvertUtil;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.auto.service.AutoService;

@AutoService(SeaTunnelSource.class)
public class CassandraSource implements SeaTunnelSource<SeaTunnelRow, CassandraSourceSplit, CassandraSourceState> {

    private SeaTunnelRowType rowTypeInfo;
    private CassandraConfig cassandraConfig;

    @Override
    public String getPluginName() {
        return "Cassandra";
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        this.cassandraConfig = CassandraConfig.getCassandraConfig(config);
        CheckResult checkResult = CheckConfigUtil.checkAllExists(config, HOST, KEYSPACE, CQL);
        if (!checkResult.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, checkResult.getMsg());
        }
        // try {
        CqlSession currentSession = CassandraClient.getCqlSessionBuilder(
            cassandraConfig.getHost(),
            cassandraConfig.getKeyspace(),
            cassandraConfig.getUsername(),
            cassandraConfig.getPassword(),
            cassandraConfig.getDatacenter()
        ).build();
        Row rs = currentSession.execute(CassandraClient.createSimpleStatement(cassandraConfig.getCql(), cassandraConfig.getConsistencyLevel())).one();
        if (rs == null) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, "No data in the table!");
        }
        int columnSize = rs.getColumnDefinitions().size();
        String[] fieldNames = new String[columnSize];
        SeaTunnelDataType<?>[] seaTunnelDataTypes = new SeaTunnelDataType[columnSize];
        for (int i = 0; i < columnSize; i++) {
            fieldNames[i] = rs.getColumnDefinitions().get(i).getName().asInternal();
            seaTunnelDataTypes[i] = TypeConvertUtil.convert(rs.getColumnDefinitions().get(i).getType());
        }
        this.rowTypeInfo = new SeaTunnelRowType(fieldNames, seaTunnelDataTypes);
        // } catch (Exception e) {
        //     throw new PrepareFailException(getPluginName(), PluginType.SOURCE, e.getMessage());
        // }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.rowTypeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, CassandraSourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new CassandraSourceReader(cassandraConfig, readerContext);
    }

    @Override
    public SourceSplitEnumerator<CassandraSourceSplit, CassandraSourceState> createEnumerator(SourceSplitEnumerator.Context<CassandraSourceSplit> enumeratorContext) throws Exception {
        return new CassandraSourceSplitEnumerator(enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<CassandraSourceSplit, CassandraSourceState> restoreEnumerator(SourceSplitEnumerator.Context<CassandraSourceSplit> enumeratorContext, CassandraSourceState checkpointState) throws Exception {
        return new CassandraSourceSplitEnumerator(enumeratorContext);
    }

}
