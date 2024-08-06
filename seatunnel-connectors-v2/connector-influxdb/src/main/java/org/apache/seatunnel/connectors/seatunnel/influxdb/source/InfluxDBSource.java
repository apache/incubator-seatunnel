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

package org.apache.seatunnel.connectors.seatunnel.influxdb.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.influxdb.config.MultipleTableSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.influxdb.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.influxdb.state.InfluxDBSourceState;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class InfluxDBSource
        implements SeaTunnelSource<SeaTunnelRow, InfluxDBSourceSplit, InfluxDBSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private final MultipleTableSourceConfig multipleTableSourceConfig;

    public InfluxDBSource(ReadonlyConfig readonlyConfig) {
        this.multipleTableSourceConfig = new MultipleTableSourceConfig(readonlyConfig);
    }

    @Override
    public String getPluginName() {
        return "InfluxDB";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader createReader(SourceReader.Context readerContext) throws Exception {
        return new InfluxdbSourceReader(readerContext, multipleTableSourceConfig);
    }

    @Override
    public SourceSplitEnumerator createEnumerator(SourceSplitEnumerator.Context enumeratorContext)
            throws Exception {
        return new InfluxDBSourceSplitEnumerator(enumeratorContext, multipleTableSourceConfig);
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return multipleTableSourceConfig.getSourceConfigs().stream()
                .map(SourceConfig::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public SourceSplitEnumerator<InfluxDBSourceSplit, InfluxDBSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<InfluxDBSourceSplit> enumeratorContext,
            InfluxDBSourceState checkpointState)
            throws Exception {
        return new InfluxDBSourceSplitEnumerator(
                enumeratorContext, checkpointState, multipleTableSourceConfig);
    }
}
