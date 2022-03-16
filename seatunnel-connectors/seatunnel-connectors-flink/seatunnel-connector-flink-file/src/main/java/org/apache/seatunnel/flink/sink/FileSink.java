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

package org.apache.seatunnel.flink.sink;

import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.utils.StringTemplate;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.enums.FormatType;
import org.apache.seatunnel.flink.stream.FlinkStreamSink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSink implements FlinkStreamSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSink.class);

    private static final long serialVersionUID = -1648045076508797396L;

    private static final String PATH = "path";
    private static final String FORMAT = "format";
    private static final String WRITE_MODE = "write_mode";
    private static final String PARALLELISM = "parallelism";
    private static final String PATH_TIME_FORMAT = "path_time_format";
    private static final String DEFAULT_TIME_FORMAT = "yyyyMMddHHmmss";
    private Config config;

    private FileOutputFormat<Row> outputFormat;

    private Path filePath;

    @Override
    public void outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        FormatType format = FormatType.from(config.getString(FORMAT).trim().toLowerCase());
        switch (format) {
            case JSON:
                RowTypeInfo rowTypeInfo = (RowTypeInfo) dataStream.getType();
                outputFormat = new JsonRowOutputFormat(filePath, rowTypeInfo);
                break;
            case CSV:
                outputFormat = new CsvRowOutputFormat(filePath);
                break;
            case TEXT:
                outputFormat = new TextOutputFormat<>(filePath);
                break;
            default:
                LOGGER.warn(" unknown file_format [{}],only support json,csv,text", format);
                break;
        }
        if (config.hasPath(WRITE_MODE)) {
            String mode = config.getString(WRITE_MODE);
            outputFormat.setWriteMode(FileSystem.WriteMode.valueOf(mode));
        }
        DataStreamSink<Row> rowDataStreamSink = dataStream.addSink(new FileSinkFunction<>(outputFormat));
        if (config.hasPath(PARALLELISM)) {
            int parallelism = config.getInt(PARALLELISM);
            rowDataStreamSink.setParallelism(parallelism);
        }
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return CheckConfigUtil.checkAllExists(config, PATH, FORMAT);
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        String format = config.hasPath(PATH_TIME_FORMAT) ? config.getString(PATH_TIME_FORMAT) : DEFAULT_TIME_FORMAT;
        String path = StringTemplate.substitute(config.getString(PATH), format);
        filePath = new Path(path);
    }
}
