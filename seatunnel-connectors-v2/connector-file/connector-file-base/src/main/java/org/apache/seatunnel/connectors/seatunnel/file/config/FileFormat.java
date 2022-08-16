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

package org.apache.seatunnel.connectors.seatunnel.file.config;

import org.apache.seatunnel.connectors.seatunnel.file.source.reader.OrcReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ParquetReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.TextReadStrategy;

import java.io.Serializable;

public enum FileFormat implements Serializable {
    CSV("csv") {
        @Override
        public ReadStrategy getReadStrategy() {
            return new TextReadStrategy();
        }
    },
    TEXT("txt") {
        @Override
        public ReadStrategy getReadStrategy() {
            return new TextReadStrategy();
        }
    },
    PARQUET("parquet") {
        @Override
        public ReadStrategy getReadStrategy() {
            return new ParquetReadStrategy();
        }
    },
    ORC("orc") {
        @Override
        public ReadStrategy getReadStrategy() {
            return new OrcReadStrategy();
        }
    },
    JSON("json") {
        @Override
        public ReadStrategy getReadStrategy() {
            return new TextReadStrategy();
        }
    };

    private final String suffix;

    FileFormat(String suffix) {
        this.suffix = suffix;
    }

    public String getSuffix() {
        return "." + suffix;
    }

    public ReadStrategy getReadStrategy() {
        return null;
    }
}
