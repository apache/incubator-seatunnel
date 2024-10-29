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

package org.apache.seatunnel.api.table.type;

import org.apache.seatunnel.api.table.catalog.Column;

import lombok.Getter;

/**
 * Common option keys of SeaTunnel {@link Column#getOptions()} / {@link SeaTunnelRow#getOptions()}.
 * Used to store some extra information of the column value.
 */
@Getter
public enum CommonOptions {
    /**
     * The key of {@link Column#getOptions()} to specify the column value is a json format string.
     */
    JSON("Json"),
    /** The key of {@link Column#getOptions()} to specify the column value is a metadata field. */
    METADATA("Metadata"),
    /**
     * The key of {@link SeaTunnelRow#getOptions()} to store the partition value of the row value.
     */
    PARTITION("Partition"),
    ;

    private final String name;

    CommonOptions(String name) {
        this.name = name;
    }
}
