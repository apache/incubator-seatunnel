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

package org.apache.seatunnel.transform.split;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonAlias;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
public class SplitTransformConfig implements Serializable {
    public static final Option<String> KEY_SEPARATOR =
            Options.key("separator")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The separator to split the field");

    public static final Option<String> KEY_SPLIT_FIELD =
            Options.key("split_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The field to be split");

    public static final Option<List<String>> KEY_OUTPUT_FIELDS =
            Options.key("output_fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription("The result fields after split");

    public static final Option<List<TableTransforms>> MULTI_TABLES =
            Options.key("table_transform")
                    .listType(TableTransforms.class)
                    .noDefaultValue()
                    .withDescription("");

    @Data
    public static class TableTransforms implements Serializable {
        @JsonAlias("table_path")
        private String tablePath;

        @JsonAlias("separator")
        private String separator;

        @JsonAlias("split_field")
        private String splitField;

        @JsonAlias("output_fields")
        private String[] outputFields;
    }

    private String separator;
    private String splitField;
    private String[] outputFields;
    private String[] emptySplits;

    public static SplitTransformConfig of(ReadonlyConfig config) {
        SplitTransformConfig splitTransformConfig = new SplitTransformConfig();
        splitTransformConfig.setSeparator(config.get(KEY_SEPARATOR));
        splitTransformConfig.setSplitField(config.get(KEY_SPLIT_FIELD));
        splitTransformConfig.setOutputFields(config.get(KEY_OUTPUT_FIELDS).toArray(new String[0]));
        splitTransformConfig.setEmptySplits(
                new String[splitTransformConfig.getOutputFields().length]);
        return splitTransformConfig;
    }

    public static SplitTransformConfig of(ReadonlyConfig config, CatalogTable catalogTable) {
        String tablePath = catalogTable.getTableId().toTablePath().getFullName();
        if (null != config.get(MULTI_TABLES)) {
            return config.get(MULTI_TABLES).stream()
                    .filter(tableTransforms -> tableTransforms.getTablePath().equals(tablePath))
                    .findFirst()
                    .map(
                            tableTransforms -> {
                                SplitTransformConfig splitTransformConfig =
                                        new SplitTransformConfig();
                                splitTransformConfig.setSeparator(tableTransforms.getSeparator());
                                splitTransformConfig.setSplitField(tableTransforms.getSplitField());
                                splitTransformConfig.setOutputFields(
                                        tableTransforms.getOutputFields());
                                splitTransformConfig.setEmptySplits(
                                        new String[splitTransformConfig.getOutputFields().length]);
                                return splitTransformConfig;
                            })
                    .orElseGet(() -> of(config));
        }
        return of(config);
    }
}
