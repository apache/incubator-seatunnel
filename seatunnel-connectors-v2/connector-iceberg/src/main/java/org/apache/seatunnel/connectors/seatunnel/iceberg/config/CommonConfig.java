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

package org.apache.seatunnel.connectors.seatunnel.iceberg.config;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

@Getter
@ToString
public class CommonConfig implements Serializable {
    private static final long serialVersionUID = 239821141534421580L;

    public static final Option<String> KEY_CATALOG_NAME = Options.key("catalog_name")
        .stringType()
        .noDefaultValue()
        .withDescription(" the iceberg catalog name");

    public static final Option<String> KEY_CATALOG_TYPE = Options.key("catalog_type")
        .stringType()
        .noDefaultValue()
        .withDescription(" the iceberg catalog type");

    public static final Option<String> KEY_NAMESPACE = Options.key("namespace")
        .stringType()
        .noDefaultValue()
        .withDescription(" the iceberg namespace");

    public static final Option<String> KEY_TABLE = Options.key("table")
        .stringType()
        .noDefaultValue()
        .withDescription(" the iceberg table");

    public static final Option<String> KEY_URI = Options.key("uri")
        .stringType()
        .noDefaultValue()
        .withDescription(" the iceberg server uri");

    public static final Option<String> KEY_WAREHOUSE = Options.key("warehouse")
        .stringType()
        .noDefaultValue()
        .withDescription(" the iceberg warehouse");

    public static final Option<String> KEY_CASE_SENSITIVE = Options.key("case_sensitive")
        .stringType()
        .noDefaultValue()
        .withDescription(" the iceberg case_sensitive");

    public static final Option<List<String>> KEY_FIELDS = Options.key("fields")
        .listType()
        .noDefaultValue()
        .withDescription(" the iceberg table fields");

    public static final String CATALOG_TYPE_HADOOP = "hadoop";
    public static final String CATALOG_TYPE_HIVE = "hive";

    private String catalogName;
    private String catalogType;
    private String uri;
    private String warehouse;
    private String namespace;
    private String table;
    private boolean caseSensitive;

    public CommonConfig(Config pluginConfig) {
        String catalogType = checkArgumentNotNull(pluginConfig.getString(KEY_CATALOG_TYPE.key()));
        checkArgument(CATALOG_TYPE_HADOOP.equals(catalogType)
                || CATALOG_TYPE_HIVE.equals(catalogType),
            "Illegal catalogType: " + catalogType);

        this.catalogType = catalogType;
        this.catalogName = checkArgumentNotNull(pluginConfig.getString(KEY_CATALOG_NAME.key()));
        if (pluginConfig.hasPath(KEY_URI.key())) {
            this.uri = checkArgumentNotNull(pluginConfig.getString(KEY_URI.key()));
        }
        this.warehouse = checkArgumentNotNull(pluginConfig.getString(KEY_WAREHOUSE.key()));
        this.namespace = checkArgumentNotNull(pluginConfig.getString(KEY_NAMESPACE.key()));
        this.table = checkArgumentNotNull(pluginConfig.getString(KEY_TABLE.key()));

        if (pluginConfig.hasPath(KEY_CASE_SENSITIVE.key())) {
            this.caseSensitive = pluginConfig.getBoolean(KEY_CASE_SENSITIVE.key());
        }
    }

    protected <T> T checkArgumentNotNull(T argument) {
        checkNotNull(argument);
        return argument;
    }
}
