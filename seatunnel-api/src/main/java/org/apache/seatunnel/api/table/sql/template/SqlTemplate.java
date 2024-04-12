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

package org.apache.seatunnel.api.table.sql.template;

import org.apache.seatunnel.api.sink.SaveModePlaceHolderEnum;

import org.apache.commons.lang3.StringUtils;

public class SqlTemplate {

    public static final String EXCEPTION_TEMPLATE =
            "The table of %s has no %s, but the template \n %s \n which has the place holder named %s. Please use the option named %s to specify sql template";

    public static void canHandledByTemplateWithPlaceholder(
            String createTemplate,
            String keyPlaceholder,
            String actualKey,
            String tableName,
            String optionsKey) {
        if (createTemplate.contains(keyPlaceholder) && StringUtils.isBlank(actualKey)) {
            throw new RuntimeException(
                    String.format(
                            EXCEPTION_TEMPLATE,
                            tableName,
                            SaveModePlaceHolderEnum.getKeyValue(keyPlaceholder),
                            createTemplate,
                            keyPlaceholder,
                            optionsKey));
        }
    }
}
