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

package org.apache.seatunnel.transform.common;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Arrays;

public final class CommonOptions {

    public static Option<ErrorHandleWay> ROW_ERROR_HANDLE_WAY_OPTION =
            Options.key("row_error_handle_way")
                    .singleChoice(
                            ErrorHandleWay.class,
                            Arrays.asList(ErrorHandleWay.FAIL, ErrorHandleWay.SKIP))
                    .defaultValue(ErrorHandleWay.FAIL)
                    .withDescription(
                            "The processing method of data format error. The default value is fail, and the optional value is (fail, skip). "
                                    + "When fail is selected, data format error will block and an exception will be thrown. "
                                    + "When skip is selected, data format error will skip this line data.");

    public static Option<ErrorHandleWay> COLUMN_ERROR_HANDLE_WAY_OPTION =
            Options.key("column_error_handle_way")
                    .enumType(ErrorHandleWay.class)
                    .noDefaultValue()
                    .withDescription(
                            "The processing method of data format error. "
                                    + "When fail is selected, data format error will block and an exception will be thrown. "
                                    + "When skip is selected, data format error will skip this column data."
                                    + "When skip_row is selected, data format error will skip this line data.");
}
