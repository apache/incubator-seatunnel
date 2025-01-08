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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor;

import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import java.util.LinkedList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

public class CopyBatchStatementExecutorFactory {
    public static CopyBatchStatementExecutor create(String dialectOrCompatibleMode) {
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            final List<CopyBatchStatementExecutor> result = new LinkedList<>();
            ServiceLoader.load(CopyBatchStatementExecutor.class, classLoader)
                    .iterator()
                    .forEachRemaining(result::add);
            for (CopyBatchStatementExecutor executor : result) {
                if (executor.dialectName().equalsIgnoreCase(dialectOrCompatibleMode)) {
                    return executor;
                }
            }
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported dialect or compatible mode: %s for copy batch statement executor.",
                            dialectOrCompatibleMode));
        } catch (ServiceConfigurationError e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.NO_SUITABLE_DIALECT_FACTORY,
                    "Could not load service provider for jdbc copy batch statement executor factory.",
                    e);
        }
    }
}
