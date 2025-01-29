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

package org.apache.seatunnel.common.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Map;

public class EnvConfigParserUtilTest {

    private String originSeatunnelHome = null;
    private DeployMode originMode = null;
    private static final String seatunnelHome;

    static {
        try {
            seatunnelHome =
                    Paths.get(EnvConfigParserUtilTest.class.getResource("/home").toURI())
                            .toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Failed to get seatunnelHome path", e);
        }
    }

    @BeforeEach
    public void before() {
        originMode = Common.getDeployMode();
        Common.setDeployMode(DeployMode.CLIENT);
        originSeatunnelHome = Common.getSeaTunnelHome();
        Common.setSeaTunnelHome(seatunnelHome);
    }

    @Test
    public void testParseEnvConfig() {
        Map<String, String> envMap = EnvConfigParserUtil.parseEnvConfig();
        Assertions.assertEquals(2, envMap.size());
        String os = System.getProperty("os.name").toLowerCase();
        if (os.startsWith("win")) {
            Assertions.assertEquals("C:\\Program Files\\spark", envMap.get("SPARK_HOME"));
            Assertions.assertEquals("C:\\Program Files\\flink", envMap.get("FLINK_HOME"));
            return;
        }
        Assertions.assertEquals("/opt/spark", envMap.get("SPARK_HOME"));
        Assertions.assertEquals("/opt/flink", envMap.get("FLINK_HOME"));
    }

    @AfterEach
    public void after() {
        Common.setSeaTunnelHome(originSeatunnelHome);
        Common.setDeployMode(originMode);
    }
}
