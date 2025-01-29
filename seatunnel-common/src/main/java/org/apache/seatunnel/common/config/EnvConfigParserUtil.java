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

import org.apache.seatunnel.common.utils.FileUtils;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class EnvConfigParserUtil {

    public static Map<String, String> parseEnvConfig() {
        Path path = Common.configDir();
        String os = System.getProperty("os.name").toLowerCase();

        if (os.startsWith("win")) {
            String envScript = "seatunnel-env.cmd";
            Path envScriptPath = path.resolve(envScript);
            if (envScriptPath.toFile().exists()) {
                String content = FileUtils.readFileToStr(envScriptPath);
                Map<String, String> config = new HashMap<>();
                String[] lines = content.split("\\r?\\n");
                for (String line : lines) {
                    line = line.trim();
                    if (line.startsWith("if") && line.contains("set")) {
                        int setIndex = line.indexOf("set");
                        if (setIndex != -1) {
                            String assignment = line.substring(setIndex + 4).trim();
                            String[] keyValue = assignment.split("=");
                            if (keyValue.length == 2) {
                                String key = keyValue[0].replace("\"", "").trim();
                                String value = keyValue[1].replace("\"", "").trim();
                                config.put(key, value);
                            }
                        }
                    }
                }
                return config;
            }

        } else {
            String envScript = "seatunnel-env.sh";
            Path envScriptPath = path.resolve(envScript);
            if (envScriptPath.toFile().exists()) {
                String content = FileUtils.readFileToStr(envScriptPath);
                Map<String, String> config = new HashMap<>();
                String[] lines = content.split("\\r?\\n");
                for (String line : lines) {
                    line = line.trim();
                    if (line.contains("=") && !line.startsWith("#")) {
                        String[] keyValue = line.split("=", 2);
                        if (keyValue.length == 2) {
                            String key = keyValue[0].trim();
                            String value = keyValue[1].trim();
                            if (value.startsWith("${") && value.contains(":-")) {
                                value =
                                        value.substring(
                                                value.indexOf(":-") + 2, value.length() - 1);
                            }
                            config.put(key, value);
                        }
                    }
                }
                return config;
            }
        }
        return null;
    }
}
