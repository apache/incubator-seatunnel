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
package org.apache.seatunnel.connectors.seatunnel.file.reader;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.XmlReadStrategy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

public class ReadStrategyTest {

    @Test
    public void testXmlRead() throws IOException, URISyntaxException {
        URL xmlFile = XmlReadStrategyTest.class.getResource("/xml/name=xmlTest");
        URL conf = XmlReadStrategyTest.class.getResource("/xml/test_grok_read_xml.conf");
        Assertions.assertNotNull(xmlFile);
        Assertions.assertNotNull(conf);
        String xmlFilePath = Paths.get(xmlFile.toURI()).toString();
        String confPath = Paths.get(conf.toURI()).toString();
        Config pluginConfig = ConfigFactory.parseFile(new File(confPath));
        XmlReadStrategy xmlReadStrategy = new XmlReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        xmlReadStrategy.setPluginConfig(pluginConfig);
        xmlReadStrategy.init(localConf);
        List<String> fileNamesByPath =
                xmlReadStrategy.getFileNamesByPath(xmlFilePath + "/%{NOTSPACE:list}.xml");
        Assertions.assertEquals(1, fileNamesByPath.size());
    }

    public static class LocalConf extends HadoopConf {
        private static final String HDFS_IMPL = "org.apache.hadoop.fs.LocalFileSystem";
        private static final String SCHEMA = "file";

        public LocalConf(String hdfsNameKey) {
            super(hdfsNameKey);
        }

        @Override
        public String getFsHdfsImpl() {
            return HDFS_IMPL;
        }

        @Override
        public String getSchema() {
            return SCHEMA;
        }
    }
}
