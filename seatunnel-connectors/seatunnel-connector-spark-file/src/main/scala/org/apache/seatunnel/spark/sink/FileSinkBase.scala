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
package org.apache.seatunnel.spark.sink

import java.util

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

import org.apache.seatunnel.common.config.{CheckResult, TypesafeConfigUtils}
import org.apache.seatunnel.common.utils.StringTemplate
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.Config.{CSV, JSON, ORC, PARQUET, PARTITION_BY, PATH, PATH_TIME_FORMAT, SAVE_MODE, SERIALIZER, TEXT}
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.sql.{Dataset, Row}

abstract class FileSinkBase extends SparkBatchSink {

  def checkConfigImpl(allowedURISchema: List[String]): CheckResult = {
    config.hasPath(PATH) && !config.getString(PATH).trim.isEmpty match {
      case true =>
        val dir = config.getString(PATH)

        dir.startsWith("/") || uriInAllowedSchema(dir, allowedURISchema) match {
          case true => CheckResult.success()
          case false =>
            CheckResult.error(
              "invalid path URI, please set the following allowed schemas: " + allowedURISchema.mkString(
                ", "))
        }
      case false => CheckResult.error("please specify [path] as non-empty string")
    }
  }

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        PARTITION_BY -> util.Arrays.asList(),
        SAVE_MODE -> "error", // allowed values: overwrite, append, ignore, error
        SERIALIZER -> "json", // allowed values: csv, json, parquet, text
        PATH_TIME_FORMAT -> "yyyyMMddHHmmss" // if variable 'now' is used in path, this option specifies its time_format
      ))
    config = config.withFallback(defaultConfig)
  }

  /**
   * check if the schema name in this uri is allowed.
   * @return true if schema name is allowed
   */
  protected def uriInAllowedSchema(uri: String, allowedURISchema: List[String]): Boolean = {
    val notAllowed = allowedURISchema.forall(schema => {
      !uri.startsWith(schema)
    })
    !notAllowed
  }

  protected def buildPathWithDefaultSchema(uri: String, defaultUriSchema: String): String = {
    val path = uri.startsWith("/") match {
      case true => defaultUriSchema + uri
      case false => uri
    }
    path
  }

  def outputImpl(df: Dataset[Row], defaultUriSchema: String): Unit = {
    var writer = df.write.mode(config.getString(SAVE_MODE))
    writer = config.getStringList(PARTITION_BY).length == 0 match {
      case true => writer
      case false =>
        val partitionKeys = config.getStringList(PARTITION_BY)
        writer.partitionBy(partitionKeys: _*)
    }

    Try(TypesafeConfigUtils.extractSubConfigThrowable(config, "options.", false)) match {

      case Success(options) =>
        val optionMap = options
          .entrySet()
          .foldRight(Map[String, String]())((entry, m) => {
            m + (entry.getKey -> entry.getValue.unwrapped().toString)
          })

        writer.options(optionMap)
      case Failure(_) => // do nothing
    }

    var path = buildPathWithDefaultSchema(config.getString(PATH), defaultUriSchema)
    path = StringTemplate.substitute(path, config.getString(PATH_TIME_FORMAT))
    config.getString(SERIALIZER) match {
      case CSV => writer.csv(path)
      case JSON => writer.json(path)
      case PARQUET => writer.parquet(path)
      case TEXT => writer.text(path)
      case ORC => writer.orc(path)
    }
  }
}
