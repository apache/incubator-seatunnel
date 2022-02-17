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
package org.apache.seatunnel.spark.source

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

import org.apache.seatunnel.common.config.{CheckResult, TypesafeConfigUtils}
import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.spark.Config._
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSource
import org.apache.spark.sql.{Dataset, Row, SparkSession}

abstract class FileSourceBase extends SparkBatchSource {

  override def prepare(env: SparkEnvironment): Unit = {}

  def getDataImpl(env: SparkEnvironment, schema: String): Dataset[Row] = {
    val path = buildPathWithDefaultSchema(config.getString(PATH), schema)
    fileReader(env.getSparkSession, path)
  }

  def checkConfigImpl(schema: String): CheckResult = {
    val checkResult = checkAllExists(config, PATH)
    if (checkResult.isSuccess) {
      val dir = config.getString(PATH)
      dir.startsWith("/") || dir.startsWith(schema) match {
        case false =>
          CheckResult.error("invalid path URI, please set the following allowed schemas: " + schema)
        case _ => checkResult
      }
    } else {
      checkResult
    }
  }

  protected def fileReader(spark: SparkSession, path: String): Dataset[Row] = {
    val format = config.getString(FORMAT)
    var reader = spark.read.format(format)

    Try(TypesafeConfigUtils.extractSubConfigThrowable(config, "options.", false)) match {

      case Success(options) => {
        val optionMap = options
          .entrySet()
          .foldRight(Map[String, String]())((entry, m) => {
            m + (entry.getKey -> entry.getValue.unwrapped().toString)
          })

        reader = reader.options(optionMap)
      }
      case Failure(_) =>
    }

    format match {
      case TEXT => reader.load(path).withColumnRenamed("value", "raw_message")
      case PARQUET => reader.parquet(path)
      case JSON => reader.json(path)
      case ORC => reader.orc(path)
      case CSV => reader.csv(path)
      case _ => reader.format(format).load(path)
    }
  }

  protected def buildPathWithDefaultSchema(uri: String, defaultUriSchema: String): String = {
    val path = uri.startsWith("/") match {
      case true => defaultUriSchema + uri
      case false => uri
    }
    path
  }

}
