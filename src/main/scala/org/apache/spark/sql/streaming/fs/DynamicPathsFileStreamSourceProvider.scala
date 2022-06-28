/*
 * Copyright 2021 and onwards Hunters.ai.
 *
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

package org.apache.spark.sql.streaming.fs

import java.time.ZoneId

import ai.hunters.spark.sql.streaming.path.DynamicPathGenerator

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

class DynamicPathsFileStreamSourceProvider
    extends DataSourceRegister
    with StreamSourceProvider
    with Logging {

  override def shortName(): String = DynamicPathsFileStreamSourceProvider.FORMAT_SHORT_NAME

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {

    require(schema.isDefined, "DynamicFileStream source doesn't support empty schema")
    (shortName(), schema.get)
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    val pathParameter = parameters.get(DynamicPathsFileStreamSourceProvider.PATH_PARAMETER_NAME)
    require(
      pathParameter.isDefined,
      s"You have to define ${DynamicPathsFileStreamSourceProvider.PATH_PARAMETER_NAME}" +
        s". E.g. s3://example-bucket/data/{YYYY}/{MM}/{DD}/")
    val fileFormat =
      parameters.get(DynamicPathsFileStreamSourceProvider.FILE_FORMAT_PARAMETER_NAME)
    require(
      fileFormat.isDefined,
      s"You have to define ${DynamicPathsFileStreamSourceProvider.FILE_FORMAT_PARAMETER_NAME}" +
        s". E.g. json, parquet or csv.")
    val maxNumberOfDaysToRead =
      parameters
        .getOrElse(
          DynamicPathsFileStreamSourceProvider.MAX_NUMBER_OF_DAYS_TO_READ_PARAMETER_NAME,
          DynamicPathsFileStreamSourceProvider.DEFAULT_MAX_NUMBER_OF_DAYS_TO_READ)
        .toInt
    val sourcePathsTimezone =
      ZoneId.of(
        parameters.getOrElse(
          DynamicPathsFileStreamSourceProvider.SOURCE_PATHS_TIMEZONE_PARAMETER_NAME,
          DynamicPathsFileStreamSourceProvider.DEFAULT_SOURCE_PATHS_TIMEZONE))
    new DynamicPathsFileStreamSource(
      sqlContext.sparkSession,
      new DynamicPathGenerator(pathParameter.get, maxNumberOfDaysToRead, sourcePathsTimezone),
      fileFormat.get,
      schema.get,
      Seq(),
      metadataPath,
      parameters)
  }
}

object DynamicPathsFileStreamSourceProvider {
  val PATH_PARAMETER_NAME = "path"
  val FILE_FORMAT_PARAMETER_NAME = "fileFormat"
  val MAX_NUMBER_OF_DAYS_TO_READ_PARAMETER_NAME =
    "dynamicPathsFileStreamSource.maxNumberOfDaysToRead"
  val SOURCE_PATHS_TIMEZONE_PARAMETER_NAME = "sourcePathsTimezone"
  val DEFAULT_MAX_NUMBER_OF_DAYS_TO_READ = "2"
  val DEFAULT_SOURCE_PATHS_TIMEZONE = "UTC"
  val FORMAT_SHORT_NAME = "dynamic-paths-file"
}
