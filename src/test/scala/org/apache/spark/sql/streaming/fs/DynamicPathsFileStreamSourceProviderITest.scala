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

import java.io.{File, PrintWriter}
import java.nio.file.Files
import java.time.{LocalDate, ZoneId}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.concurrent.duration.DurationInt
import scala.reflect.io.Directory

import io.circe.{jawn, Decoder}
import io.circe.generic.semiauto.deriveDecoder
import nl.altindag.log.LogCaptor
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import org.scalatest.concurrent.Eventually

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.FileScanRDD
import org.apache.spark.sql.execution.streaming.MicroBatchExecution
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{LongType, StructField, StructType}

class DynamicPathsFileStreamSourceProviderITest
    extends FlatSpec
    with GivenWhenThen
    with Matchers
    with Eventually {
  private val logCaptorFileScanRDD: LogCaptor = LogCaptor.forClass(classOf[FileScanRDD])
  private val logCaptorMicroBatchExecution: LogCaptor =
    LogCaptor.forClass(classOf[MicroBatchExecution])
  private val logCaptorDynamicPathsFileStreamSource: LogCaptor =
    LogCaptor.forClass(classOf[DynamicPathsFileStreamSource])
  private val sourcePath = "target/tmp/tests/dynamic_path_read"
  private val outputPath = "target/tmp/tests/dynamic_path_write"
  private val outputDataPath = s"$outputPath/output"

  "end-to-end dynamic-paths-file" should "work with Spark Streaming" in {
    clearLogCaptorLoggers()
    Given("Empty source path")
    getDirectory(sourcePath).deleteRecursively()
    Given("empty output path")
    getDirectory(outputPath).deleteRecursively()
    Given("Created files structured in date paths")
    val now =
      LocalDate.now(ZoneId.of(DynamicPathsFileStreamSourceProvider.DEFAULT_SOURCE_PATHS_TIMEZONE))
    createJsonFileToRead(now, 0, 0)
    createJsonFileToRead(now, 1, 1)
    createJsonFileToRead(now, 2, 2)
    Given("Definition of reading of json data from file with SparkStreaming")
    val sparkConfig = new SparkConf()
      .set("spark.driver.host", "localhost")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.master", "local[*]")
    val spark = SparkSession.builder.config(sparkConfig).getOrCreate()
    val readFileJsonStream = spark.readStream
      .format(DynamicPathsFileStreamSourceProvider.FORMAT_SHORT_NAME)
      .option(DynamicPathsFileStreamSourceProvider.FILE_FORMAT_PARAMETER_NAME, "json")
      .schema(StructType(Seq(StructField("id", LongType, nullable = false))))
      .load(sourcePath + "/{YYYY}/{MM}/{DD}")
    When("start spark streaming")
    val readSaver = readFileJsonStream.writeStream
      .option("checkpointLocation", s"$outputPath/checkpoint")
      .format("json")
      .trigger(Trigger.ProcessingTime(1.second))
    val saverOne = readSaver.start(outputDataPath)
    try {
      Then(
        "should read only paths that are in the scope " +
          "of Dynamic Path Generator (today and yesterday)")
      eventually(timeout(10.seconds), interval(1.second)) {
        val firstSparkLogs =
          logCaptorFileScanRDD.getLogs.asScala.filter(log => log.contains("Reading File path"))
        firstSparkLogs.size >= 2 shouldBe true
        firstSparkLogs
          .filterNot(_.contains(getDatePath(now)))
          .filterNot(_.contains(getDatePath(now.minusDays(1)))) shouldBe empty
      }
      And("batch should be finished in 5 seconds")
      eventually(timeout(5.seconds), interval(1.second)) {
        val microBatchLogs = logCaptorMicroBatchExecution.getLogs.asScala
            .filter(log => log.contains("Streaming query made progress"))
        microBatchLogs.size == 1 shouldBe true
      }
      When("new file is copied into existing current path")
      createJsonFileToRead(now, 0, 3)
      Then("the file is read and saved to the output in max 5s")
      eventually(timeout(5.seconds), interval(1.second)) {
        readAllOutputTestJsons.contains(TestJsonWithId(3)) shouldBe true
      }
      When("spark application is stopped")
    } finally saverOne.stop()
    Then("the application stops in 5 seconds")
    eventually(timeout(5.seconds), interval(500.milliseconds)) {
      saverOne.isActive shouldBe false
    }
    Then("checkpoint is saved")
    And("result are present on the output path")
    val resultsAfterStopSaverOne = readAllOutputTestJsons
    resultsAfterStopSaverOne.length shouldBe 3
    When("spark application is started again")
    clearLogCaptorLoggers()
    val saverTwo = readSaver.start(outputDataPath)
    try {
      Then("Checkpoint is loaded")
      eventually(timeout(5.seconds), interval(500.milliseconds)) {
        logCaptorMicroBatchExecution.getInfoLogs.asScala.exists(
          _.contains("Resuming at batch 2 with committed offsets")) shouldBe true
      }
      And("there is nothing to read, because it started from checkpoint")
      eventually(timeout(5.seconds), interval(500.milliseconds)) {
        logCaptorMicroBatchExecution.getDebugLogs.asScala.exists(
          _.contains("latestOffset took")) shouldBe true
      }
      logCaptorFileScanRDD.getInfoLogs shouldBe empty
      logCaptorMicroBatchExecution.getDebugLogs.asScala.exists(
        _.contains("New file")) shouldBe false
      resultsAfterStopSaverOne shouldBe readAllOutputTestJsons
      When("new file comes to a new (current) path")
      createJsonFileToRead(now, 0, 4)
      Then("the new file is read (and saved to the output)")
      eventually(timeout(5.seconds), interval(1.second)) {
        readAllOutputTestJsons.contains(TestJsonWithId(3)) shouldBe true
      }
    } finally saverTwo.stop()
  }

  private def clearLogCaptorLoggers(): Unit = {
    logCaptorMicroBatchExecution.clearLogs()
    logCaptorFileScanRDD.clearLogs()
    logCaptorDynamicPathsFileStreamSource.clearLogs()
  }

  private def readAllOutputTestJsons =
    new File(outputDataPath)
      .listFiles((_: File, name: String) => name.endsWith(".json"))
      .flatMap(file => Files.readAllLines(file.toPath).asScala)
      .flatMap(line => jawn.decode[TestJsonWithId](line).toOption)

  def createFile(path: String, id: Long): Unit = {
    new File(path).mkdirs()
    val writer = new PrintWriter(new File(s"$path/$id.json"))
    try writer.write(s"{'id': $id}")
    finally writer.close()
  }

  private def createJsonFileToRead(now: LocalDate, minusDays: Int, id: Long): Unit =
    createFile(s"$sourcePath/${getDatePath(now.minusDays(minusDays))}", id)

  private def getDatePath(now: LocalDate) =
    s"${now.getYear}/${twoDigit(now.getMonthValue)}/${twoDigit(now.getDayOfMonth)}"

  private def twoDigit(number: Int) = {
    require(number.abs < 100)
    if (number.abs < 10) {
      "0" + number
    } else {
      number.toString
    }
  }

  def getDirectory(path: String): Directory =
    new Directory(new File(path))

  case class TestJsonWithId(id: Long)

  implicit private val decoder: Decoder[TestJsonWithId] = deriveDecoder[TestJsonWithId]
}
