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

package ai.hunters.spark.sql.streaming.path

import java.time.{LocalDate, ZoneId}

import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

class DynamicPathGeneratorITest extends FlatSpec with GivenWhenThen with Matchers {
  case class TestJson(date: String, purpose: String)

  "end-to-end" should "read text from a wildcarded pattern path" in {
    Given("Dynamic Path Generator with date wildcards")
    val pathGenerator =
      new DynamicPathGenerator(
        "src/test/resources/dynamic_path_read/{YYYY}/{MM}/{DD}",
        DynamicPathGeneratorITest.TEST_MAX_NUMBER_OF_DAYS_TO_READ,
        DynamicPathGeneratorITest.TEST_TIMEZONE)
    When("Spark read test data for test date " + DynamicPathGeneratorITest.TEST_CURRENT_DATE)
    val generatedPaths = pathGenerator.getPaths(DynamicPathGeneratorITest.TEST_CURRENT_DATE)
    val sparkConfig = new SparkConf()
      .set("spark.driver.host", "localhost")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.master", "local[*]")
    val spark = SparkSession.builder.config(sparkConfig).getOrCreate()
    val readedJsons = spark.read
      .option("checkpointLocation", s"target/tmp/DynamicPathGeneratorITest/checkpoint")
      .json(generatedPaths.toArray: _*)
    val driverResult = readedJsons
      .collect()
      .map({ case Row(date: String, purpose: String) => TestJson(date, purpose) })
    Then("Spark should return exactly 2 records")
    driverResult.length shouldBe 2
    And("Their dates should be 29 and 30.12.2021")
    driverResult
      .filterNot(testJson => testJson.date == "2021-12-30")
      .filterNot(testJson => testJson.date == "2021-12-29") shouldBe empty
  }
}

object DynamicPathGeneratorITest {
  val TEST_MAX_NUMBER_OF_DAYS_TO_READ: Int = 2
  private val TEST_TIMEZONE: ZoneId = ZoneId.of("UTC")
  private val TEST_CURRENT_DATE: LocalDate = LocalDate.of(2021, 12, 30)
}
