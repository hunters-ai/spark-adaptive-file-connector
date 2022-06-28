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

import DynamicPathGenerator.{
  DAY_WILDCARD,
  MONTH_WILDCARD,
  YEAR_WILDCARD
}

class DynamicPathGenerator(wildcardedGlob: String, maxNumberOfDaysToRead: Int, timeZone: ZoneId)
    extends PathGenerator {
  require(verifyPattern)

  private def verifyPattern =
    wildcardedGlob.contains(YEAR_WILDCARD) &&
      wildcardedGlob.contains(MONTH_WILDCARD) &&
      wildcardedGlob.contains(DAY_WILDCARD)

  private def twoDigit(number: Int) = {
    require(number.abs < 100)
    if (number.abs < 10) {
      "0" + number
    } else {
      number.toString
    }
  }

  private[path] def getPaths(currentDate: LocalDate): Set[String] =
    getValidDates(currentDate).map(date =>
      wildcardedGlob
        .replace(YEAR_WILDCARD, date.getYear.toString)
        .replace(MONTH_WILDCARD, twoDigit(date.getMonthValue))
        .replace(DAY_WILDCARD, twoDigit(date.getDayOfMonth)))

  override def getPaths: Set[String] = getPaths(LocalDate.now(timeZone))

  private def getValidDates(currentDate: LocalDate): Set[LocalDate] =
    (0 until maxNumberOfDaysToRead).map(offset => currentDate.minusDays(offset)).toSet
}

object DynamicPathGenerator {
  val YEAR_WILDCARD: String = "{YYYY}"
  val MONTH_WILDCARD: String = "{MM}"
  val DAY_WILDCARD: String = "{DD}"
}
