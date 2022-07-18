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

ThisBuild / scalaVersion := "2.12.16"

lazy val root = (project in file("."))
  .settings(
    organization := "ai.hunters",
    name := "spark-adaptive-file-connector",
    version := "1.0.0-SNAPSHOT")

ThisBuild / versionScheme := Some("semver-spec")

val sparkVersion = "3.1.2"
val circeVersion = "0.13.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "io.circe" %% "circe-parser" % circeVersion % "test",
  "io.circe" %% "circe-generic" % circeVersion % "test",
  "io.github.hakky54" % "logcaptor" % "2.7.9" % "test")

excludeDependencies ++= Seq(
  ExclusionRule().withName("slf4j-log4j12") // logback from tests colide with log4j from Spark
)

Test / parallelExecution := false

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/hunters-ai/spark-adaptive-file-connector"),
    "scm:git@github.com:hunters-ai/spark-adaptive-file-connector.git"))
ThisBuild / developers := List(
  Developer(
    id = "woj-i",
    name = "Wojciech Indyk",
    email = "wojciech.indyk@hunters.ai",
    url = url("https://github.com/woj-i")),
  Developer(
    id = "ada-hunters",
    name = "Ada Sharoni",
    email = "ada@hunters.ai",
    url = url("https://github.com/ada-hunters")))

ThisBuild / description := "The library provides Spark Source " +
  "to efficiently handle streaming from object store for files saved in dynamically changing paths."
ThisBuild / licenses := List(
  "Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(
  url("https://github.com/hunters-ai/spark-adaptive-file-connector"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://s01.oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle := true

credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")

