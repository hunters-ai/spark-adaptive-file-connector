/*
 * Copyright 2021 and onwards Hunters.ai.
 * Copyright 2014 and onwards The Apache Software Foundation.
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
 *
 * This file base is based on FieStreamSource from Apache Spark:
 * https://github.com/apache/spark/blob/512d337abf1387a81ac47e50656e330eb3f51b22
 * /sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/FileStreamSource.scala
 * and it has been modified by Hunters.ai to add support for dynamic paths.
 */

package org.apache.spark.sql.streaming.fs

import java.net.URI
import java.util.concurrent.TimeUnit.NANOSECONDS

import ai.hunters.spark.sql.streaming.path.PathGenerator
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.read.streaming
import org.apache.spark.sql.connector.read.streaming.{
  ReadAllAvailable,
  ReadLimit,
  ReadMaxFiles,
  SupportsAdmissionControl
}
import org.apache.spark.sql.execution.datasources.{DataSource, InMemoryFileIndex, LogicalRelation}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.types.StructType

class DynamicPathsFileStreamSource(
    sparkSession: SparkSession,
    pathGenerator: PathGenerator,
    fileFormatClassName: String,
    override val schema: StructType,
    partitionColumns: Seq[String],
    metadataPath: String,
    options: Map[String, String])
    extends SupportsAdmissionControl
    with Source
    with Logging {
  import FileStreamSource._

  private val sourceOptions = new FileStreamOptions(options)

  private val hadoopConf = sparkSession.sessionState.newHadoopConf()

  private var currentPaths = pathGenerator.getPaths

  @transient private val fs = new Path(currentPaths.head).getFileSystem(hadoopConf)

  private def qualifiedBasePath(path: String): Path =
    fs.makeQualified(new Path(path)) // can contain glob patterns

  private var sourceCleaner: Seq[FileStreamSourceCleaner] = createSourceCleaners

  private def createSourceCleaners: Seq[FileStreamSourceCleaner] =
    currentPaths
      .flatMap(path =>
        FileStreamSourceCleaner(fs, qualifiedBasePath(path), sourceOptions, hadoopConf))
      .toSeq

  private val optionsWithPartitionBasePath = sourceOptions.optionMapWithoutPath ++ {
    if (!SparkHadoopUtil.get.isGlobPath(new Path(currentPaths.head)) && options.contains(
        "path")) {
      logWarning(
        "Partitioning is not supported for this source. Continuing with non-partitions mode.")
      // for enabling partitioning (based on label=value)
      // we need to modify PartitioningAwareFileIndex.basePaths (spark-core)
      // to be able to read multiple paths from "basePath" option
      // (or another, that we would create).
    }
    Map()
  }

  private val metadataLog =
    new FileStreamSourceLog(FileStreamSourceLog.VERSION, sparkSession, metadataPath)
  private var metadataLogCurrentOffset = metadataLog.getLatest().map(_._1).getOrElse(-1L)

  /** Maximum number of new files to be considered in each batch */
  private val maxFilesPerBatch = sourceOptions.maxFilesPerTrigger

  private val fileSortOrder = if (sourceOptions.latestFirst) {
    logWarning(
      """'latestFirst' is true. New files will be processed first, which may affect the watermark
                 |value. In addition, 'maxFileAge' will be ignored.""".stripMargin)
    implicitly[Ordering[Long]].reverse
  } else {
    implicitly[Ordering[Long]]
  }

  private val maxFileAgeMs: Long = if (sourceOptions.latestFirst && maxFilesPerBatch.isDefined) {
    Long.MaxValue
  } else {
    sourceOptions.maxFileAgeMs
  }

  private val fileNameOnly = sourceOptions.fileNameOnly
  if (fileNameOnly) {
    logWarning("'fileNameOnly' is enabled. Make sure your file names are unique (e.g. using " +
      "UUID), otherwise, files with the same name but under different paths will be considered " +
      "the same and causes data lost.")
  }

  /**
   * A mapping from a file that we have processed to some timestamp it was last modified. Visible
   * for testing and debugging in production.
   */
  val seenFiles = new SeenFilesMap(maxFileAgeMs, fileNameOnly)

  metadataLog.restore().foreach { entry =>
    seenFiles.add(entry.path, entry.timestamp)
  }
  seenFiles.purge()

  logInfo(s"maxFilesPerBatch = $maxFilesPerBatch, maxFileAgeMs = $maxFileAgeMs")

  private var unreadFiles: Seq[(String, Long)] = _

  def updateCurrentPaths(): Unit = {
    val oldPaths = currentPaths
    currentPaths = pathGenerator.getPaths
    if (oldPaths != currentPaths) {
      logDebug("Change in current paths detected. Recreating source cleaners.")
      // TODO: consider to apply .clean() on paths that are not present in the new currentPaths
      sourceCleaner = createSourceCleaners
    }
  }

  /**
   * Returns the maximum offset that can be retrieved from the source.
   *
   * `synchronized` on this method is for solving race conditions in tests. In the normal usage,
   * there is no race here, so the cost of `synchronized` should be rare.
   */
  private def fetchMaxOffset(limit: ReadLimit): FileStreamSourceOffset = synchronized {
    updateCurrentPaths()
    val newFiles = if (unreadFiles != null) {
      logDebug(s"Reading from unread files - ${unreadFiles.size} files are available.")
      unreadFiles
    } else {
      // All the new files found - ignore aged files and files that we have seen.
      fetchFiles(currentPaths).filter { case (path, timestamp) =>
        seenFiles.isNewFile(path, timestamp)
      }
    }

    // Obey user's setting to limit the number of files in this batch trigger.
    val (batchFiles, unselectedFiles) = limit match {
      case files: ReadMaxFiles if !sourceOptions.latestFirst =>
        // we can cache and reuse remaining fetched list of files in further batches
        val (batchedFiles, unseenFiles) = newFiles.splitAt(files.maxFiles())
        if (unseenFiles.size < files.maxFiles() * DISCARD_UNSEEN_FILES_RATIO) {
          // Discard unselected files if the number of files are smaller than threshold.
          // This is to avoid the case when the next batch would have too few files to read
          // whereas there're new files available.
          logTrace(
            s"Discarding ${unseenFiles.length} unread files as it's smaller than threshold.")
          (batchedFiles, null)
        } else {
          (batchedFiles, unseenFiles)
        }

      case files: ReadMaxFiles =>
        // implies "sourceOptions.latestFirst = true" which we want to refresh the list per batch
        (newFiles.take(files.maxFiles()), null)

      case _: ReadAllAvailable => (newFiles, null)
    }

    if (unselectedFiles != null && unselectedFiles.nonEmpty) {
      logTrace(s"Taking first $MAX_CACHED_UNSEEN_FILES unread files.")
      unreadFiles = unselectedFiles.take(MAX_CACHED_UNSEEN_FILES)
      logTrace(s"${unreadFiles.size} unread files are available for further batches.")
    } else {
      unreadFiles = null
      logTrace(s"No unread file is available for further batches.")
    }

    batchFiles.foreach { file =>
      seenFiles.add(file._1, file._2)
      logDebug(s"New file: $file")
    }
    val numPurged = seenFiles.purge()

    logTrace(s"""
                |Number of new files = ${newFiles.size}
                |Number of files selected for batch = ${batchFiles.size}
                |Number of unread files = ${Option(unreadFiles).map(_.size).getOrElse(0)}
                |Number of seen files = ${seenFiles.size}
                |Number of files purged from tracking map = $numPurged
       """.stripMargin)

    if (batchFiles.nonEmpty) {
      metadataLogCurrentOffset += 1

      val fileEntries = batchFiles.map { case (p, timestamp) =>
        FileEntry(path = p, timestamp = timestamp, batchId = metadataLogCurrentOffset)
      }.toArray
      if (metadataLog.add(metadataLogCurrentOffset, fileEntries)) {
        logInfo(s"Log offset set to $metadataLogCurrentOffset with ${batchFiles.size} new files")
      } else {
        throw new IllegalStateException(
          "Concurrent update to the log. Multiple streaming jobs " +
            s"detected for $metadataLogCurrentOffset")
      }
    }

    FileStreamSourceOffset(metadataLogCurrentOffset)
  }

  override def getDefaultReadLimit: ReadLimit =
    maxFilesPerBatch.map(ReadLimit.maxFiles).getOrElse(super.getDefaultReadLimit)

  /**
   * For test only. Run `func` with the internal lock to make sure when `func` is running, the
   * current offset won't be changed and no new batch will be emitted.
   */
  def withBatchingLocked[T](func: => T): T = synchronized {
    func
  }

  /** Return the latest offset in the [[FileStreamSourceLog]] */
  def currentLogOffset: Long = synchronized(metadataLogCurrentOffset)

  /**
   * Returns the data that is between the offsets (`start`, `end`].
   */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val startOffset = start.map(FileStreamSourceOffset(_).logOffset).getOrElse(-1L)
    val endOffset = FileStreamSourceOffset(end).logOffset

    assert(startOffset <= endOffset)
    val files = metadataLog.get(Some(startOffset + 1), Some(endOffset)).flatMap(_._2)
    logInfo(s"Processing ${files.length} files from ${startOffset + 1}:$endOffset")
    logTrace(s"Files are:\n\t" + files.mkString("\n\t"))
    val newDataSource =
      DataSource(
        sparkSession,
        paths = files.map(f => new Path(new URI(f.path)).toString),
        userSpecifiedSchema = Some(schema),
        partitionColumns = partitionColumns,
        className = fileFormatClassName,
        options = optionsWithPartitionBasePath)
    Dataset.ofRows(
      sparkSession,
      LogicalRelation(newDataSource.resolveRelation(checkFilesExist = false), isStreaming = true))
  }

  /**
   * None means we don't know at the moment Some(true) means we know for sure the source DOES have
   * metadata Some(false) means we know for sure the source DOSE NOT have metadata
   */
  @volatile private[sql] var sourceHasMetadata: Option[Boolean] = None

  private def allFilesUsingInMemoryFileIndex(paths: Set[String]) = {
    val globbedPaths = paths.flatMap(path => parseGlobIfNeccessary(path)).toSeq
    val fileIndex =
      new InMemoryFileIndex(sparkSession, globbedPaths, options, Some(new StructType))
    fileIndex.allFiles()
  }

  private def parseGlobIfNeccessary(path: String) =
    SparkHadoopUtil.get.globPathIfNecessary(fs, qualifiedBasePath(path))

  private def allFilesUsingMetadataLogFileIndex(paths: Set[String]) =
    paths
      .flatMap(path => parseGlobIfNeccessary(path))
      .flatMap(path =>
        new MetadataLogFileIndex(sparkSession, path, CaseInsensitiveMap(options), None)
          .allFiles())
      .toSeq

  private def setSourceHasMetadata(newValue: Option[Boolean]): Unit = newValue match {
    case Some(true) =>
      if (sourceCleaner.nonEmpty) {
        throw new UnsupportedOperationException(
          "Clean up source files is not supported when" +
            " reading from the output directory of FileStreamSink.")
      }
      sourceHasMetadata = Some(true)
    case _ =>
      sourceHasMetadata = newValue
  }

  private def hasMetadata(paths: Set[String]): Boolean =
    // FileStreamSink accept only single paths wrapped with Seq.
    // Therefore all files are checked separately.
    paths.forall(path =>
      FileStreamSink.hasMetadata(Seq(path), hadoopConf, sparkSession.sessionState.conf))

  /**
   * Returns a list of files found, sorted by their timestamp.
   */
  private def fetchFiles(paths: Set[String]): Seq[(String, Long)] = {
    val startTime = System.nanoTime

    var allFiles: Seq[FileStatus] = null
    sourceHasMetadata match {
      case None =>
        if (hasMetadata(paths)) {
          setSourceHasMetadata(Some(true))
          allFiles = allFilesUsingMetadataLogFileIndex(paths)
        } else {
          allFiles = allFilesUsingInMemoryFileIndex(paths)
          if (allFiles.isEmpty) {
            // we still cannot decide
          } else {
            // decide what to use for future rounds
            // double check whether source has metadata, preventing the extreme corner case that
            // metadata log and data files are only generated after the previous
            // `FileStreamSink.hasMetadata` check
            if (hasMetadata(paths)) {
              setSourceHasMetadata(Some(true))
              allFiles = allFilesUsingMetadataLogFileIndex(paths)
            } else {
              setSourceHasMetadata(Some(false))
              // `allFiles` have already been fetched using InMemoryFileIndex in this round
            }
          }
        }
      case Some(true) => allFiles = allFilesUsingMetadataLogFileIndex(paths)
      case Some(false) => allFiles = allFilesUsingInMemoryFileIndex(paths)
    }

    val files = allFiles.sortBy(_.getModificationTime)(fileSortOrder).map { status =>
      (status.getPath.toUri.toString, status.getModificationTime)
    }
    val endTime = System.nanoTime
    val listingTimeMs = NANOSECONDS.toMillis(endTime - startTime)
    if (listingTimeMs > 2000) {
      // Output a warning when listing files uses more than 2 seconds.
      logWarning(s"Listed ${files.size} file(s) in $listingTimeMs ms")
    } else {
      logTrace(s"Listed ${files.size} file(s) in $listingTimeMs ms")
    }
    logTrace(s"Files are:\n\t" + files.mkString("\n\t"))
    files
  }

  override def getOffset: Option[Offset] =
    throw new UnsupportedOperationException(
      "latestOffset(Offset, ReadLimit) should be called " +
        "instead of this method for DynamicPathsFileStreamSource")

  override def latestOffset(startOffset: streaming.Offset, limit: ReadLimit): streaming.Offset =
    Some(fetchMaxOffset(limit)).filterNot(_.logOffset == -1).orNull

  override def toString: String = s"FileStreamSource[$currentPaths]"

  /**
   * Informs the source that Spark has completed processing all data for offsets less than or
   * equal to `end` and will only request offsets greater than `end` in the future.
   */
  override def commit(end: Offset): Unit = {
    val logOffset = FileStreamSourceOffset(end).logOffset

    sourceCleaner.foreach { cleaner =>
      val files = metadataLog.get(Some(logOffset), Some(logOffset)).flatMap(_._2)
      val validFileEntities = files.filter(_.batchId == logOffset)
      logDebug(s"completed file entries: ${validFileEntities.mkString(",")}")
      validFileEntities.foreach(cleaner.clean)
    }
  }

  override def stop(): Unit = sourceCleaner.foreach(_.stop())
}
