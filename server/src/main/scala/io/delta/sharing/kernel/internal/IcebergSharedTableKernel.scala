/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.sharing.kernel.internal

import java.io.{FileNotFoundException, IOException}
import java.net.{URI, URL, URLEncoder}
import java.nio.charset.StandardCharsets.UTF_8
import java.security.cert.X509Certificate
import java.sql.Timestamp
import java.time.{Instant, OffsetDateTime}
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.util.Base64
import javax.net.ssl._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.{Breaks, NonFatal}

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
import com.google.common.hash.Hashing
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.hadoop.fs.azure.NativeAzureFileSystem
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.iceberg.{DataFile, Snapshot, Table => IcebergTable, TableScan}
import org.apache.iceberg.expressions.{Expression, Expressions, UnboundTerm}
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.types.Types
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructType}
import scalapb.GeneratedMessage
import scalapb.GeneratedMessageCompanion

import io.delta.sharing.server.{
  DeltaSharedTableProtocol,
  DeltaSharingIllegalArgumentException,
  DeltaSharingUnsupportedOperationException,
  ErrorStrings,
  QueryResult
}
import io.delta.sharing.server.common._
import io.delta.sharing.server.common.actions._
import io.delta.sharing.server.config.ServerConfig
import io.delta.sharing.server.config.TableConfig
import io.delta.sharing.server.model._
import io.delta.sharing.server.protocol._

/**
 * A table class that wraps Iceberg Table to provide the methods used by the
 * server.
 */
class IcebergSharedTableKernel(
    tableConfig: TableConfig,
    preSignedUrlTimeoutSeconds: Long,
    evaluatePredicateHints: Boolean,
    evaluateJsonPredicateHints: Boolean,
    evaluateJsonPredicateHintsV2: Boolean,
    queryTablePageSizeLimit: Int,
    queryTablePageTokenTtlMs: Int,
    refreshTokenTtlMs: Int
) extends DeltaSharedTableProtocol {

  protected val tablePath: Path = new Path(tableConfig.getLocation)
  private val conf = new Configuration()
  private val tables = new HadoopTables(conf)
  private val table = tables.load(tableConfig.getLocation)

  private var numRecords = 0L
  private var earlyTermination = false
  private var minUrlExpirationTimestamp = Long.MaxValue
  private val JsonPredicateHintSizeLimit = 1L * 1024L * 1024L // 1MB
  private val JsonPredicateHintMaxTreeDepth = 100

  private val fileSigner = withClassLoader {
    val tablePath = new Path(tableConfig.getLocation)
    val conf = new Configuration()
    val fs = tablePath.getFileSystem(conf)
    val dataPath = new URI(table.location())

    fs match {
      case _: S3AFileSystem =>
        new S3FileSigner(dataPath, conf, preSignedUrlTimeoutSeconds)
      case wasb: NativeAzureFileSystem =>
        WasbFileSigner(wasb, dataPath, conf, preSignedUrlTimeoutSeconds)
      case abfs: AzureBlobFileSystem =>
        AbfsFileSigner(abfs, dataPath, preSignedUrlTimeoutSeconds)
      case gc: GoogleHadoopFileSystem =>
        new GCSFileSigner(dataPath, conf, preSignedUrlTimeoutSeconds)
      case _: LocalFileSystem =>
        new LocalFileSigner(dataPath, conf, preSignedUrlTimeoutSeconds)
      case _ =>
        throw new IllegalStateException(
          s"File system ${fs.getClass} is not supported"
        )
    }
  }

  override def getTableVersion(startingTimestamp: Option[String]): Long =
    withClassLoader {
      if (startingTimestamp.isDefined) {
        try {
          val timestamp = millisSinceEpoch(startingTimestamp.get)
          val snapshot = table.currentSnapshot()
          if (snapshot == null || snapshot.timestampMillis() < timestamp) {
            throw new DeltaSharingIllegalArgumentException(
              "Not a valid starting timestamp"
            )
          }
          snapshot.snapshotId()
        } catch {
          case e: DeltaSharingIllegalArgumentException => throw e
          case e: Exception =>
            if (e.getMessage.contains("before first snapshot")) {
              return -1L // Return -1 for initial version to match Delta behavior
            }
            throw e
        }
      } else {
        Option(table.currentSnapshot()).map(_.snapshotId()).getOrElse(-1L)
      }
    }

  // scalastyle:off argcount
  override def query(
      includeFiles: Boolean,
      predicateHints: Seq[String],
      jsonPredicateHints: Option[String],
      limitHint: Option[Long],
      version: Option[Long],
      timestamp: Option[String],
      startingVersion: Option[Long],
      endingVersion: Option[Long],
      maxFiles: Option[Int],
      pageToken: Option[String],
      includeRefreshToken: Boolean,
      refreshToken: Option[String],
      responseFormatSet: Set[String],
      clientReaderFeaturesSet: Set[String],
      includeEndStreamAction: Boolean
  ): QueryResult = withClassLoader {

    if (
      Seq(version, timestamp, startingVersion).filter(_.isDefined).size >= 2
    ) {
      throw new DeltaSharingIllegalArgumentException(
        ErrorStrings.multipleParametersSetErrorMsg(
          Seq("version", "timestamp", "startingVersion")
        )
      )
    }

    refreshToken.map(decodeAndValidateRefreshToken)

    val queryType = if (version.isDefined || timestamp.isDefined) {
      QueryTypes.QueryVersionSnapshot
    } else {
      QueryTypes.QueryLatestSnapshot
    }

    // Get the appropriate snapshot
    val snapshot = if (version.isDefined) {
      Option(table.snapshot(version.get))
        .getOrElse(
          throw new FileNotFoundException(s"Version ${version.get} not found")
        )
    } else if (timestamp.isDefined) {
      val ts = millisSinceEpoch(timestamp.get)
      Option(table.snapshot(ts))
        .getOrElse(
          throw new FileNotFoundException(
            s"No snapshot found at timestamp $timestamp"
          )
        )
    } else {
      Option(table.currentSnapshot())
        .getOrElse(throw new FileNotFoundException("No snapshot found"))
    }

    val respondedFormat =
      getRespondedFormat(responseFormatSet, 1) // Iceberg uses version 1

    var actions = Seq[Object](
      // getResponseProtocol(1, respondedFormat),
      getResponseMetadata(
        snapshot,
        version.map(x => x: java.lang.Long).orNull,
        respondedFormat
      )
    )

    if (includeFiles) {
      val scan = table.newScan()

      // Apply predicates if provided
      if (predicateHints.nonEmpty || jsonPredicateHints.isDefined) {
        val expr = createIcebergExpression(predicateHints, jsonPredicateHints)
        expr.foreach(scan.filter)
      }

      // Get files from scan
      val files = scan.planFiles().iterator().asScala.toArray

      // Handle pagination
      val (startIndex, endIndex) =
        getPageBounds(files.length, maxFiles, pageToken)
      var numRecords = 0L
      var minUrlExpirationTimestamp = Long.MaxValue

      for (i <- startIndex until endIndex) {
        val file = files(i)
        if (limitHint.exists(_ <= numRecords)) {
          // Early termination if we hit the limit
          // TODO: break
        }

        val filePath = file.file().path().toString
        val preSignedUrl = fileSigner.sign(new Path(filePath))
        minUrlExpirationTimestamp =
          minUrlExpirationTimestamp.min(preSignedUrl.expirationTimestamp)

        val stats = Option(file.file().valueCounts())
          .map(counts => JsonUtils.toJson(Map("numRecords" -> counts.size())))
          .orNull

        numRecords += Option(stats)
          .flatMap(JsonUtils.extractNumRecords)
          .getOrElse(0L)

        val fileAction =
          if (
            respondedFormat == IcebergSharedTableKernel.RESPONSE_FORMAT_DELTA
          ) {
            DeltaResponseFileAction(
              id = Hashing.sha256().hashString(filePath, UTF_8).toString,
              version =
                if (queryType == QueryTypes.QueryLatestSnapshot) null
                else snapshot.snapshotId(),
              timestamp = null, // Iceberg doesn't track per-file timestamps
              expirationTimestamp = preSignedUrl.expirationTimestamp,
              deltaSingleAction = DeltaSingleAction(
                add = DeltaAddFile(
                  path = preSignedUrl.url,
                  partitionValues = convertPartitionValues(file.file()),
                  size = file.file().fileSizeInBytes(),
                  modificationTime = 0L, // TODO: file.file().lastModified(),
                  dataChange = true,
                  stats = stats,
                  deletionVector = null // Iceberg doesn't have deletion vectors
                )
              )
            ).wrap
          } else {
            AddFile(
              url = preSignedUrl.url,
              id = Hashing.md5().hashString(filePath, UTF_8).toString,
              expirationTimestamp = preSignedUrl.expirationTimestamp,
              partitionValues = convertPartitionValues(file.file()),
              size = file.file().fileSizeInBytes(),
              stats = stats,
              version =
                if (queryType == QueryTypes.QueryLatestSnapshot) null
                else snapshot.snapshotId(),
              timestamp = null
            ).wrap
          }

        actions = actions :+ fileAction
      }

      // Add refresh token if requested
      val refreshTokenStr = if (includeRefreshToken) {
        IcebergSharedTableKernel.encodeToken(
          RefreshToken(
            id = Some(tableConfig.id),
            version = Some(snapshot.snapshotId()),
            expirationTimestamp =
              Some(System.currentTimeMillis() + refreshTokenTtlMs)
          )
        )
      } else {
        null
      }

      // Add end stream action if needed
      if (includeRefreshToken || includeEndStreamAction) {
        val nextPageToken = if (endIndex < files.length) {
          Some(encodePageToken(endIndex))
        } else {
          None
        }
        actions = actions :+ getEndStreamAction(
          nextPageToken.orNull,
          minUrlExpirationTimestamp,
          refreshTokenStr
        )
      }
    }

    QueryResult(snapshot.snapshotId(), actions, respondedFormat)
  }

  override def queryCDF(
      cdfOptions: Map[String, String],
      includeHistoricalMetadata: Boolean,
      maxFiles: Option[Int],
      pageToken: Option[String],
      responseFormatSet: Set[String],
      includeEndStreamAction: Boolean
  ): QueryResult = {

    // Get start and end snapshots from options
    val (startSnapshot, endSnapshot) = getSnapshotRange(cdfOptions)

    val actions = scala.collection.mutable.ListBuffer[Object]()

    // Add metadata
    actions.append(getResponseMetadata(endSnapshot, null, "parquet"))

    val snapshotRestriction: TableScan => TableScan =
      if (startSnapshot.snapshotId() == endSnapshot.snapshotId()) {
        _.useSnapshot(endSnapshot.snapshotId())
      } else {
        _.appendsBetween(startSnapshot.snapshotId(), endSnapshot.snapshotId())
      }
    // Get changes between snapshots
    val changes = snapshotRestriction(table.newScan())
      .planFiles()
      .iterator()
      .asScala

    changes.foreach { fileScanTask =>
      val file = fileScanTask.file()
      val preSignedUrl = fileSigner.sign(new Path(file.path().toString))
      if (fileScanTask.deletes().isEmpty) {
        // This is an addition
        actions.append(createAddFileAction(file, preSignedUrl))
      } else {
        // This is a deletion
        actions.append(createRemoveFileAction(file))
      }
    }

    QueryResult(endSnapshot.snapshotId(), actions.toSeq, "parquet")
  }

  private def getSnapshotRange(
      cdfOptions: Map[String, String]
  ): (Snapshot, Snapshot) = {
    val startVersion = cdfOptions.get("startingVersion").map(_.toLong)
    val endVersion = cdfOptions.get("endingVersion").map(_.toLong)

    val startSnapshot =
      startVersion.map(table.snapshot).getOrElse(table.currentSnapshot())
    val endSnapshot =
      endVersion.map(table.snapshot).getOrElse(table.currentSnapshot())

    if (startSnapshot == null || endSnapshot == null) {
      throw new DeltaSharingIllegalArgumentException("Invalid snapshot version")
    }

    (startSnapshot, endSnapshot)
  }

  private def createAddFileAction(
      file: DataFile,
      preSignedUrl: PreSignedUrl
  ): Object = {
    AddFile(
      url = preSignedUrl.url,
      id = Hashing.sha256().hashString(file.path().toString, UTF_8).toString,
      expirationTimestamp = preSignedUrl.expirationTimestamp,
      partitionValues = convertPartitionValues(file),
      size = file.fileSizeInBytes(),
      stats = convertStats(file),
      version = Some(0L).get,
      timestamp = System.currentTimeMillis()
    ).wrap
  }

  private def createRemoveFileAction(file: DataFile): Object = {
    RemoveFile(
      url = file.path().toString,
      id = Hashing.sha256().hashString(file.path().toString, UTF_8).toString,
      partitionValues = convertPartitionValues(file),
      size = file.fileSizeInBytes(),
      expirationTimestamp =
        System.currentTimeMillis() + (preSignedUrlTimeoutSeconds * 1000),
      timestamp = System.currentTimeMillis(),
      version = Some(0L).get
    ).wrap
  }

  private def getPageBounds(
      totalFiles: Int,
      maxFiles: Option[Int],
      pageToken: Option[String]
  ): (Int, Int) = {
    val start = pageToken.map(decodePageToken).getOrElse(0)
    val pageSize = maxFiles.getOrElse(queryTablePageSizeLimit)
    (start, math.min(start + pageSize, totalFiles))
  }

  private def decodePageToken(token: String): Int = {
    try {
      Base64.getUrlDecoder.decode(token)(0).toInt
    } catch {
      case _: Exception =>
        throw new DeltaSharingIllegalArgumentException(
          s"Invalid page token: $token"
        )
    }
  }

  private def convertStats(file: org.apache.iceberg.DataFile): String = {
    // Convert Iceberg metrics to JSON stats string
    JsonUtils.toJson(
      Map(
        "numRecords" -> file.recordCount(),
        "minValues" -> file.lowerBounds(),
        "maxValues" -> file.upperBounds(),
        "nullCount" -> file.nullValueCounts()
      )
    )
  }

  private def getResponseMetadata(
      snapshot: Snapshot,
      version: java.lang.Long,
      respondedFormat: String
  ): Object = {
    val schema = table.schema()
    val spec = table.spec()

    if (respondedFormat == IcebergSharedTableKernel.RESPONSE_FORMAT_DELTA) {
      DeltaResponseMetadata(
        deltaMetadata = DeltaMetadataCopy(
          id = tableConfig.id,
          name = table.name(),
          description =
            table.properties().getOrDefault("comment", "NO DESCRIPTION"),
          format = DeltaFormat(
            provider = "iceberg",
            options = Map.empty[String, String]
          ),
          schemaString = schema.toString,
          partitionColumns = spec.fields().asScala.map(_.name).toSeq,
          configuration = table.properties().asScala.toMap,
          createdTime = None
        ),
        version = version
      ).wrap
    } else {
      Metadata(
        id = tableConfig.id,
        name = table.name(),
        description =
          table.properties().getOrDefault("comment", "NO DESCRIPTION"),
        format = Format("iceberg"),
        schemaString = cleanUpTableSchema(schema.toString),
        partitionColumns = spec.fields().asScala.map(_.name).toSeq,
        configuration = Map.empty[String, String],
        version = version
      ).wrap
    }
  }

  private def cleanUpTableSchema(schemaString: String): String = {
    // For now, just return the schema string as is since we can't use SparkSchemaConverter
    // We'll need to implement proper schema conversion later
    schemaString
  }

  private def getEndStreamAction(
      nextPageTokenStr: String,
      minUrlExpirationTimestamp: Long,
      refreshTokenStr: String = null
  ): SingleAction = {
    EndStreamAction(
      refreshTokenStr,
      nextPageTokenStr,
      if (minUrlExpirationTimestamp == Long.MaxValue) null
      else minUrlExpirationTimestamp
    ).wrap
  }

  private def createIcebergExpression(
      predicateHints: Seq[String],
      jsonPredicateHints: Option[String]
  ): Option[Expression] = {
    try {
      // Convert JSON predicates if present
      val jsonPredicate = jsonPredicateHints.flatMap { json =>
        try {
          val op = JsonPredicates.fromJsonWithLimits(
            json,
            JsonPredicateHintSizeLimit.toInt,
            JsonPredicateHintMaxTreeDepth
          )
          op.map(convertJsonOpToIcebergExpression)
        } catch {
          case _: Exception => None
        }
      }

      // Convert SQL predicates
      val sqlPredicates = if (predicateHints.nonEmpty) {
        Some(
          predicateHints
            .map(convertSqlToIcebergExpression)
            .reduce(Expressions.and)
        )
      } else {
        None
      }

      // Combine both predicates if present
      (jsonPredicate, sqlPredicates) match {
        case (Some(p1), Some(p2)) => Some(Expressions.and(p1, p2))
        case (Some(p1), None) => Some(p1)
        case (None, Some(p2)) => Some(p2)
        case _ => None
      }
    } catch {
      case _: Exception => None // Return None if any conversion fails
    }
  }

  private def convertJsonOpToIcebergExpression(op: BaseOp): Expression = {
    op match {
      // Leaf operations
      case ColumnOp(name, _) =>
        Expressions.ref(name).asInstanceOf[Expression]
      case LiteralOp(value, valueType) =>
        valueType match {
          case OpDataTypes.BoolType =>
            val ref =
              Expressions.ref("value").asInstanceOf[UnboundTerm[Boolean]]
            Expressions.equal(ref, value.toBoolean)
          case OpDataTypes.IntType =>
            val ref = Expressions.ref("value").asInstanceOf[UnboundTerm[Int]]
            Expressions.equal(ref, value.toInt)
          case OpDataTypes.LongType =>
            val ref = Expressions.ref("value").asInstanceOf[UnboundTerm[Long]]
            Expressions.equal(ref, value.toLong)
          case OpDataTypes.StringType =>
            val ref = Expressions.ref("value").asInstanceOf[UnboundTerm[String]]
            Expressions.equal(ref, value)
          case OpDataTypes.DateType =>
            val ref =
              Expressions.ref("value").asInstanceOf[UnboundTerm[java.sql.Date]]
            Expressions.equal(ref, java.sql.Date.valueOf(value))
          case OpDataTypes.FloatType =>
            val ref = Expressions.ref("value").asInstanceOf[UnboundTerm[Float]]
            Expressions.equal(ref, value.toFloat)
          case OpDataTypes.DoubleType =>
            val ref = Expressions.ref("value").asInstanceOf[UnboundTerm[Double]]
            Expressions.equal(ref, value.toDouble)
          case OpDataTypes.TimestampType =>
            val ref =
              Expressions.ref("value").asInstanceOf[UnboundTerm[Timestamp]]
            Expressions.equal(ref, Timestamp.from(Instant.parse(value)))
          case _ =>
            throw new IllegalArgumentException(s"Unsupported type: $valueType")
        }

      // Unary operations
      case NotOp(children) =>
        Expressions.not(convertJsonOpToIcebergExpression(children.head))
      case IsNullOp(children) =>
        val ref = convertJsonOpToIcebergExpression(children.head)
          .asInstanceOf[UnboundTerm[_]]
        Expressions.isNull(ref)

      // Binary operations
      case EqualOp(children) =>
        val Seq(left, right) = children.map(convertJsonOpToIcebergExpression)
        val leftTerm = left.asInstanceOf[UnboundTerm[Any]]
        val rightTerm = right.asInstanceOf[Any]
        Expressions.equal(leftTerm, rightTerm)
      case LessThanOp(children) =>
        val Seq(left, right) = children.map(convertJsonOpToIcebergExpression)
        val leftTerm = left.asInstanceOf[UnboundTerm[Any]]
        val rightTerm = right.asInstanceOf[Any]
        Expressions.lessThan(leftTerm, rightTerm)
      case LessThanOrEqualOp(children) =>
        val Seq(left, right) = children.map(convertJsonOpToIcebergExpression)
        val leftTerm = left.asInstanceOf[UnboundTerm[Any]]
        val rightTerm = right.asInstanceOf[Any]
        Expressions.lessThanOrEqual(leftTerm, rightTerm)
      case GreaterThanOp(children) =>
        val Seq(left, right) = children.map(convertJsonOpToIcebergExpression)
        val leftTerm = left.asInstanceOf[UnboundTerm[Any]]
        val rightTerm = right.asInstanceOf[Any]
        Expressions.greaterThan(leftTerm, rightTerm)
      case GreaterThanOrEqualOp(children) =>
        val Seq(left, right) = children.map(convertJsonOpToIcebergExpression)
        val leftTerm = left.asInstanceOf[UnboundTerm[Any]]
        val rightTerm = right.asInstanceOf[Any]
        Expressions.greaterThanOrEqual(leftTerm, rightTerm)

      // N-ary operations
      case AndOp(children) =>
        children.map(convertJsonOpToIcebergExpression).reduce(Expressions.and)
      case OrOp(children) =>
        children.map(convertJsonOpToIcebergExpression).reduce(Expressions.or)

      case _ =>
        throw new IllegalArgumentException(s"Unsupported operation: $op")
    }
  }

  private def convertSqlToIcebergExpression(sql: String): Expression = {
    val tokens = sql.split("\\s+")
    if (tokens.length != 3) {
      throw new IllegalArgumentException(s"Invalid SQL predicate: $sql")
    }

    val (col, op, value) = (tokens(0), tokens(1), tokens(2))
    val ref = Expressions.ref(col).asInstanceOf[UnboundTerm[_]]

    // Handle IS NULL and IS NOT NULL separately
    if (op.equalsIgnoreCase("IS")) {
      if (value.equalsIgnoreCase("NULL")) {
        return Expressions.isNull(ref)
      } else if (
        value.equalsIgnoreCase("NOT") &&
        tokens.length == 4 &&
        tokens(3).equalsIgnoreCase("NULL")
      ) {
        return Expressions.notNull(ref)
      }
    }

    // Parse the literal value
    val literal = if (value.startsWith("'") && value.endsWith("'")) {
      value.substring(1, value.length - 1)
    } else {
      try {
        value.toLong
      } catch {
        case _: NumberFormatException =>
          try {
            value.toDouble
          } catch {
            case _: NumberFormatException =>
              if (
                value
                  .equalsIgnoreCase("true") || value.equalsIgnoreCase("false")
              ) {
                value.toBoolean
              } else {
                throw new IllegalArgumentException(
                  s"Invalid literal value: $value"
                )
              }
          }
      }
    }

    // Create the expression based on the operator
    op match {
      case "=" => Expressions.equal(ref.asInstanceOf[UnboundTerm[Any]], literal)
      case ">" =>
        Expressions.greaterThan(ref.asInstanceOf[UnboundTerm[Any]], literal)
      case "<" =>
        Expressions.lessThan(ref.asInstanceOf[UnboundTerm[Any]], literal)
      case ">=" =>
        Expressions.greaterThanOrEqual(
          ref.asInstanceOf[UnboundTerm[Any]],
          literal
        )
      case "<=" =>
        Expressions.lessThanOrEqual(ref.asInstanceOf[UnboundTerm[Any]], literal)
      case "<>" =>
        Expressions.notEqual(ref.asInstanceOf[UnboundTerm[Any]], literal)
      case _ => throw new IllegalArgumentException(s"Unsupported operator: $op")
    }
  }

  private def millisSinceEpoch(timestamp: String): Long = {
    try {
      OffsetDateTime
        .parse(timestamp, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        .toInstant
        .toEpochMilli
    } catch {
      case e: DateTimeParseException =>
        try {
          Timestamp.valueOf(timestamp).getTime
        } catch {
          case _: IllegalArgumentException =>
            throw new DeltaSharingIllegalArgumentException(
              s"Invalid timestamp format: ${e.getMessage}"
            )
        }
    }
  }

  private def decodeAndValidateRefreshToken(tokenStr: String): RefreshToken = {
    val token =
      try {
        IcebergSharedTableKernel.decodeToken[RefreshToken](tokenStr)
      } catch {
        case NonFatal(_) =>
          throw new DeltaSharingIllegalArgumentException(
            s"Error decoding refresh token: $tokenStr."
          )
      }
    if (token.getExpirationTimestamp < System.currentTimeMillis()) {
      throw new DeltaSharingIllegalArgumentException(
        "The refresh token has expired. Please restart the query."
      )
    }
    if (token.getId != tableConfig.id) {
      throw new DeltaSharingIllegalArgumentException(
        "The table specified in the refresh token does not match the table being queried."
      )
    }
    token
  }

  protected def getRespondedFormat(
      responseFormatSet: Set[String],
      minReaderVersion: Int
  ): String = {
    if (responseFormatSet.size == 1) {
      responseFormatSet.head
    } else if (
      responseFormatSet.contains(
        IcebergSharedTableKernel.RESPONSE_FORMAT_PARQUET
      )
    ) {
      IcebergSharedTableKernel.RESPONSE_FORMAT_PARQUET
    } else {
      IcebergSharedTableKernel.RESPONSE_FORMAT_DELTA
    }
  }

  /**
   * Run `func` under the classloader of `IcebergSharedTable`. We cannot use
   * the classloader set by Armeria as Hadoop needs to search the classpath to
   * find its classes.
   */
  private def withClassLoader[T](func: => T): T = {
    val classLoader = Thread.currentThread().getContextClassLoader
    if (classLoader == null) {
      Thread.currentThread().setContextClassLoader(this.getClass.getClassLoader)
      try func
      finally {
        Thread.currentThread().setContextClassLoader(null)
      }
    } else {
      func
    }
  }

  private def convertPartitionValues(
      file: org.apache.iceberg.DataFile
  ): Map[String, String] = {
    val struct = file.partition()
    val spec = table.spec()
    spec
      .fields()
      .asScala
      .map { field =>
        field.name() -> Option(struct.get(field.fieldId(), classOf[Object]))
          .map(_.toString)
          .orNull
      }
      .toMap
  }

  private def encodePageToken(index: Int): String = {
    Base64.getUrlEncoder.encodeToString(Array[Byte](index.toByte))
  }
}

object IcebergSharedTableKernel {
  val RESPONSE_FORMAT_PARQUET = "parquet"
  val RESPONSE_FORMAT_DELTA = "delta"

  private def encodeToken[T <: GeneratedMessage](token: T): String = {
    Base64.getUrlEncoder.encodeToString(token.toByteArray)
  }

  private def decodeToken[T <: GeneratedMessage](
      tokenStr: String
  )(implicit protoCompanion: GeneratedMessageCompanion[T]): T = {
    protoCompanion.parseFrom(Base64.getUrlDecoder.decode(tokenStr))
  }
}
