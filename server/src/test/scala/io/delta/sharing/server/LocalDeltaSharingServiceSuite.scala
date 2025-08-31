package io.delta.sharing.server

import java.io.IOException
import java.net.{URL, URLEncoder}
import java.nio.charset.StandardCharsets.UTF_8
import java.security.cert.X509Certificate
import java.sql.{Date, Timestamp}
import java.time.{LocalDateTime, ZoneOffset}
import javax.net.ssl._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.linecorp.armeria.server.Server
import io.delta.standalone.CommitResult
import io.delta.standalone.internal.deltaLogForTableWithClock
import io.delta.standalone.internal.util.newManualClock
import io.delta.standalone.types._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.security.GroupMappingServiceProvider
import org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.apache.parquet.io.OutputFile
import org.scalatest.{BeforeAndAfterAll, Canceled, Outcome}
import org.scalatest.funsuite.AnyFunSuite
import scalapb.json4s.JsonFormat

import io.delta.sharing.server.BaseTestResource._
import io.delta.sharing.server.config.ServerConfig
import io.delta.sharing.server.protocol.{Schema => DeltaSchema, _}

object LocalDeltaTestResource extends BaseTestResource {
  override def format: String = "delta"

  override def createAndPopulateTable(
    path: String,
    actions: Seq[Action] = defaultActions(),
    structType: StructType = structType
  ): String = {
    import io.delta.standalone.{DeltaLog, Operation}
    import io.delta.standalone.actions._
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.Path
    import org.apache.parquet.hadoop.ParquetWriter
    import java.util.UUID

    // Convert Delta standalone StructType to Avro Schema
    def convertToAvroSchema(structType: StructType): Schema = {
      val fields = structType.getFields.map { field =>
        new Schema.Field(
          field.getName,
          convertDeltaTypeToAvro(field.getDataType),
          "",
          null // default value
        )
      }

      Schema.createRecord(
        "test", // name
        "", // doc
        "delta.sharing", // namespace
        false, // isError
        java.util.Arrays.asList(fields: _*)
      )
    }

    def convertDeltaTypeToAvro(deltaType: DataType): Schema = deltaType match {
      case _: LongType => Schema.create(Schema.Type.LONG)
      case _: IntegerType => Schema.create(Schema.Type.INT)
      case _: StringType => Schema.create(Schema.Type.STRING)
      case _: BooleanType => Schema.create(Schema.Type.BOOLEAN)
      case _: DoubleType => Schema.create(Schema.Type.DOUBLE)
      case _: FloatType => Schema.create(Schema.Type.FLOAT)
      case _: ByteType => Schema.create(Schema.Type.INT)
      case _: ShortType => Schema.create(Schema.Type.INT)
      case _: BinaryType => Schema.create(Schema.Type.BYTES)
      case _: TimestampType => Schema.create(Schema.Type.LONG)
      case _: DateType => Schema.create(Schema.Type.INT)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported type: ${deltaType}")
    }

    val hadoopConf = new Configuration()
    val tablePath = new Path(path)

    val fs = tablePath.getFileSystem(hadoopConf)

    // Create schema dynamically from structType
    val schema = convertToAvroSchema(structType)

    // Initialize DeltaLog
    val clock = newManualClock
    val deltaLog = deltaLogForTableWithClock(hadoopConf, tablePath, clock)
    val dataPath = new Path(tablePath, "data")
    fs.mkdirs(dataPath)

    def commit[T <: Action](
      actions: Iterable[T],
      op: Operation,
      engineInfo: String,
      timestamp: Long
    ): CommitResult = {
      clock.setTime(timestamp)
      val transaction = deltaLog.startTransaction()
      val commitResult = transaction
        .commit(actions.asJava, op, engineInfo)
      val filePath = deltaLog.getPath().suffix(f"/_delta_log/${commitResult.getVersion}%020d.json")
      fs.setTimes(filePath, timestamp, -1)
      val snapshot = deltaLog.snapshot()
      val fileContents = fs.open(filePath)
      commitResult
    }

    val partitionColumns = structType.getFields
      .filter(_.getDataType.isInstanceOf[StringType])
      .drop(1)

    def ts2Long(ts: LocalDateTime): Long = ts.toInstant(ZoneOffset.UTC).getEpochSecond * 1000
    // Execute actions in chronological order
    actions
      .foreach {
        case Create(ts, metadataConfig, metadataId) =>
          val timestamp = ts2Long(ts)
          // Commit initial version
          val protocol = new Protocol(1, 2)
          val metadataBuilder = metadataId.foldLeft(Metadata.builder())(_.id(_))
          val metadata = metadataBuilder
            .createdTime(timestamp)
            .schema(structType)
            .partitionColumns(java.util.Arrays.asList(partitionColumns.map(_.getName): _*))
            .configuration(metadataConfig.asJava)
            .build()
          commit(
            Seq(protocol, metadata),
            new Operation(Operation.Name.CREATE_TABLE),
            "Created test table",
            timestamp
          )

        case UpdateConfiguration(ts, changes) =>
          val timestamp = ts2Long(ts)
          val metadata = Metadata
            .builder()
            .createdTime(timestamp)
            .schema(structType)
            .partitionColumns(java.util.Arrays.asList(partitionColumns.map(_.getName): _*))
            .configuration(changes.asJava)
            .build()

          commit(
            Seq(metadata),
            new Operation(Operation.Name.MANUAL_UPDATE),
            s"Updated configuration at $timestamp",
            timestamp
          )

        case GenerateCDC(ts, changes) =>
          val timestamp = ts2Long(ts)
          changes.foreach { change =>
            val cdcFile = new Path(tablePath, s"_change_data/${change.path}")
            val writer = AvroParquetWriter
              .builder[GenericRecord](HadoopOutputFile.fromPath(cdcFile, hadoopConf))
              .withConf(hadoopConf)
              .withSchema(schema)
              .build()

            try {
              // Create CDC record with operation type
              val record = createSampleRecord(schema, structType, 0)
              writer.write(record)
            } finally {
              writer.close()
            }
          }

        case AddData(ts, numRecords, id) =>
          val timestamp = ts2Long(ts)
          // Create new data file with random UUID
          val newDataFile =
            new Path(dataPath, s"part-${UUID.randomUUID()}.parquet")

          // Write new test data using same schema
          val writer = AvroParquetWriter
            .builder[GenericRecord](HadoopOutputFile.fromPath(newDataFile, hadoopConf))
            .withConf(hadoopConf)
            .withSchema(schema)
            .build()

          try {
            // Create new sample records
            val records =
              (0 until numRecords).map(i => createSampleRecord(schema, structType, i))
            records.foreach(writer.write)
          } finally {
            writer.close()
          }

          val addFile = AddFile
            .builder(
              newDataFile.toString,
              partitionColumns
                .map(field => field.getName -> s"${field.getName}1")
                .toMap
                .asJava,
              fs.getFileStatus(newDataFile).getLen,
              timestamp * 1000,
              true
            )
            .build()

          commit(
            Seq(addFile),
            new Operation(Operation.Name.MANUAL_UPDATE),
            s"Added data at $timestamp",
            timestamp
          )

        case RemovePath(path) =>
          val toRemove = deltaLog.getPath.suffix(s"/$path")
          fs.delete(toRemove, true)

        case Bump(ts) =>
          commit(
            Seq[Action](),
            new Operation(Operation.Name.MANUAL_UPDATE),
            s"Bump version at $ts",
            ts2Long(ts)
          )
      }

    path
  }

  private def createSampleRecord(
    schema: Schema,
    structType: StructType,
    index: Int
  ): GenericRecord = {
    val record = new GenericData.Record(schema)

    structType.getFields.foreach { field =>
      val sampleValue = generateSampleValue(field.getDataType, index)
      record.put(field.getName, sampleValue)
    }
    record
  }

  private def generateSampleValue(dataType: DataType, index: Int): Any =
    dataType match {
      case _: LongType => index.toLong
      case _: IntegerType => index
      case _: StringType => s"data${index}"
      case _: BooleanType => index % 2 == 0
      case _: DoubleType => index.toDouble
      case _: FloatType => index.toFloat
      case _: ByteType => index.toByte
      case _: ShortType => index.toShort
      case _: BinaryType => s"data${index}".getBytes(UTF_8)
      case _: TimestampType => index.toLong
      case _: DateType => index
      case _ =>
        throw new IllegalArgumentException(s"Unsupported type: ${dataType}")
    }
}

class LocalDeltaSharingServiceSuite extends DeltaSharingServiceSuite {
  override def shouldRunIntegrationTest: Boolean = true

  val nonWorkingTests = Set("azure support")
  override def withFixture(test: NoArgTest): Outcome = {
    if (nonWorkingTests.contains(test.name)) {
      Canceled(s"Test '${test.name}' ignored in child class")
    } else {
      super.withFixture(test)
    }
  }

  override def beforeAll() {
    if (shouldRunIntegrationTest) {
      allowUntrustedServer()
      val serverConfigPath =
        LocalDeltaTestResource.setupTestTables().getCanonicalPath
      serverConfig = ServerConfig.load(serverConfigPath)
      println(serverConfig.toString)
      serverConfig.evaluateJsonPredicateHints = true
      serverConfig.evaluateJsonPredicateHintsV2 = true
      server = DeltaSharingService.start(serverConfig)
    }
  }
}
