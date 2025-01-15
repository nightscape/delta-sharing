package io.delta.sharing.server

import java.io.IOException
import java.net.{URL, URLEncoder}
import java.nio.charset.StandardCharsets.UTF_8
import java.security.cert.X509Certificate
import java.sql.Timestamp
import java.time.LocalDateTime
import javax.net.ssl._

import scala.collection.mutable.ArrayBuffer

import org.apache.iceberg.{Schema => IcebergSchema, Snapshot => IcebergSnapshot, Table => IcebergTable}
import org.apache.iceberg.types.Types
import org.apache.commons.io.IOUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import com.linecorp.armeria.server.Server
import io.delta.standalone.types._
import scalapb.json4s.JsonFormat
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.hadoop.fs.Path
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.parquet.ParquetSchemaUtil
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.Record

import io.delta.sharing.server.config.ServerConfig
import io.delta.sharing.server.protocol._

object IcebergTestResource extends BaseTestResource {
  import BaseTestResource._
  val format: String = "iceberg"
  private val conf = new Configuration()
  private val tables = new HadoopTables(conf)

  // Schema for test table
  private val schema = new IcebergSchema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.required(2, "data", Types.StringType.get()),
    Types.NestedField.required(3, "category", Types.StringType.get())
  )


  def createAndPopulateTable(
    path: String,
    actions: Seq[Action] = defaultActions(),
    structType: StructType = structType): String = {
    val table = createIcebergTestTable(path)
    actions.foreach {
      case AddData(ts, numRecords, id) => addTestData(table, numRecords)
      case Bump(ts) => table.newAppend().commit()
      case _ =>
    }


    path
  }

  def createIcebergTestTable(path: String): IcebergTable = {
    // Create Iceberg table with partitioning
    val table = tables.create(
      schema,
      org.apache.iceberg.PartitionSpec.builderFor(schema)
        .identity("category")
        .build(),
      path
    )
    table
  }

  def addTestData(table: IcebergTable, numRecords: Int): Unit = {
    val writer = table.newAppend()

    // Create test data files
    val dataPath = new Path(table.location(), "data")
    val fs = dataPath.getFileSystem(conf)
    fs.mkdirs(dataPath)

    // Create test records
    val records = (1 to numRecords).map(i => createRecord(i.toLong, s"data$i", s"category$i"))

    // Write test data file
    val dataFile = new Path(dataPath, "test-data.parquet")
    val outputFile = org.apache.iceberg.hadoop.HadoopOutputFile.fromPath(dataFile, conf)
    val writer2 = org.apache.iceberg.parquet.Parquet.write(outputFile)
      .schema(schema)
      .createWriterFunc(_ => GenericParquetWriter.buildWriter(ParquetSchemaUtil.convert(schema, "iceberg")))
      .build[Record]()

    println(s"records: ${records}")
    try {
      records.foreach(writer2.add)
    } finally {
      writer2.close()
    }

    // Add the data file to the table
    writer.appendFile(
      org.apache.iceberg.DataFiles.builder(table.spec())
        .withPath(dataFile.toString)
        .withFileSizeInBytes(fs.getFileStatus(dataFile).getLen)
        .withRecordCount(records.size)
        .build()
    )

    writer.commit()
  }

  private def createRecord(id: Long, data: String, category: String): Record = {
    val record = GenericRecord.create(schema)
    record.setField("id", id)
    record.setField("data", data)
    record.setField("category", category)
    record
  }
}
class IcebergSharingServiceSuite extends DeltaSharingServiceSuite {
  override def shouldRunIntegrationTest: Boolean = true

  private var testTable: IcebergTable = _

  override def beforeAll() {
    // scalastyle:off
    println("Java Version: " + System.getProperty("java.version"))
    println("Java Home: " + System.getProperty("java.home"))
    println("Java Class Path: " + System.getProperty("java.class.path"))

    if (shouldRunIntegrationTest) {
      allowUntrustedServer()
      val serverConfigPath = IcebergTestResource.setupTestTables().getCanonicalPath
      serverConfig = ServerConfig.load(serverConfigPath)
      println(serverConfig.toString)
      serverConfig.evaluateJsonPredicateHints = true
      serverConfig.evaluateJsonPredicateHintsV2 = true
      server = DeltaSharingService.start(serverConfig)
      // Initialize test table (Iceberg-specific)
      val tableConfig = serverConfig.shares.get(0).getSchemas.get(0).getTables.get(0)
      val location = tableConfig.getLocation
      val hadoopConf = new org.apache.hadoop.conf.Configuration()
      val path = new org.apache.hadoop.fs.Path(location)
      val fs = path.getFileSystem(hadoopConf)
      if (fs.exists(path)) {
        fs.delete(path, true)
      }
      testTable = IcebergTestResource.createIcebergTestTable(location)
    }
  }
}
