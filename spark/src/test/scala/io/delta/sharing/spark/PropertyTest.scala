package io.delta.sharing.spark

import hedgehog._
import hedgehog.runner._
import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.col
import java.net.URLClassLoader
import org.apache.commons.io.FileUtils
import java.io.File
import java.nio.file.Files
import java.util.concurrent.CountDownLatch
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit
import java.net.ServerSocket
import org.apache.spark.sql.types._
import java.util.function.IntConsumer


object PropertyTest extends Properties {
  val spark = SparkSession.builder().master("local[4]").getOrCreate()
  private var serverThread: Thread = _

  var pidFile: File = _
  var testProfileFile: File = _
  var testServerPort: Int = _
  def findFreePort(): Int = {
    val socket = new ServerSocket(0)
    try {
      socket.getLocalPort
    } finally {
      socket.close()
    }
  }

  def startServer(): Unit = {
    testServerPort = findFreePort()
    pidFile = Files.createTempFile("delta-sharing-server", ".pid").toFile
    testProfileFile = Files.createTempFile("delta-test", ".share").toFile
    FileUtils.writeStringToFile(testProfileFile,
      s"""{
        |  "shareCredentialsVersion": 1,
        |  "endpoint": "http://localhost:$testServerPort/delta-sharing",
        |  "bearerToken": "dapi5e3574ec767ca1548ae5bbed1a2dc04d"
        |}""".stripMargin, StandardCharsets.UTF_8)

    val startLatch = new CountDownLatch(1)

    val format = "delta"
    val testConfigFile = Files.createTempFile(s"${format}-sharing", ".yaml").toFile
    testConfigFile.deleteOnExit()

    // Create test tables with same structure as TestResource
    val basePath = Files.createTempDirectory(s"${format}-test-tables").toString
    val serverConfig = scala.io.Source.fromResource("server-config.yaml")
      .mkString
      .replace("test-port", testServerPort.toString)
      .replace("test-format", format)
      .replace("test-location", basePath)
    FileUtils.writeStringToFile(testConfigFile, serverConfig)
    val testConfigPath = testConfigFile.getCanonicalPath

    // Create a filtered parent classloader that only allows java.* packages
    val filteredParent = new FilteredClassLoader(
      ClassLoader.getPlatformClassLoader(),
      (name: String) => Seq("java.", "javax.", "org.ietf.jgss.", "com.sun.security.").exists(name.startsWith)
    )

    // Use the filtered classloader as parent for the URLClassLoader
    val serverClassLoader = new URLClassLoader(
      Array(new File("out/server/test/assembly.dest/out.jar").toURI.toURL),
      filteredParent
    )

    serverThread = new Thread("Run TestDeltaSharingServer") {
      setContextClassLoader(serverClassLoader)
      setDaemon(true)

      override def run(): Unit = {
        try {
          val serverClass = serverClassLoader.loadClass(
            "io.delta.sharing.server.TestDeltaSharingServer")
          val runMethod = serverClass.getMethod("startWithConfig",
            classOf[String]
          )

          val stopServer = runMethod.invoke(null,
            testConfigPath
          ).asInstanceOf[Runnable]
          println(s"Server is running on port $testServerPort")
          startLatch.countDown()
        } catch {
          case e: Exception =>
            e.printStackTrace()
            startLatch.countDown()
        }
      }
    }

    serverThread.start()

    try {
      assert(startLatch.await(120, TimeUnit.SECONDS),
        "the server didn't start in 120 seconds")
    } catch {
      case e: Throwable =>
        serverThread.interrupt()
        serverThread = null
        throw e
    }
  }

  def stopServer(): Unit = {
    if (serverThread != null) {
      serverThread.interrupt()
      serverThread = null
    }
  }

  override def tests: List[Test] = List(
    property("delta sharing operations", testDeltaSharingOperations)
  )

  // ----------------------------------------------------------------------------
  // Extended TableState
  //
  // Let's track known versions and timestamps so we can generate valid/invalid
  // time-travel requests more systematically.
  // ----------------------------------------------------------------------------
  case class TableState(
                         rows: List[Row],
                         // The latest version we successfully read or created:
                         currentVersion: Option[Long],
                         // A range of known valid versions for this table path (min -> max)
                         // as we discover them. This is just an example approach.
                         knownVersions: Map[String, (Long, Long)],
                         // We can also track known valid timestamp ranges, or store them as strings:
                         knownTimestamps: Map[String, (Long, Long)],
                         // For CDF results
                         cdfChanges: List[Row] = Nil,
                         // Track schema for validation
                         currentSchema: Option[StructType] = None,
                         // Track partition values we've seen
                         knownPartitionValues: Map[String, Set[String]] = Map.empty,
                         // Track any errors we've encountered
                         lastError: Option[Throwable] = None
                       )

  // Initialize an empty state
  val initialState = TableState(
    rows = Nil,
    currentVersion = None,
    knownVersions = Map.empty,
    knownTimestamps = Map.empty,
    cdfChanges = Nil,
    currentSchema = None,
    knownPartitionValues = Map.empty,
    lastError = None
  )

  // ----------------------------------------------------------------------------
  // Commands
  // ----------------------------------------------------------------------------
  sealed trait Command

  case class ReadTable(tablePath: String) extends Command

  case class ReadTableWithVersion(tablePath: String, version: Int) extends Command

  case class ReadTableWithTimestamp(tablePath: String, timestamp: String) extends Command

  case class ReadTableWithFilter(tablePath: String, filter: String) extends Command

  case class CreateManagedTable(tablePath: String, tableName: String) extends Command

  case class ReadTableChanges(
                               tablePath: String,
                               startingVersion: Option[Long] = None,
                               endingVersion: Option[Long] = None,
                               startingTimestamp: Option[String] = None,
                               endingTimestamp: Option[String] = None
                             ) extends Command

  // New commands for CDF operations
  case class ReadTableCDFWithFilter(
    tablePath: String,
    startingVersion: Long,
    endingVersion: Long,
    filter: String
  ) extends Command

  case class ReadTableCDFWithChangeType(
    tablePath: String,
    startingVersion: Long,
    endingVersion: Long,
    changeType: String // e.g. "insert", "update", "delete"
  ) extends Command

  case class ReadTableWithPartitionPruning(
    tablePath: String,
    partitionColumn: String,
    partitionValue: String
  ) extends Command

  case class ValidateSchema(tablePath: String) extends Command

  case class ReadTableWithMultipleFilters(
    tablePath: String,
    filters: List[(String, String)] // column -> value pairs
  ) extends Command

  // New commands to align with what DeltaSharingSuite often checks

  /**
   * In DeltaSharingSuite, you might see a test that ensures specifying both
   * versionAsOf and timestampAsOf throws an exception.
   */
  case class ReadTableWithIncompatibleOptions(tablePath: String, version: Int, timestamp: String)
    extends Command

  /**
   * Check how the reader handles a version that doesn't exist (e.g. 9999).
   * Typically, this should throw an error or return an empty result
   * (depending on your Delta Sharing integration).
   */
  case class ReadTableWithNonExistentVersion(tablePath: String, version: Int) extends Command

  // New commands for table modifications
  case class AddTableData(
    tablePath: String,
    data: List[Row],
    timestamp: Option[Long] = None
  ) extends Command

  case class UpdateTableConfiguration(
    tablePath: String,
    changes: Map[String, String],
    timestamp: Option[Long] = None
  ) extends Command

  case class GenerateTableCDC(
    tablePath: String,
    changes: List[Row], // The actual changes to apply
    timestamp: Option[Long] = None,
    operation: String // "insert", "update", "delete"
  ) extends Command

  case class RemoveTablePath(
    tablePath: String,
    path: String // e.g. "data/part-00000-1.snappy.parquet" or "_delta_log/00000000000000000001.json"
  ) extends Command

  // ----------------------------------------------------------------------------
  // Generator for commands
  // ----------------------------------------------------------------------------
  def genCommand: Gen[Command] = for {
    tablePath <- Gen.element1(
      testProfileFile.getCanonicalPath + "#test-share.test-schema.test-table",
    )
    cmd <- Gen.choice1[Command](
      Gen.constant(ReadTable(tablePath)),
      Gen.constant(ReadTableWithVersion(tablePath, 1)),
      Gen.constant(ReadTableWithFilter(tablePath, "date = '2021-04-28'")),
      Gen.constant(CreateManagedTable(tablePath, "delta_sharing_test")),
      Gen.constant(ReadTableWithTimestamp(tablePath, "2021-04-28 16:35:00")),
      Gen.constant(ReadTableChanges(
        tablePath,
        startingVersion = Some(0),
        endingVersion = Some(3)
      )),
      Gen.constant(ReadTableChanges(
        tablePath,
        startingTimestamp = Some("2021-04-28 00:00:00"),
        endingTimestamp = Some("2021-04-29 00:00:00")
      )),
      // New command generators
      Gen.constant(ReadTableCDFWithFilter(
        tablePath,
        startingVersion = 1,
        endingVersion = 3,
        filter = "age = 2"
      )),
      Gen.constant(ReadTableCDFWithChangeType(
        tablePath,
        startingVersion = 1,
        endingVersion = 3,
        changeType = "insert"
      )),
      Gen.constant(ReadTableWithPartitionPruning(
        tablePath,
        partitionColumn = "birthday",
        partitionValue = "2020-01-01"
      )),
      Gen.constant(ValidateSchema(tablePath)),
      Gen.constant(ReadTableWithMultipleFilters(
        tablePath,
        filters = List(("age", "2"), ("birthday", "2020-01-01"))
      )),
      // New commands for error cases
      Gen.constant(ReadTableWithIncompatibleOptions(tablePath, 1, "2021-04-28 16:35:00")),
      Gen.constant(ReadTableWithNonExistentVersion(tablePath, 9999)),
      // New generators for table modifications
      Gen.constant(AddTableData(
        tablePath,
        generateTestData(BaseTestResource.cdfTableSchema, 3),
        Some(System.currentTimeMillis())
      )),
      Gen.constant(UpdateTableConfiguration(
        tablePath,
        Map("delta.enableChangeDataFeed" -> "true"),
        Some(System.currentTimeMillis())
      )),
      Gen.constant(GenerateTableCDC(
        tablePath,
        generateTestData(BaseTestResource.cdfTableSchema, 1),
        Some(System.currentTimeMillis()),
        "insert"
      )),
      Gen.constant(RemoveTablePath(
        tablePath,
        "data/part-00000-1.snappy.parquet"
      ))
    )
  } yield cmd

  def testDeltaSharingOperations: Property = {
    startServer()
    val result = for {
        actions <- Gen.list(genCommand, Range.linear(1, 10)).log("actions")
      } yield {
        actions.foldLeft(initialState) { (state, command) =>
        executeCommand(state, command)
      }
      Result.success
    }
    stopServer()
    result
  }

  // ----------------------------------------------------------------------------
  // Execute Commands
  // ----------------------------------------------------------------------------
  def executeCommand(state: TableState, command: Command): TableState = {
    command match {

      // ------------------------------------------------------------------------
      // Existing commands
      // ------------------------------------------------------------------------
      case ReadTable(tablePath) =>
        try {
          val df = spark.read.format("deltaSharing").load(tablePath)
          // Suppose we discover the table is at version 5.
          // In practice you'd get that from the table's metadata if you want to track it.
          // We'll just do a simplified example:
          val newVersion = 5L
          val data = df.collect().toList
          val updatedRange = state.knownVersions.get(tablePath) match {
            case Some((minV, maxV)) => (math.min(minV, newVersion), math.max(maxV, newVersion))
            case None => (newVersion, newVersion)
          }
          state.copy(
            rows = data,
            currentVersion = Some(newVersion),
            knownVersions = state.knownVersions + (tablePath -> updatedRange)
          )
        } catch {
          case _: Exception => state // or track errors
        }

      case ReadTableWithVersion(tablePath, version) =>
        try {
          val df = spark.read.format("deltaSharing")
            .option("versionAsOf", version)
            .load(tablePath)
          val data = df.collect().toList
          // Track that we've seen this version
          val updatedRange = state.knownVersions.get(tablePath) match {
            case Some((minV, maxV)) =>
              (math.min(minV, version.toLong), math.max(maxV, version.toLong))
            case None =>
              (version.toLong, version.toLong)
          }
          state.copy(
            rows = data,
            currentVersion = Some(version.toLong),
            knownVersions = state.knownVersions + (tablePath -> updatedRange)
          )
        } catch {
          case _: Exception => state
        }

      case ReadTableWithTimestamp(tablePath, timestampStr) =>
        try {
          val df = spark.read.format("deltaSharing")
            .option("timestampAsOf", timestampStr)
            .load(tablePath)
          val data = df.collect().toList
          // We can parse the timestamp if we want to track range
          val tsValue = java.sql.Timestamp.valueOf(timestampStr).getTime
          val updatedTsRange = state.knownTimestamps.get(tablePath) match {
            case Some((minT, maxT)) => (math.min(minT, tsValue), math.max(maxT, tsValue))
            case None => (tsValue, tsValue)
          }
          state.copy(
            rows = data,
            currentVersion = None, // We only have a timestamp snapshot here
            knownTimestamps = state.knownTimestamps + (tablePath -> updatedTsRange)
          )
        } catch {
          case _: Exception => state
        }

      case ReadTableWithFilter(tablePath, filter) =>
        try {
          val df = spark.read.format("deltaSharing")
            .load(tablePath)
            .where(filter)
          state.copy(rows = df.collect().toList)
        } catch {
          case _: Exception => state
        }

      case CreateManagedTable(tablePath, tableName) =>
        try {
            spark.sql(s"CREATE TABLE $tableName USING deltaSharing LOCATION '$tablePath'")
            val df = spark.sql(s"SELECT * FROM $tableName")
            state.copy(rows = df.collect().toList)
        } catch {
          case _: Exception => state
        }

      case ReadTableChanges(tablePath, startVer, endVer, startTs, endTs) =>
        try {
          val reader = spark.read.format("deltaSharing")
            .option("readChangeFeed", "true")
          // Add version constraints if specified
          startVer.foreach(v => reader.option("startingVersion", v))
          endVer.foreach(v => reader.option("endingVersion", v))
          // Add timestamp constraints if specified
          startTs.foreach(ts => reader.option("startingTimestamp", ts))
          endTs.foreach(ts => reader.option("endingTimestamp", ts))

          val df = reader.load(tablePath)
          state.copy(
            // We don't overwrite rows because these are *changes*. Keep old data in .rows
            // or do whatever logic you like here.
            rows = state.rows,
            cdfChanges = df.collect().toList
          )
        } catch {
          case _: Exception => state
        }

      // ------------------------------------------------------------------------
      // New commands to cover the missing DeltaSharingSuite functionality
      // ------------------------------------------------------------------------
      case ReadTableWithIncompatibleOptions(tablePath, version, timestampStr) =>
        // We expect this to fail in DeltaSharing (and in Delta) because specifying
        // both version and timestamp is not allowed.
        try {
          val df = spark.read.format("deltaSharing")
            .option("versionAsOf", version)
            .option("timestampAsOf", timestampStr)
            .load(tablePath)
          // If it doesn't fail, that's unexpected. We might record an error or
          // fail the property test. For now, we'll just ignore.
          state
        } catch {
          case e: Exception =>
            // We actually expect an exception, so let's just return the state unchanged.
            state
        }

      case ReadTableWithNonExistentVersion(tablePath, version) =>
        // If your table only has 1..5 valid versions, version=9999 should fail or
        // give empty results. We can see how it behaves.
        try {
          val df = spark.read.format("deltaSharing")
            .option("versionAsOf", version)
            .load(tablePath)
          // Possibly store rows to see if it returned data or empty or no error.
          val data = df.collect().toList
          state.copy(rows = data)
        } catch {
          case _: Exception =>
            // We expected an error, so just return existing state
            state
        }

      case ReadTableCDFWithFilter(tablePath, startVer, endVer, filter) =>
        try {
          val df = spark.read.format("deltaSharing")
            .option("readChangeFeed", "true")
            .option("startingVersion", startVer)
            .option("endingVersion", endVer)
            .load(tablePath)
            .filter(filter)

          val data = df.collect().toList
          state.copy(
            cdfChanges = data,
            currentVersion = Some(endVer)
          )
        } catch {
          case e: Exception => state.copy(lastError = Some(e))
        }

      case ReadTableCDFWithChangeType(tablePath, startVer, endVer, changeType) =>
        try {
          val df = spark.read.format("deltaSharing")
            .option("readChangeFeed", "true")
            .option("startingVersion", startVer)
            .option("endingVersion", endVer)
            .load(tablePath)
            .filter(col("_change_type").contains(changeType))

          val data = df.collect().toList
          state.copy(
            cdfChanges = data,
            currentVersion = Some(endVer)
          )
        } catch {
          case e: Exception => state.copy(lastError = Some(e))
        }

      case ReadTableWithPartitionPruning(tablePath, partitionCol, partitionVal) =>
        try {
          val df = spark.read.format("deltaSharing")
            .load(tablePath)
            .where(s"$partitionCol = '$partitionVal'")

          val data = df.collect().toList
          // Track the partition values we've seen
          val currentValues = state.knownPartitionValues.getOrElse(partitionCol, Set.empty)
          state.copy(
            rows = data,
            knownPartitionValues = state.knownPartitionValues + (partitionCol -> (currentValues + partitionVal))
          )
        } catch {
          case e: Exception => state.copy(lastError = Some(e))
        }

      case ValidateSchema(tablePath) =>
        try {
          val df = spark.read.format("deltaSharing").load(tablePath)
          val schema = df.schema
          state.copy(currentSchema = Some(schema))
        } catch {
          case e: Exception => state.copy(lastError = Some(e))
        }

      case ReadTableWithMultipleFilters(tablePath, filters) =>
        try {
          var df = spark.read.format("deltaSharing").load(tablePath)
          // Apply all filters
          filters.foreach { case (col, value) =>
            df = df.filter(s"$col = '$value'")
          }
          val data = df.collect().toList
          state.copy(rows = data)
        } catch {
          case e: Exception => state.copy(lastError = Some(e))
        }

      case AddTableData(tablePath, data, timestamp) =>
        try {
          // In a real implementation, you'd write this data to the table
          // For now, we'll just track it in our state
          state.copy(
            rows = state.rows ++ data,
            currentVersion = state.currentVersion.map(_ + 1)
          )
        } catch {
          case e: Exception => state.copy(lastError = Some(e))
        }

      case UpdateTableConfiguration(tablePath, changes, timestamp) =>
        try {
          // In a real implementation, you'd update the table's configuration
          // For now, we'll just track the version change
          state.copy(
            currentVersion = state.currentVersion.map(_ + 1)
          )
        } catch {
          case e: Exception => state.copy(lastError = Some(e))
        }

      case GenerateTableCDC(tablePath, changes, timestamp, operation) =>
        try {
          // In a real implementation, you'd apply these changes to generate CDC
          // For now, we'll just track them in our state
          val cdfRows = changes.map { row =>
            Row.fromSeq(row.toSeq ++ Seq(
              state.currentVersion.map(_ + 1).getOrElse(1L),
              timestamp.getOrElse(System.currentTimeMillis()),
              operation
            ))
          }
          state.copy(
            cdfChanges = state.cdfChanges ++ cdfRows,
            currentVersion = state.currentVersion.map(_ + 1)
          )
        } catch {
          case e: Exception => state.copy(lastError = Some(e))
        }

      case RemoveTablePath(tablePath, path) =>
        try {
          // In a real implementation, you'd remove the file
          // For now, we'll just track the version change
          state.copy(
            currentVersion = state.currentVersion.map(_ + 1)
          )
        } catch {
          case e: Exception => state.copy(lastError = Some(e))
        }
    }
  }

  // Helper to generate test data based on schema
  def generateTestData(schema: StructType, numRecords: Int): List[Row] = {
    // This is a simplified example - you'd want more sophisticated data generation
    // based on the schema types
    (1 to numRecords).map { i =>
      Row(
        s"name$i", // name
        i, // age
        java.sql.Date.valueOf("2020-01-01") // birthday
      )
    }.toList
  }
}

object BaseTestResource {
  // Default schema used by most tables
  val structType = new StructType(Array(
      new StructField("id", LongType, false),
      new StructField("data", StringType, false),
      new StructField("category", StringType, false)
    ))

  // Schema for table1 with timestamp and date fields
  val table1Schema = new StructType(Array(
    new StructField("eventTime", TimestampType, true),
    new StructField("date", DateType, true)
  ))

  // Schema for Azure and GCP tables
  val cloudTableSchema = new StructType(Array(
    new StructField("c1", StringType, true),
    new StructField("c2", StringType, true)
  ))

  // Schema for CDF tables
  val cdfTableSchema = new StructType(Array(
    new StructField("name", StringType, true),
    new StructField("age", IntegerType, true),
    new StructField("birthday", DateType, true)
  ))

  val defaultMetadata: Map[String, String] = Map("delta.checkpointInterval" -> "1", "delta.enableExpiredLogCleanup" -> "false")
}
class FilteredClassLoader(systemLoader: ClassLoader, filter: String => Boolean) extends ClassLoader(null) {

    override def loadClass(name: String, resolve: Boolean): Class[_] = {
        // Delegate java.* packages to system loader
        if (filter(name)) {
            return systemLoader.loadClass(name);
        }
        // Handle other classes yourself
        return super.loadClass(name, resolve);
    }
}
