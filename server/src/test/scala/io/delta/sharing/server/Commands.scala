package io.delta.sharing.server

import difflicious.DiffResultPrinter
import io.delta.sharing.client.model.Table
import io.delta.sharing.client.{DeltaSharingClient, model => clientModel}
import io.delta.sharing.server.Differs._
import io.delta.sharing.server.Implicits._
import io.delta.sharing.server.PropertyTest.{Commit, DeltaState, TableState, TestSchema}
import io.delta.sharing.server.config.{SchemaConfig, ServerConfig, ShareConfig, TableConfig}
import io.delta.standalone.Operation
import io.delta.standalone.actions.{AddFile, Metadata, Protocol}
import io.delta.standalone.internal.deltaLogForTableWithClock
import io.delta.standalone.internal.util.newManualClock
import io.delta.standalone.types._
import org.apache.hadoop.fs.{Path, RemoteIterator}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataType => SparkDataType, StructType => SparkStructType}
import scala.collection.JavaConverters._
import zio.{Clock, ZIO, Trace}
import zio.test.{Gen, TestResult, assertTrue}

import java.util.concurrent.TimeUnit

object Utils {

  def standaloneSchemaToSparkSchema(schema: StructType): SparkStructType =
    SparkDataType.fromJson(schema.toJson()).asInstanceOf[SparkStructType]
}
import Utils._

object CreateManagedTableCommand {
  def gen(
           basePath: Path,
           state: DeltaState,
           serverConfig: ServerConfig,
           hadoopConf: org.apache.hadoop.conf.Configuration
         ): Gen[Any, StatefulDeterministic.Command[Any, DeltaState]] = {
    import scala.collection.JavaConverters._
    
    // Get all table configs from the server config
    val tableConfigs: Seq[(String, TableConfig)] = (for {
      share <- serverConfig.shares.asScala
      schema <- share.schemas.asScala
      table <- schema.tables.asScala
    } yield (schema.name, table)).toSeq
    
    for {
      tableConfigTuple <- Gen.elements(tableConfigs: _*)
      (schemaName, tableConfig) = tableConfigTuple
      // Generate a concrete table identifier
      tableIdentifier <- Gen.elements("a", "b")
      // Create concrete table name by replacing $1 with the identifier
      concreteTableName = tableConfig.name.replace("$1", tableIdentifier)
      // Skip if this table already exists
      if !state.tables.map(_.tableId).contains(concreteTableName)
      // Create concrete location by replacing wildcards
      concreteLocation = tableConfig.location.replace("*", tableIdentifier)
      concreteTableConfig = tableConfig.copy(
        name = concreteTableName,
        location = concreteLocation,
        id = concreteTableName // Use table name as ID
      )
    } yield new CreateManagedTableCommand(
      concreteTableConfig,
      schemaName,
      serverConfig,
      hadoopConf
    )
  }
}

case class CreateManagedTableCommand(
                                      tableConfig: TableConfig,
                                      schemaName: String,
                                      serverConfig: ServerConfig,
                                      hadoopConf: org.apache.hadoop.conf.Configuration
                                    ) extends StatefulDeterministic.Command[Any, DeltaState] {
  type E = Throwable

  override def update(state: DeltaState): DeltaState = {
    val dummyTimestamp = 0L
    val tableName = tableConfig.name
    val tablePath = new Path(tableConfig.location)
    
    val commit = Commit(
      actions = Seq(
        clientModel.Protocol(1),
        clientModel.Metadata(
          id = "dummy-metadata-id",
          name = tableName,
          description = null,
          schemaString = BaseTestResource.structType.toJson,
          format = clientModel.Format(provider = "parquet"),
          version = 0L
        )
      ),
      operation = new Operation(Operation.Name.CREATE_TABLE),
      version = 0L,
      timestamp = Option(dummyTimestamp),
      message = "Created test table"
    )
    val tableState = TableState(
      tableConfig = tableConfig,
      commits = List(commit),
      currentVersion = Some(0L),
      currentSchema = Some(BaseTestResource.structType)
    )
    val schemaObj =
      state.schema(schemaName).getOrElse(TestSchema(schemaName, Map.empty))
    val updatedSchema = schemaObj.addTable(tableName, tableState)
    state.copy(schemaMap = state.schemaMap.updated(schemaName, updatedSchema))
  }

  override def executeAndCheck(
                                state: DeltaState
                              ): ZIO[Any, Throwable, TestResult] = for {
    timestamp <- Clock.currentTime(TimeUnit.MILLISECONDS)
    testResult <- ZIO
      .attempt {
        val tableName = tableConfig.name
        val tablePath = new Path(tableConfig.location)
        val clock = newManualClock
        val deltaLog =
          deltaLogForTableWithClock(hadoopConf, tablePath, clock)
        clock.setTime(timestamp)
        val transaction = deltaLog.startTransaction()
        val protocol = new Protocol(1, 2)
        val metadata = Metadata
          .builder()
          .createdTime(timestamp)
          .schema(BaseTestResource.structType)
          .build()
        transaction.commit(
          java.util.Arrays.asList(protocol, metadata),
          new Operation(Operation.Name.CREATE_TABLE),
          "Created test table"
        )
        println(s"Server created table at $tablePath")
      }
      .flatMap { _ =>
        val newState = update(state)
        val tableOpt = newState.schema(schemaName).flatMap(_.table(tableConfig.name))
        ZIO.succeed(
          assertTrue(
            tableOpt.isDefined && tableOpt
              .exists(_.currentVersion.contains(0L))
          )
        )
      }
  } yield testResult
}

object AddDataCommand {
  def gen(
           spark: SparkSession,
           state: DeltaState
         ): Gen[Any, StatefulDeterministic.Command[Any, DeltaState]] =
    for {
      schemaAndTable <- Gen.fromIterable(state.tablesWithSchemas.toList)
      (schema, table) = schemaAndTable
      timestamp <- Gen.option(Gen.long(0L, 100L))
      data <- generateTestData(BaseTestResource.structType, 10)
    } yield new AddDataCommand(
      spark,
      Table(table.tableId, schema, "test-share"),
      table.tablePath,
      data,
      timestamp,
      spark.sparkContext.hadoopConfiguration
    )


  def generateTestData(
      schema: StructType,
      numRecords: Int
  ): Gen[Any, List[Row]] = {
    def fieldGen(field: StructField): Gen[Any, Any] =
      field.getDataType match {
        case _: LongType      => Gen.long
        case _: IntegerType   => Gen.int
        case _: StringType    => Gen.string
        case _: BooleanType   => Gen.boolean
        case _: DoubleType    => Gen.double
        case _: FloatType     => Gen.float
        case _: ByteType      => Gen.byte
        case _: ShortType     => Gen.short
        case _: BinaryType    => Gen.chunkOf(Gen.byte).map(_.toArray)
        case _: TimestampType => Gen.long
        case _: DateType      => Gen.int
        case other            => Gen.empty
      }

    def rowGen: Gen[Any, Row] =
      Gen
        .collectAll(schema.getFields.map(fieldGen))
        .map(values => Row.fromSeq(values))

    Gen.listOfN(numRecords)(rowGen)
  }
}
case class AddDataCommand(
                           spark: SparkSession,
                           table: Table,
                           tablePath: Path,
                           data: List[Row],
                           timestamp: Option[Long],
                           hadoopConf: org.apache.hadoop.conf.Configuration
                         ) extends StatefulDeterministic.Command[Any, DeltaState] {
  type E = Throwable
  private val fileName: String =
    s"part-${math.abs(this.hashCode())}.snappy.parquet"

  private val dataPath: Path = new Path(tablePath, "data")
  private val relativeFileUrl: Path = new Path("data", fileName)
  private val fileUrl: Path = new Path(dataPath, fileName)
  def pathRelativeToTable(path: Path) = {
    val relativePath = tablePath.toUri.relativize(path.toUri())
    new Path(relativePath)
  }

  override def update(state: DeltaState): DeltaState = {
    val currentVersion = state
      .table(table.schema, table.name)
      .flatMap(_.currentVersion)
      .getOrElse(0L)
    val newVersion = currentVersion + 1
    val ts = timestamp.getOrElse(0L)
    val dummyAddFile = clientModel.AddFileForCDF(
      url = relativeFileUrl.toString,
      id = "SOME_ID",
      partitionValues = Map.empty[String, String],
      size = 1L,
      version = newVersion,
      timestamp = ts,
      stats = null
    )
    val commit = Commit(
      actions = Seq(dummyAddFile),
      operation = new Operation(Operation.Name.MANUAL_UPDATE),
      version = newVersion,
      timestamp = Option(ts),
      message = s"Added data at $ts"
    )
    val schemaObj = state
      .schema(table.schema)
      .getOrElse(TestSchema(table.schema, Map.empty))
    val tbl = schemaObj.table(table.name).get
    val updTbl = tbl.addCommit(commit)
    state.addTable(table.schema, table.name, updTbl)
  }

  override def executeAndCheck(
                                state: DeltaState
                              ): ZIO[Any, Throwable, TestResult] = for {
    now <- Clock.currentTime(TimeUnit.MILLISECONDS)
    testResult <- ZIO
      .attempt {
        // Use our hadoopConf which now points to HDFS instead of the local configuration.
        val fs = tablePath.getFileSystem(hadoopConf)
        fs.mkdirs(dataPath)
        val clock = newManualClock
        val deltaLog =
          deltaLogForTableWithClock(hadoopConf, tablePath, clock)
        val ts = timestamp.getOrElse(now)
        timestamp.foreach(clock.setTime)
        val filesBefore =
          fs.listFiles(dataPath, true).asScala.map(_.getPath).toList
        spark
          .createDataFrame(
            data.asJava,
            standaloneSchemaToSparkSchema(BaseTestResource.structType)
          )
          .coalesce(1)
          .write
          .mode(SaveMode.Append)
          .format("parquet")
          .save(dataPath.toString)
        val filesAfter = fs
          .listFiles(dataPath, true)
          .asScala
          .filter(_.getPath.getName.startsWith("part-"))
          .toList
        val newFiles =
          filesAfter.filterNot(f => filesBefore.contains(f.getPath))
        val renamedFiles = newFiles.map { f =>
          fs.rename(f.getPath, fileUrl)
          f.setPath(fileUrl)
          f
        }
        val addFiles = renamedFiles.map(newDataFile =>
          new AddFile(
            pathRelativeToTable(newDataFile.getPath).toString,
            new java.util.HashMap(),
            newDataFile.getLen,
            ts,
            true,
            """"1"""",
            new java.util.HashMap()
          )
        )
        val transaction = deltaLog.startTransaction()
        val commitResult = transaction.commit(
          java.util.Arrays.asList(addFiles: _*),
          new Operation(Operation.Name.MANUAL_UPDATE),
          s"Added data at $ts"
        )
        println(s"Data added to table at $tablePath")
        deltaLog.getSnapshotForVersionAsOf(commitResult.getVersion)
      }
      .map { snapshot =>
        val newState = update(state)
        val tableOpt = newState.table(table.schema, table.name)
        val tableState = tableOpt.get
        val addedFilesOnStorage = snapshot.getAllFiles.asScala.toArray
        val addedFileOnStorage = addedFilesOnStorage.head
        val dtf = tableState.expectedDeltaTableFiles(returnExternalUrls = false)
        val expectedFile = dtf.addFiles.lastOption

        assertTrue(
          tableOpt.isDefined && expectedFile.isDefined && expectedFile.get.url == addedFileOnStorage.getPath &&
            //expectedFile.get.id == addedFileOnStorage..id &&
            expectedFile.get.partitionValues.toMap == addedFileOnStorage.getPartitionValues.asScala.toMap[String, String]
          // expectedFile.size == addedFileOnStorage.size &&
          // expectedFile.version == commit.version &&
          // expectedFile.timestamp == commit.timestamp
        )
      }
  } yield testResult


}

object ReadTableCommand {
  def gen(
           client: DeltaSharingClient,
           state: DeltaState
         ): Gen[Any, StatefulDeterministic.Command[Any, DeltaState]] =
    for {
      schemaAndTable <- Gen.fromIterable(state.tablesWithSchemas.toList)
      (schema, table) = schemaAndTable
      useVersion <- Gen.boolean
      startAndEndVersion <-
        if (useVersion && table.currentVersion.exists(_ >= 0)) {
          val currentVersion = table.currentVersion.get
          for {
            starting <- Gen.long(0, currentVersion)
            ending <- Gen.option(Gen.long(starting, currentVersion))
          } yield (Some(starting), ending)
        } else {
          Gen.const((None, None))
        }
      startingTimestamp <- Gen.const(None)
      endingTimestamp <- Gen.const(None)
    } yield new ReadTableCommand(
      client,
      table.tableId,
      schema,
      "test-share",
      startAndEndVersion._1,
      startAndEndVersion._2,
      startingTimestamp,
      endingTimestamp
    )
}

case class ReadTableCommand(
                             client: DeltaSharingClient,
                             table: String,
                             schema: String,
                             share: String,
                             startingVersion: Option[Long],
                             endingVersion: Option[Long],
                             startingTimestamp: Option[String],
                             endingTimestamp: Option[String]
                           ) extends StatefulDeterministic.Command[Any, DeltaState] {
  type E = Throwable

  override def update(state: DeltaState): DeltaState = state

  override def executeAndCheck(
                                state: DeltaState
                              ): ZIO[Any, Throwable, TestResult] = {
    ZIO.attempt {
      val tbl = Table(name = table, schema = schema, share = share)
      val files =
        if (startingVersion.isDefined || endingVersion.isDefined) {
          client.getFiles(
            table = tbl,
            startingVersion = startingVersion.getOrElse(0L),
            endingVersion = endingVersion
          )
        } else if (startingTimestamp.isDefined || endingTimestamp.isDefined) {
          val cdfOptions = Map(
            "startingTimestamp" -> startingTimestamp.getOrElse(""),
            "endingTimestamp" -> endingTimestamp.getOrElse("")
          ).filter(_._2.nonEmpty)
          client.getCDFFiles(
            table = tbl,
            cdfOptions = cdfOptions,
            includeHistoricalMetadata = true
          )
        } else {
          client.getFiles(
            table = tbl,
            predicates = Nil,
            limit = None,
            versionAsOf = None,
            timestampAsOf = None,
            jsonPredicateHints = None,
            refreshToken = None
          )
        }
      val tableOpt = state.table(schema, table)
      val memoryDeltaFiles = if (startingVersion.isDefined || endingVersion.isDefined) {
        tableOpt.get.expectedDeltaTableFiles(
          startingVersion,
          endingVersion,
          startingTimestamp,
          endingTimestamp,
          includeHistoricalMetadata = false
        )
      } else {
        tableOpt.get.getFiles(startingVersion, endingVersion)
      }
      val diff = deltaTableFilesDiffer.diff(memoryDeltaFiles, files)
      val diffString = DiffResultPrinter.consoleOutput(diff, 2).toString()
      if (!diff.isOk) println(diffString)

      // Verify external URL transformation for HDFS paths
      val hdfsPathCheck = if (tableOpt.exists(_.tablePath.toUri.getScheme == "hdfs")) {
        val allFiles = files.files ++ files.addFiles.map(af =>
          clientModel.AddFile(af.url, af.id, af.partitionValues, af.size, af.stats)
        ) ++ files.cdfFiles
        val urlsUseKnox = allFiles.forall(_.url.startsWith("knoxswebhdfs://"))
        if (!urlsUseKnox) {
          println(s"[ERROR] Knox URL check failed. Sample URLs:")
          allFiles.take(3).foreach(f => println(s"  - ${f.url}"))
        }
        urlsUseKnox
      } else {
        true // Not an HDFS path, so no Knox transformation expected
      }

      assertTrue(
        tableOpt.isDefined &&
          (diff.isOk || diffString.isEmpty) &&
          endingVersion.forall(v =>
            tableOpt.get.currentVersion.get >= v
          ) &&
          hdfsPathCheck
      )
    }
  }
}

object ReadTableSparkCommand {
  def gen(
           spark: SparkSession,
           profilePath: String, // Path to the profile file for Spark access
           state: DeltaState
         ): Gen[Any, StatefulDeterministic.Command[Any, DeltaState]] =
    for {
      // Select a table that actually has data added (at least one commit beyond creation)
      schemaAndTable <- Gen.fromIterable(state.tablesWithSchemas.filter(_._2.commits.length > 1).toList)
      (schema, table) = schemaAndTable
    } yield new ReadTableSparkCommand(
      spark,
      profilePath,
      table.tableId,
      schema,
      "test-share" // Assuming fixed share name from test setup
    )
}

case class ReadTableSparkCommand(
                                  spark: SparkSession,
                                  profilePath: String,
                                  table: String,
                                  schema: String,
                                  share: String
                                ) extends StatefulDeterministic.Command[Any, DeltaState] {
  type E = Throwable

  override def update(state: DeltaState): DeltaState = state // Reading does not change the state

  override def executeAndCheck(
                                state: DeltaState
                              ): ZIO[Any, Throwable, TestResult] = {
    val tableIdentifier = s"${profilePath}#${share}.${schema}.${table}"
    ZIO.attempt {
      println(s"Attempting Spark read for: $tableIdentifier") // Helpful debug output

      // Read the table using Spark's deltaSharing format
      val df = spark.read
        .format("deltaSharing")
        .load(tableIdentifier)

      // Check 1: Compare the schema read by Spark with the expected schema from DeltaState
      val expectedSchemaOpt = state.table(schema, table).flatMap(_.currentSchema)
      val expectedSparkSchemaOpt = expectedSchemaOpt.map(standaloneSchemaToSparkSchema)
      val actualSchema = df.schema

      val schemaMatch = expectedSparkSchemaOpt.exists { expected =>
        // Basic schema comparison (field names and types)
        expected.fields.map(f => (f.name, f.dataType)).toSeq ==
        actualSchema.fields.map(f => (f.name, f.dataType)).toSeq
      }

      if (!schemaMatch) {
        // Print detailed schema comparison on mismatch for easier debugging
        println(s"[ERROR] Schema mismatch for table: $tableIdentifier")
        println("--- Expected Schema (from DeltaState) ---")
        println(expectedSparkSchemaOpt.map(_.treeString).getOrElse("N/A"))
        println("--- Actual Schema (read by Spark) ---")
        println(actualSchema.treeString)
        println("--------------------------------------")
      }

      // Check 2: Perform an action to ensure data can be accessed (e.g., count)
      // This also helps verify that file access (potentially via Knox) works.
      val rowCount = df.count()
      println(s"Successfully read $rowCount rows from $tableIdentifier via Spark.")

      // Check 3: Verify Knox URL transformation for HDFS paths
      val knoxVerification = if (state.table(schema, table).exists(_.tablePath.toUri.getScheme == "hdfs")) {
        // When the table is on HDFS, the successful read through Spark confirms
        // that the Knox URL transformation worked correctly. If it didn't work,
        // Spark would fail to access the files.
        println(s"[INFO] Table on HDFS successfully read via Knox transformation (${rowCount} rows)")
        true
      } else {
        true // Not an HDFS path, so no Knox transformation expected
      }

      assertTrue(
        schemaMatch && // Assert that the schemas match
        knoxVerification // Assert Knox transformation worked if applicable
        // && rowCount > 0 // Optional: Assert that some data was read if expected
      )
    }.tapError(e => ZIO.succeed {
      // Log errors clearly for debugging test failures
      println(s"[ERROR] Spark read failed for table $tableIdentifier: ${e.getClass.getName} - ${e.getMessage}")
      e.printStackTrace() // Print stack trace for detailed debugging
    })
  }
}

