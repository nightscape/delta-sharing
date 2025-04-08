package io.delta.sharing.server

import difflicious.DiffResultPrinter
import io.delta.sharing.client.model.Table
import io.delta.sharing.client.{DeltaSharingClient, model => clientModel}
import io.delta.sharing.server.Differs._
import io.delta.sharing.server.Implicits._
import io.delta.sharing.server.PropertyTest.{Commit, DeltaState, TableState, TestSchema, hadoopConf}
import io.delta.sharing.server.config.{SchemaConfig, ServerConfig, TableConfig}
import io.delta.standalone.Operation
import io.delta.standalone.actions.{AddFile, Metadata, Protocol}
import io.delta.standalone.internal.deltaLogForTableWithClock
import io.delta.standalone.internal.util.newManualClock
import io.delta.standalone.types._
import org.apache.hadoop.fs.{Path, RemoteIterator}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataType => SparkDataType, StructType => SparkStructType}
import scala.collection.JavaConverters._
import zio.{Clock, ZIO}
import zio.test.{Gen, TestResult, assertTrue}

import java.util.concurrent.TimeUnit

object CreateManagedTableCommand {
  def gen(
           basePath: Path,
           state: DeltaState,
           serverConfig: ServerConfig
         ): Gen[Any, StatefulDeterministic.Command[Any, DeltaState]] =
    for {
      schema <- Gen.const("test-schema")
      tableName <- Gen
        .elements("a", "b")
        .map(id => s"table-${id.toLowerCase}")
        .filterNot(state.tables.map(_.tableId).contains)
      tablePath = s"$basePath/$schema/$tableName-${java.lang.System.currentTimeMillis()}"
    } yield new CreateManagedTableCommand(
      tablePath,
      schema,
      tableName,
      serverConfig
    )
}

case class CreateManagedTableCommand(
                                      tablePath: String,
                                      schemaName: String,
                                      tableName: String,
                                      serverConfig: ServerConfig
                                    ) extends StatefulDeterministic.Command[Any, DeltaState] {
  type E = Throwable

  override def update(state: DeltaState): DeltaState = {
    val dummyTimestamp = 0L
    val commit = Commit(
      actions = Seq(
        clientModel.Protocol(1),
        clientModel.Metadata(
          id = "dummy-metadata-id",
          name = tableName,
          description = null,
          schemaString = BaseTestResource.structType.toJson,
          format = clientModel.Format(provider = "parquet")
        )
      ),
      operation = new Operation(Operation.Name.CREATE_TABLE),
      version = 0L,
      timestamp = dummyTimestamp,
      message = "Created test table"
    )
    val tableState = TableState(
      tableId = tableName,
      tablePath = tablePath,
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
        val targetPath = new Path(tablePath)
        val clock = newManualClock
        val deltaLog =
          deltaLogForTableWithClock(hadoopConf, targetPath, clock)
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
        val shareOpt =
          serverConfig.shares.asScala.find(_.name == "test-share")
        val share = shareOpt.getOrElse(
          throw new Exception("Share 'test-share' not found in server config")
        )
        val schemaOpt = share.schemas.asScala.find(_.name == schemaName)
        val schemaConfig = schemaOpt.getOrElse {
          val newSchemaConfig =
            SchemaConfig(schemaName, new java.util.ArrayList[TableConfig]())
          share.schemas.add(newSchemaConfig)
          newSchemaConfig
        }
        val newTableConfig = TableConfig(
          name = tableName,
          location = tablePath,
          id = java.util.UUID.randomUUID().toString,
          historyShared = true,
          startVersion = 0L
        )
        schemaConfig.tables.add(newTableConfig)
      }
      .flatMap { _ =>
        val newState = update(state)
        val tableOpt = newState.schema(schemaName).flatMap(_.table(tableName))
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
      table.tablePath.toString,
      data,
      timestamp
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
                           tablePath: String,
                           data: List[Row],
                           timestamp: Option[Long]
                         ) extends StatefulDeterministic.Command[Any, DeltaState] {
  type E = Throwable
  private val fileName: String =
    s"part-${math.abs(this.hashCode())}.snappy.parquet"
  private val dataPath: Path = new Path(tablePath, "data")
  private val fileUrl: Path = new Path(dataPath, fileName)

  override def update(state: DeltaState): DeltaState = {
    val currentVersion = state
      .table(table.schema, table.name)
      .flatMap(_.currentVersion)
      .getOrElse(0L)
    val newVersion = currentVersion + 1
    val ts = timestamp.getOrElse(0L)
    val dummyAddFile = clientModel.AddFile(
      url = fileUrl.toString,
      id = "SOME_ID",
      partitionValues = Map.empty[String, String],
      size = 1L,
      stats = null
    )
    val commit = Commit(
      actions = Seq(dummyAddFile),
      operation = new Operation(Operation.Name.MANUAL_UPDATE),
      version = newVersion,
      timestamp = ts,
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
        val targetPath = new Path(tablePath)
        // Use our hadoopConf which now points to HDFS instead of the local configuration.
        val fs = targetPath.getFileSystem(hadoopConf)
        fs.mkdirs(dataPath)
        val clock = newManualClock
        val deltaLog =
          deltaLogForTableWithClock(hadoopConf, targetPath, clock)
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
            newDataFile.getPath.toString,
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
        val commit = Commit(
          actions = addFiles.map(addFile =>
            clientModel.AddFile(
              url = addFile.getPath,
              id = "SOME_ID",
              partitionValues = addFile.getPartitionValues.asScala.toMap,
              size = addFile.getSize,
              stats = addFile.getStats
            )
          ),
          operation = new Operation(Operation.Name.MANUAL_UPDATE),
          version = commitResult.getVersion,
          timestamp = ts,
          message = s"Added data at $ts"
        )
        commit
      }
      .map { commit =>
        val newState = update(state)
        val tableOpt = newState.table(table.schema, table.name)
        val addedFileOnStorage =
          commit.actions.head.asInstanceOf[clientModel.AddFile]
        val dtf = tableOpt.get.getCDFFiles()
        val expectedFile = dtf.addFiles.last

        assertTrue(
          tableOpt.isDefined && expectedFile.url == addedFileOnStorage.url &&
            expectedFile.id == addedFileOnStorage.id &&
            expectedFile.partitionValues == addedFileOnStorage.partitionValues
          // expectedFile.size == addedFileOnStorage.size &&
          // expectedFile.version == commit.version &&
          // expectedFile.timestamp == commit.timestamp
        )
      }
  } yield testResult

    def standaloneSchemaToSparkSchema(schema: StructType): SparkStructType =
    SparkDataType.fromJson(schema.toJson()).asInstanceOf[SparkStructType]

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
      val memoryDeltaFiles = dummyNonComparableFields(
        tableOpt.get.getCDFFiles(
          startingVersion,
          endingVersion,
          startingTimestamp,
          endingTimestamp,
          includeHistoricalMetadata = false
        )
      )
      val apiDeltaFiles = dummyNonComparableFields(files)
      val diff = deltaTableFilesDiffer.diff(memoryDeltaFiles, apiDeltaFiles)
      val diffString = DiffResultPrinter.consoleOutput(diff, 2).toString()
      if (!diff.isOk) println(diffString)
      assertTrue(
        tableOpt.isDefined &&
          (diff.isOk || diffString.isEmpty) &&
          endingVersion.forall(v =>
            tableOpt.get.currentVersion.get >= v
          )
      )
    }
  }
  private def dummyNonComparableFields(
                                        deltaTableFiles: clientModel.DeltaTableFiles
                                      ): clientModel.DeltaTableFiles = {
    val dummyVersion = 0L
    val dummyMetadataVersion: java.lang.Long = 0L
    val dummyId = "dummy-id"
    val dummyTimestamp = 0L
    val dummyExpirationTimestamp: java.lang.Long = 0L
    val newMetadata = if (deltaTableFiles.metadata != null) {
      deltaTableFiles.metadata.copy(version = dummyMetadataVersion)
    } else null
    val newAddFiles = deltaTableFiles.addFiles.map { addFile =>
      addFile.copy(
        id = dummyId,
        timestamp = dummyTimestamp,
        expirationTimestamp = dummyExpirationTimestamp
      )
    }
    deltaTableFiles.copy(
      version = dummyVersion,
      metadata = newMetadata,
      addFiles = newAddFiles,
      refreshToken = None
    )
  }
}

