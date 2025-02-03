package io.delta.sharing.server

import difflicious._
import difflicious.implicits._
import io.delta.sharing.client.model.Table
import io.delta.sharing.client.{DeltaSharingClient, DeltaSharingRestClient}
import io.delta.standalone.Operation
import io.delta.standalone.actions._
import io.delta.standalone.internal._
import io.delta.standalone.internal.util.newManualClock
import io.delta.standalone.types._
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{
  DataType => SparkDataType,
  StructType => SparkStructType
}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import zio.{Differ => _, _}
import zio.test._

import java.io.File
import java.net.ServerSocket
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.collection.JavaConverters._
import io.delta.sharing.server.config.ServerConfig
import io.delta.sharing.server.config.TableConfig
import io.delta.sharing.server.config.SchemaConfig
import io.delta.sharing.server.config.ShareConfig
import io.delta.sharing.server.config.Authorization
import org.apache.hadoop.fs.RemoteIterator
import scala.collection.mutable.ArrayBuffer
import io.delta.sharing.client.{model => clientModel}
import io.delta.sharing.client.DeltaSharingProfile

object PropertyTest extends ZIOSpecDefault {
  private val format = "delta"
  private val spark = SparkSession.builder().master("local[4]").getOrCreate()

  val hadoopConf = new Configuration()

  def findFreePort(): Task[Int] = {
    val socket = new ServerSocket(0)
    ZIO
      .attempt {
        socket.getLocalPort
      }
      .ensuring(ZIO.attempt(socket.close()).orDie)
  }

  final case class TestServerConfig(
      port: Int,
      profileFile: File,
      stop: Runnable
  )

  final case class DeltaServerEnvironment(
      port: Int,
      profileFile: File,
      serverConfig: ServerConfig
  )

  val serverConfigLayer
      : ZLayer[Scope with TestTables, Throwable, DeltaServerEnvironment] =
    ZLayer.scoped {
      for {
        port <- findFreePort()
        token = "dapi5e3574ec767ca1548ae5bbed1a2dc04d"
        profileFile <- ZIO.attempt {
          val file = Files.createTempFile("delta-test", ".share").toFile
          FileUtils.writeStringToFile(
            file,
            s"""{
             |  "shareCredentialsVersion": 1,
             |  "endpoint": "http://localhost:$port/delta-sharing",
             |  "bearerToken": "$token"
             |}""".stripMargin,
            StandardCharsets.UTF_8
          )
          file
        }
        testTables <- ZIO.service[TestTables]
        basePath = testTables.basePath
        serverConfig = ServerConfig(
          version = 1,
          shares = java.util.Arrays.asList(
            ShareConfig(
              "test-share",
              new java.util.ArrayList()
            )
          ),
          authorization = Authorization(token),
          ssl = null, // No SSL configuration provided.
          host = "localhost",
          port = port,
          endpoint = "/delta-sharing",
          preSignedUrlTimeoutSeconds = 3600L,
          deltaTableCacheSize = 10,
          stalenessAcceptable = false,
          evaluatePredicateHints = true,
          evaluateJsonPredicateHints = true,
          evaluateJsonPredicateHintsV2 = true,
          requestTimeoutSeconds = 30L,
          queryTablePageSizeLimit = 10000,
          queryTablePageTokenTtlMs = 259200000,
          refreshTokenTtlMs = 3600000
        )
      } yield DeltaServerEnvironment(port, profileFile, serverConfig)
    }

  val serverLayer
      : ZLayer[Scope with DeltaServerEnvironment, Throwable, TestServerConfig] =
    ZLayer.scoped {
      for {
        env <- ZIO.service[DeltaServerEnvironment]
        promise <- Promise.make[Throwable, Unit]
        fiber <- (for {
          stopServer <- ZIO.attempt {
            println("serverConfig=" + env.serverConfig)
            val server = DeltaSharingService.start(env.serverConfig)
            new Runnable {
              def run(): Unit = server.stop()
            }
          }
          _ = println(s"Server is running on port ${env.port}")
          _ <- promise.succeed(())
        } yield stopServer).fork
        _ <- promise.await.timeoutFail(
          new Exception("Server didn't start in time")
        )(120.seconds)
        stopServer <- fiber.await.map(
          _.getOrElse(_ =>
            new Runnable {
              def run(): Unit = ()
            }
          )
        )
      } yield TestServerConfig(env.port, env.profileFile, stopServer)
    }

  final case class TestTables(basePath: Path)

  val testTablesLayer: ZLayer[Scope, Throwable, TestTables] = ZLayer.scoped {
    ZIO.acquireRelease {
      val bp = Files.createTempDirectory(s"${format}-test-tables")
      ZIO.succeed(TestTables(new Path(bp.toString)))
    } { testTables =>
      ZIO.attempt {
        val fs = testTables.basePath.getFileSystem(hadoopConf)
        fs.delete(testTables.basePath, true)
      }.orDie
    }
  }

  val overallLayer: ZLayer[
    Scope,
    Throwable,
    Scope with TestTables with DeltaServerEnvironment with TestServerConfig with ServiceEndpoints with Subject
  ] =
    ZLayer.makeSome[
      Scope,
      Scope with TestTables with DeltaServerEnvironment with TestServerConfig
    ](
      testTablesLayer,
      serverConfigLayer,
      serverLayer
    )

  implicit val protocolDiffer: Differ[clientModel.Protocol] = Differ.alwaysIgnore
  implicit val metadataDiffer: Differ[clientModel.Metadata] = Differ.alwaysIgnore
  implicit val addFileDiffer: Differ[clientModel.AddFile] = Differ.useEquals[clientModel.AddFile](_.toString())
  implicit val addFileForCdfDiffer: Differ[clientModel.AddFileForCDF] = Differ.useEquals[clientModel.AddFileForCDF](_.toString())
  implicit val addFilesForCdfSeqDiffer: Differ[Seq[clientModel.AddFileForCDF]] = Differ.seqDiffer[Seq, clientModel.AddFileForCDF].pairBy(_.url)
  implicit val addCdcFileDiffer: Differ[clientModel.AddCDCFile] = Differ.useEquals[clientModel.AddCDCFile](_.toString())
  implicit val addCdcFileSeqDiffer: Differ[Seq[clientModel.AddCDCFile]] = Differ.seqDiffer[Seq, clientModel.AddCDCFile].pairBy(_.url)
  implicit val removeFileDiffer: Differ[clientModel.RemoveFile] = Differ.useEquals[clientModel.RemoveFile](_.toString())
  implicit val deltaTableFilesDiffer: Differ[clientModel.DeltaTableFiles] = Differ.derived[clientModel.DeltaTableFiles]
  case class TableState(
      tableId: String,
      tablePath: String,
      commits: List[Commit] = Nil,
      currentVersion: Option[Long] = None,
      currentSchema: Option[StructType] = None,
      knownPartitionValues: Map[String, Set[String]] = Map.empty,
  ) {
    // Convenience method for appending a commit
    def addCommit(commit: Commit): TableState =
      copy(commits = commits :+ commit, currentVersion = currentVersion.orElse(Some(0L)).map(_ + 1))

    /** Updated replay to support legacy AddFile, AddFileForCDF and AddCDCFile *
      */
    def getFiles(
        startingVersion: Option[Long],
        endingVersion: Option[Long]
    ): clientModel.DeltaTableFiles = {
      if (commits.size < 2) {
        throw new IllegalStateException(
          "Commits data is incomplete, missing protocol or metadata headers"
        )
      }
      val respondedFormat = DeltaSharingRestClient.RESPONSE_FORMAT_PARQUET

      // Header information from the first commit.
      val firstCommit = commits.head
      val protocol = firstCommit.actions.head.asInstanceOf[clientModel.Protocol]
      if (protocol.minReaderVersion > DeltaSharingProfile.CURRENT) {
        throw new IllegalArgumentException(
          s"The table requires a newer version ${protocol.minReaderVersion} to read. " +
            s"But the current release supports version ${DeltaSharingProfile.CURRENT} and below. " +
            "Please upgrade to a newer release."
        )
      }
      val metadata = firstCommit.actions(1).asInstanceOf[clientModel.Metadata]

      val addFiles = ArrayBuffer[clientModel.AddFileForCDF]()
      val cdcFiles = ArrayBuffer[clientModel.AddCDCFile]()
      val removeFiles = ArrayBuffer[clientModel.RemoveFile]()
      val additionalMetadatas = ArrayBuffer[clientModel.Metadata]()
      var version = 0L
      commits
        .filter(c =>
          startingVersion.forall(_ <= c.version) && endingVersion.forall(
            _ >= c.version
          )
        )
        .foreach { commit =>
          commit.actions.foreach {
            case a: clientModel.AddFile =>
              // Legacy action – convert it on the fly.
              version = math.max(version, commit.version)
              addFiles.append(
                clientModel.AddFileForCDF(
                  url = a.url,
                  id = a.id,
                  partitionValues = a.partitionValues,
                  size = a.size,
                  expirationTimestamp = null,
                  version = commit.version,
                  timestamp = commit.timestamp,
                  stats = a.stats
                )
              )
            case a: clientModel.AddFileForCDF =>
              version = math.max(version, a.version)
              addFiles.append(a)
            case c: clientModel.AddCDCFile =>
              version = math.max(version, c.version)
              cdcFiles.append(c)
            case r: clientModel.RemoveFile =>
              version = math.max(version, r.version)
              removeFiles.append(r)
            case m: clientModel.Metadata =>
              version = math.max(version, m.version)
              additionalMetadatas.append(m)
            case other =>
              throw new IllegalStateException(
                s"Unexpected action encountered: $other"
              )
          }
        }
      val finalVersion = endingVersion.getOrElse(version)
      clientModel.DeltaTableFiles(
        version = finalVersion,
        protocol = protocol,
        metadata = metadata,
        addFiles = addFiles.toSeq,
        cdfFiles = cdcFiles.toSeq,
        removeFiles = removeFiles.toSeq,
        additionalMetadatas = additionalMetadatas.toSeq,
        respondedFormat = respondedFormat
      )
    }

    /** Updated getCDFFiles with similar changes * */
    def getCDFFiles(
        startingVersion: Option[Long] = None,
        endingVersion: Option[Long] = None,
        startingTimestamp: Option[String] = None,
        endingTimestamp: Option[String] = None,
        includeHistoricalMetadata: Boolean = false
    ): clientModel.DeltaTableFiles = {
      if (commits.size < 1) {
        throw new IllegalStateException(
          "Commits data is incomplete, missing protocol or metadata headers"
        )
      }
      val respondedFormat = DeltaSharingRestClient.RESPONSE_FORMAT_PARQUET
      val firstCommit = commits.head
      val protocol = firstCommit.actions.head.asInstanceOf[clientModel.Protocol]
      if (protocol.minReaderVersion > DeltaSharingProfile.CURRENT) {
        throw new IllegalArgumentException(
          s"The table requires a newer version ${protocol.minReaderVersion} to read. " +
            s"But the current release supports version ${DeltaSharingProfile.CURRENT} and below. " +
            "Please upgrade to a newer release."
        )
      }
      val metadata = firstCommit.actions(1).asInstanceOf[clientModel.Metadata]

      val addFiles = ArrayBuffer[clientModel.AddFileForCDF]()
      val cdcFiles = ArrayBuffer[clientModel.AddCDCFile]()
      val removeFiles = ArrayBuffer[clientModel.RemoveFile]()
      val additionalMetadatas = ArrayBuffer[clientModel.Metadata]()
      var version = 0L
      commits.drop(1).foreach { commit =>
        if (
          startingVersion.forall(_ <= commit.version) &&
          endingVersion.forall(_ >= commit.version)
          //startingTimestamp.forall(ts => commit.timestamp >= ts) &&
          //endingTimestamp.forall(ts => commit.timestamp <= ts)
        ) {
          commit.actions.foreach {
            case a: clientModel.AddFile =>
              version = math.max(version, commit.version)
              addFiles.append(
                clientModel.AddFileForCDF(
                  url = a.url,
                  id = a.id,
                  partitionValues = a.partitionValues,
                  size = a.size,
                  expirationTimestamp = null,
                  version = commit.version,
                  timestamp = commit.timestamp,
                  stats = a.stats
                )
              )
            case a: clientModel.AddFileForCDF =>
              version = math.max(version, a.version)
              addFiles.append(a)
            case c: clientModel.AddCDCFile =>
              version = math.max(version, c.version)
              cdcFiles.append(c)
            case r: clientModel.RemoveFile =>
              version = math.max(version, r.version)
              removeFiles.append(r)
            case m: clientModel.Metadata =>
              version = math.max(version, m.version)
              if (includeHistoricalMetadata) additionalMetadatas.append(m)
            case other =>
              throw new IllegalStateException(
                s"Unexpected action encountered in CDF: $other"
              )
          }
        }
      }
      val finalVersion = version
      clientModel.DeltaTableFiles(
        version = finalVersion,
        protocol = protocol,
        metadata = metadata,
        addFiles = addFiles.toSeq,
        cdfFiles = cdcFiles.toSeq,
        removeFiles = removeFiles.toSeq,
        additionalMetadatas = additionalMetadatas.toSeq,
        respondedFormat = respondedFormat
      )
    }
  }

  case class TestSchema(schemaId: String, tables: Map[String, TableState]) {
    def table(id: String): Option[TableState] = tables.get(id)
    def addTable(name: String, tableState: TableState): TestSchema =
      copy(tables = tables + (name -> tableState))
    def removeTable(id: String): TestSchema =
      copy(tables = tables.filter(_._1 != id))
  }

  case class TestState(
    deltaState: DeltaState,
  )
  case class DeltaState(
      schemaMap: Map[String, TestSchema] = Map(
        "test-schema" -> TestSchema("test-schema", Map.empty)
      )
  ) {
    val defaultSchemaId: String = "test-schema"
    def schemas: Set[String] = schemaMap.keySet
    def deletableSchemas: Set[String] = schemas.filter(_ != defaultSchemaId)
    def schema(id: String): Option[TestSchema] = schemaMap.get(id)
    def addSchema(id: String): DeltaState =
      if (schemaMap.contains(id)) this
      else copy(schemaMap = schemaMap + (id -> TestSchema(id, Map.empty)))
    def canRemoveSchema(id: String): Boolean = id != defaultSchemaId
    def removeSchema(id: String): DeltaState =
      if (canRemoveSchema(id)) copy(schemaMap = schemaMap - id) else this

    def tables: Set[TableState] =
      schemaMap.values.flatMap(_.tables.values).toSet
    def tableNames: Set[String] =
      schemaMap.values.flatMap(_.tables.keys).toSet
    def tablesWithSchemas: Set[(String, TableState)] =
      schemaMap.values.flatMap { schema =>
        schema.tables.map { case (_, tableVar) => (schema.schemaId, tableVar) }
      }.toSet
    def tablesIn(schemaId: String): Set[TableState] =
      schemaMap.get(schemaId).map(_.tables.values.toSet).getOrElse(Set.empty)
    def tableNamesIn(schemaId: String): Set[String] =
      schemaMap.get(schemaId).map(_.tables.keys.toSet).getOrElse(Set.empty)
    def table(schemaId: String, tableId: String): Option[TableState] =
      schemaMap.get(schemaId).flatMap(_.tables.get(tableId))
    def addTable(
        schemaId: String,
        tableId: String,
        tableState: TableState
    ): DeltaState =
      copy(schemaMap =
        schemaMap.updated(
          schemaId,
          schemaMap
            .getOrElse(schemaId, TestSchema(schemaId, Map.empty))
            .addTable(tableId, tableState)
        )
      )
    def removeTable(schemaId: String, tableId: String): DeltaState = if (
      !schemaMap.contains(schemaId)
    ) this
    else
      copy(schemaMap =
        schemaMap.updated(schemaId, schemaMap(schemaId).removeTable(tableId))
      )
  }

  object DeltaState {
    def empty: DeltaState = DeltaState()
  }

  final case class Commit(
      actions: Seq[clientModel.Action],
      operation: Operation,
      version: Long,
      timestamp: Long,
      message: String
  ) {}

  object CreateManagedTableCommand {
    def gen(
        basePath: Path,
        state: DeltaState,
        serverConfig: ServerConfig
    ): Gen[Any, Command[Any, DeltaState]] =
      for {
        schema <- Gen.const("test-schema")
        // Only generate few different table names
        // so we get more actions per table
        tableName <- Gen
          .elements("a", "b")
          .map(id => s"table-${id.toLowerCase}")
          .filterNot(state.tables.map(_.tableId).contains)
        tablePath = s"$basePath/$schema/$tableName"
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
  ) extends Command[Any, DeltaState] {
    type E = Throwable
    type Result = CommandResult[DeltaState]
    override def execute(): ZIO[Any, Throwable, CommandResult[DeltaState]] =
      ZIO.attempt {
        val targetPath = new org.apache.hadoop.fs.Path(tablePath)
        val clock = newManualClock
        val deltaLog = deltaLogForTableWithClock(hadoopConf, targetPath, clock)
        val timestamp = java.lang.System.currentTimeMillis()
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
        // Create a commit representing the creation of the table.
        val commit = Commit(
          actions = Seq(
            clientModel.Protocol(protocol.getMinReaderVersion),
            clientModel.Metadata(
              id = metadata.getId,
              name = metadata.getName,
              description = metadata.getDescription,
              schemaString = metadata.getSchema.toJson,
              format = clientModel.Format(provider = "parquet")
            )
          ),
          operation = new Operation(Operation.Name.CREATE_TABLE),
          version = 0L,
          timestamp = timestamp,
          message = "Created test table"
        )
        // Build the initial table state using the commit.
        val tableState = TableState(
          tableId = tableName,
          tablePath = tablePath,
          commits = List(commit),
          currentVersion = Some(commit.version),
          currentSchema = Some(BaseTestResource.structType)
        )

        // Modify the existing share configuration rather than adding a new one.
        val shareOpt = serverConfig.shares.asScala.find(_.name == "test-share")
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

        CreateManagedTableCommandResult(schemaName, tableName, tableState)
      }
  }

  final case class CreateManagedTableCommandResult(
      schemaName: String,
      tableName: String,
      output: TableState
  ) extends CommandResult[DeltaState] {
    override def update(s: DeltaState): DeltaState = {
      val schemaObj =
        s.schema(schemaName).getOrElse(TestSchema(schemaName, Map.empty))
      val updatedSchema = schemaObj.addTable(tableName, output)
      s.copy(schemaMap = s.schemaMap.updated(schemaName, updatedSchema))
    }
    override def ensure(s: DeltaState): TestResult = {
      val tableOpt = s.schema(schemaName).flatMap(_.table(tableName))
      assertTrue(
        tableOpt.isDefined && tableOpt.exists(_.currentVersion.contains(0L))
      )
    }
  }

  object AddDataCommand {
    def gen(
        spark: SparkSession,
        state: DeltaState
    ): Gen[Any, Command[Any, DeltaState]] =
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
  }

  implicit class RichRemoteIterator[T](iter: RemoteIterator[T]) {
    def asScala: Iterator[T] = {
      new Iterator[T] {
        def hasNext: Boolean = iter.hasNext
        def next(): T = iter.next()
      }
    }
  }

  case class AddDataCommand(
      spark: SparkSession,
      table: Table,
      tablePath: String,
      data: List[Row],
      timestamp: Option[Long]
  ) extends Command[Any, DeltaState] {
    type E = Throwable
    type Result = CommandResult[DeltaState]
    override def execute(): ZIO[Any, Throwable, CommandResult[DeltaState]] =
      ZIO.attempt {
        val targetPath = new org.apache.hadoop.fs.Path(tablePath)
        val fs =
          targetPath.getFileSystem(new org.apache.hadoop.conf.Configuration())
        val dataPath = new org.apache.hadoop.fs.Path(targetPath, "data")
        fs.mkdirs(dataPath)
        val clock = newManualClock
        val deltaLog = deltaLogForTableWithClock(
          new org.apache.hadoop.conf.Configuration(),
          targetPath,
          clock
        )
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
          .map(_.getPath)
          .filter(_.getName != "_SUCCESS")
          .toList
        val newFiles = filesAfter.filterNot(filesBefore.contains)
        val ts = timestamp.getOrElse(java.lang.System.currentTimeMillis())
        val addFiles = newFiles.map(newDataFile =>
          new AddFile(
            newDataFile.toString,
            new java.util.HashMap(),
            fs.getFileStatus(newDataFile).getLen,
            ts,
            true,
            "1",
            new java.util.HashMap()
          )
        )
        val transaction = deltaLog.startTransaction()
        val commitResult = transaction.commit(
          java.util.Arrays.asList(addFiles: _*),
          new Operation(Operation.Name.MANUAL_UPDATE),
          s"Added data at $ts"
        )
        // Create a commit representing this data addition.
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
        AddDataCommandResult(table, commit)
      }
  }

  final case class AddDataCommandResult(
      table: Table,
      commit: Commit
  ) extends CommandResult[DeltaState] {
    override def update(s: DeltaState): DeltaState = {
      val schemaObj = s.schema(table.schema).get
      val tbl = schemaObj.table(table.name).get
      // Use the convenience method to add the commit to the table.
      val updTbl = tbl.addCommit(commit)
      s.addTable(table.schema, table.name, updTbl)
    }
    override def ensure(s: DeltaState): TestResult = {
      val tableOpt = s.table(table.schema, table.name)
      assertTrue(tableOpt.isDefined)
      // Collect the single file action that we generated.
      val expected = commit.actions.head.asInstanceOf[clientModel.AddFile]
      // In our TableState replay (in both getFiles and getCDFFiles),
      // legacy AddFile actions are converted into AddFileForCDF.
      val dtf = tableOpt.get.getCDFFiles()
      // Combine both kinds from the replay.
      val actualFiles = dtf.addFiles ++ dtf.cdfFiles
      // We check that at least one file exists with matching common fields.
      val existsMatch = actualFiles.exists { file =>
        file.url == expected.url &&
        file.id == expected.id &&
        file.partitionValues == expected.partitionValues &&
        file.size == expected.size &&
        (file match {
          case af: clientModel.AddFileForCDF =>
            expected match {
              case x: clientModel.AddFileForCDF =>
                af.version == x.version && af.timestamp == x.timestamp
              case x: clientModel.AddFile =>
                af.version == commit.version && af.timestamp == commit.timestamp
              case _ => false
            }
          case cd: clientModel.AddCDCFile =>
            expected match {
              case x: clientModel.AddCDCFile =>
                cd.version == x.version && cd.timestamp == x.timestamp
              case _ =>
                cd.version == commit.version && cd.timestamp == commit.timestamp
            }
          case _ => false
        })
      }
      assertTrue(existsMatch)
    }
  }

  object ReadTableCommand {
    def gen(
        client: DeltaSharingClient,
        state: DeltaState
    ): Gen[Any, Command[Any, DeltaState]] =
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
  ) extends Command[Any, DeltaState] {
    type E = Throwable
    type Result = CommandResult[DeltaState]
    override def execute(): ZIO[Any, Throwable, CommandResult[DeltaState]] =
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

        ReadTableCommandResult(
          schema,
          table,
          files,
          startingVersion,
          endingVersion,
          startingTimestamp,
          endingTimestamp
        )
      }
  }

  final case class ReadTableCommandResult(
      schema: String,
      table: String,
      output: clientModel.DeltaTableFiles,
      startingVersion: Option[Long],
      endingVersion: Option[Long],
      startingTimestamp: Option[String],
      endingTimestamp: Option[String]
  ) extends CommandResult[DeltaState] {
    override def update(s: DeltaState): DeltaState = s
    override def ensure(s: DeltaState): TestResult = {
      val tableOpt = s.table(schema, table)
      assertTrue(tableOpt.exists(_.currentVersion.isDefined)) &&
      assertTrue(tableOpt.isDefined) && {
        val memoryDeltaFiles = dummyNonComparableFields(tableOpt.get.getCDFFiles(startingVersion, endingVersion, startingTimestamp, endingTimestamp, includeHistoricalMetadata = false))
        val apiDeltaFiles = dummyNonComparableFields(output)
        val diff = deltaTableFilesDiffer.diff(memoryDeltaFiles, apiDeltaFiles)
        val diffString = DiffResultPrinter.consoleOutput(diff, 2).toString()
        if (!diff.isOk) {
          println(diffString)
          ()
        }
        assertTrue(diff.isOk || diffString.isEmpty)
      } &&
      assertTrue(
        endingVersion.forall(v => tableOpt.get.currentVersion.contains(v))
      )
    }
  }

  private def evaluateFilter(
      partitionValues: Map[String, String],
      filter: String
  ): Boolean = {
    val parts = filter.split("=").map(_.trim)
    if (parts.length != 2) return true
    val column = parts(0).replaceAll("'", "").replaceAll("\"", "")
    val value = parts(1).replaceAll("'", "").replaceAll("\"", "")
    partitionValues.get(column).exists(_ == value)
  }

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

  def standaloneSchemaToSparkSchema(schema: StructType): SparkStructType =
    SparkDataType.fromJson(schema.toJson()).asInstanceOf[SparkStructType]

  // Helper method to set non-deterministic fields to dummy values.
  private def dummyNonComparableFields(
      deltaTableFiles: clientModel.DeltaTableFiles
  ): clientModel.DeltaTableFiles = {
    // Define dummy values for the fields.
    val dummyVersion = 0L
    val dummyMetadataVersion: java.lang.Long = 0L
    val dummyId = "dummy-id"
    val dummyTimestamp = 0L
    val dummyExpirationTimestamp: java.lang.Long = 0L

    // Normalize metadata if it exists.
    val newMetadata = if (deltaTableFiles.metadata != null) {
      deltaTableFiles.metadata.copy(version = dummyMetadataVersion)
    } else {
      null
    }

    // For each element in addFiles, replace id, timestamp, and expirationTimestamp.
    val newAddFiles = deltaTableFiles.addFiles.map { addFile =>
      addFile.copy(
        id = dummyId,
        timestamp = dummyTimestamp,
        expirationTimestamp = dummyExpirationTimestamp
      )
    }

    // Return a duplicate of DeltaTableFiles with the dummy values applied.
    deltaTableFiles.copy(
      version = dummyVersion,
      metadata = newMetadata,
      addFiles = newAddFiles,
      refreshToken = None
    )
  }

  def spec = suite("DeltaSharingOperations")(test("delta sharing operations") {
    for {
      testTables <- ZIO.service[TestTables]
      testServerEnv <- ZIO.service[DeltaServerEnvironment]
      testServer <- ZIO.service[TestServerConfig]
      client = DeltaSharingRestClient(testServer.profileFile.toString)
      commandsGen = { state: DeltaState =>
        List(
          CreateManagedTableCommand.gen(
            testTables.basePath,
            state,
            testServerEnv.serverConfig
          ),
          AddDataCommand.gen(spark, state),
          ReadTableCommand.gen(client, state),
        )
      }
      result <- checkN(10)(
        Stateful.genActions(DeltaState.empty, commandsGen, numSteps = 10)
      )(steps =>
        ZIO
          .succeed(Stateful.allStepsSuccessful(steps))
          .debug("Test finished")
          <* ZIO.attempt(
            testTables.basePath
              .getFileSystem(hadoopConf)
              .delete(testTables.basePath, true)
          ) <* ZIO.attempt(
            testTables.basePath
              .getFileSystem(hadoopConf)
              .mkdirs(testTables.basePath)
          ) <* ZIO.succeed(
            testServerEnv.serverConfig.shares.get(0).schemas.clear()
          )
      )
    } yield result
  }).provideSomeLayer(overallLayer.fresh)
}
