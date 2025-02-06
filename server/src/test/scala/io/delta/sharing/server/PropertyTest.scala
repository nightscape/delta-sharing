package io.delta.sharing.server
// scalastyle:off
import com.dimafeng.testcontainers.{DockerComposeContainer, ExposedService}
import difflicious._
import difflicious.differ.RecordDiffer
import difflicious.implicits._
import difflicious.utils.TypeName
import io.delta.sharing.client.model.Table
import io.delta.sharing.client.{DeltaSharingClient, DeltaSharingRestClient}
import io.delta.standalone.Operation
import io.delta.standalone.actions._
import io.delta.standalone.internal._
import io.delta.standalone.internal.util.newManualClock
import io.delta.standalone.types._
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{
  DataType => SparkDataType,
  StructType => SparkStructType
}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import zio.{Differ => _, _}
import zio.stream._
import zio.test._
import zio.test.Assertion._
import zio.test.Gen._
import zio.test.ZIOSpecDefault

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

import javax.security.auth.Subject

// Import the deterministic stateful framework.
import io.delta.sharing.server.StatefulDeterministic
import scala.collection.immutable.ListMap
import scala.reflect.ClassTag
import java.util.concurrent.TimeUnit
import org.apache.hadoop.security.UserGroupInformation
import io.delta.sharing.server.DockerLayer._
import java.time.Instant

object PropertyTest extends ZIOSpecDefault {
  private val format = "delta"
  private lazy val spark =
    SparkSession.builder().master("local[4]").getOrCreate()

  // Create the Hadoop configuration. Notice that later we update it to point to the HDFS cluster.
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
      profileString: String,
      stop: Runnable
  )

  final case class DeltaServerEnvironment(
      port: Int,
      profileString: String,
      serverConfig: ServerConfig
  )
  final case class TestTables(basePath: Path)
  val serverConfigLayer
      : ZLayer[Scope with TestTables, Throwable, DeltaServerEnvironment] =
    ZLayer.scoped {
      for {
        port <- findFreePort()
        token = "dapi5e3574ec767ca1548ae5bbed1a2dc04d"
        profileString =
          s"""{
             |  "shareCredentialsVersion": 1,
             |  "endpoint": "http://localhost:$port/delta-sharing",
             |  "bearerToken": "$token"
             |}""".stripMargin
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
      } yield DeltaServerEnvironment(port, profileString, serverConfig)
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
      } yield TestServerConfig(env.port, env.profileString, stopServer)
    }

  val testTablesLayer: ZLayer[
    Scope with ServiceEndpoints with Subject,
    Throwable,
    TestTables
  ] =
    ZLayer.scoped {
      ZIO.acquireRelease {
        for {
          endpoints <- ZIO.service[ServiceEndpoints]
          subject <- ZIO.service[Subject]
          hdfsUri =
            s"hdfs://${endpoints.namenodeHost}:${endpoints.namenodePort}"
          _ <- ZIO.attempt {
            val configs = Map(
              "fs.defaultFS" -> hdfsUri,
              "hadoop.security.authentication" -> "kerberos",
              "hadoop.rpc.protection" -> "privacy",
              "dfs.namenode.kerberos.principal" -> s"nn/${endpoints.namenodeContainerName}@HADOOP.LOCAL"
            )
            configs.foreach { case (k, v) => hadoopConf.set(k, v)}
            // Initialize the security configuration and login using your keytab.
            UserGroupInformation.setConfiguration(hadoopConf)
            UserGroupInformation.loginUserFromSubject(subject)
          }
          fs <- ZIO.attempt(FileSystem.get(hadoopConf))
          _ <- TestClock.setTime(Instant.now())
          timestamp <- Clock.currentTime(TimeUnit.MILLISECONDS)
          testDir: Path = new Path(
            s"$hdfsUri/tmp/${format}-test-tables-$timestamp"
          )
          _ <- ZIO.attempt(fs.mkdirs(testDir))
        } yield TestTables(testDir)
      } { testTables =>
        ZIO
          .attempt(
            FileSystem
              .get(hadoopConf)
              .rename(
                testTables.basePath,
                testTables.basePath.suffix(s"-deleted")
              )
          )
          .orDie
      }
    }

  val overallLayer: ZLayer[
    Scope,
    Throwable,
    Scope with TestTables with DeltaServerEnvironment with TestServerConfig with ServiceEndpoints with Subject
  ] =
    ZLayer.makeSome[
      Scope,
      Scope
        with TestTables
        with DeltaServerEnvironment
        with TestServerConfig
        with ServiceEndpoints
        with Subject
    ](
      testTablesLayer,
      serverConfigLayer,
      serverLayer,
      hadoopPreStartedLayer,
      kerberosSubjectLayer
    )

  def beanDiffer[T: ClassTag](exclude: Set[String] = Set.empty): Differ[T] = {
    import java.beans.Introspector
    def getBeanProperties(
        beanClass: Class[_]
    ): Array[java.beans.PropertyDescriptor] = {
      val beanInfo = Introspector.getBeanInfo(beanClass)
      beanInfo.getPropertyDescriptors
    }
    val classTag = implicitly[ClassTag[T]]
    val runtimeClass = classTag.runtimeClass
    val properties = getBeanProperties(runtimeClass).filterNot(p =>
      exclude.contains(p.getName)
    )
    val extractors = ListMap(properties.map { p =>
      val extractor = (t: T) => p.getReadMethod.invoke(t).asInstanceOf[AnyRef]
      p.getName -> (extractor, Differ.useEquals[Any](_.toString()))
    }: _*)
    new RecordDiffer[T](
      extractors,
      isIgnored = false,
      TypeName(runtimeClass.getName, runtimeClass.getSimpleName, List.empty)
    )
  }
  implicit val protocolDiffer: Differ[clientModel.Protocol] =
    beanDiffer[clientModel.Protocol](Set("id"))
  implicit val metadataDiffer: Differ[clientModel.Metadata] =
    beanDiffer[clientModel.Metadata](Set("id"))
  implicit val addFileDiffer: Differ[clientModel.AddFile] =
    beanDiffer[clientModel.AddFile](Set("size"))
  implicit val addFileForCdfDiffer: Differ[clientModel.AddFileForCDF] =
    beanDiffer[clientModel.AddFileForCDF](Set("size"))
  implicit val addCdcFileDiffer: Differ[clientModel.AddCDCFile] =
    beanDiffer[clientModel.AddCDCFile](Set("size"))
  implicit val addCdcFileSeqDiffer: Differ[Seq[clientModel.AddCDCFile]] =
    Differ.seqDiffer[Seq, clientModel.AddCDCFile].pairBy(_.url)
  implicit val removeFileDiffer: Differ[clientModel.RemoveFile] =
    beanDiffer[clientModel.RemoveFile](Set("url"))
  implicit val deltaTableFilesDiffer: Differ[clientModel.DeltaTableFiles] =
    beanDiffer[clientModel.DeltaTableFiles](Set("url"))

  case class TableState(
      tableId: String,
      tablePath: String,
      commits: List[Commit] = Nil,
      currentVersion: Option[Long] = None,
      currentSchema: Option[StructType] = None,
      knownPartitionValues: Map[String, Set[String]] = Map.empty
  ) {
    def addCommit(commit: Commit): TableState =
      copy(
        commits = commits :+ commit,
        currentVersion = currentVersion.orElse(Some(0L)).map(_ + 1)
      )
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
      deltaState: DeltaState
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
  }
  implicit class RichRemoteIterator[T](iter: RemoteIterator[T]) {
    def asScala: Iterator[T] = new Iterator[T] {
      def hasNext: Boolean = iter.hasNext
      def next(): T = iter.next()
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
            diff.isOk || diffString.isEmpty &&
            endingVersion.forall(v =>
              tableOpt.get.currentVersion.get >= v
            )
        )
      }
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

  def spec: Spec[TestEnvironment with Scope, Any] = suite(
    "DeltaSharingOperations"
  )(test("delta sharing operations") {
    for {
      // TODO: The deletion of old directories still leaves caches in the server intact
      _ <- ZIO.succeed(spark)
      testTables <- ZIO.service[TestTables]
      testServerEnv <- ZIO.service[DeltaServerEnvironment]
      testServer <- ZIO.service[TestServerConfig]
      profilePath <- ZIO.attempt {
        val fs = testTables.basePath.getFileSystem(
          spark.sparkContext.hadoopConfiguration
        )
        val profilePath = testTables.basePath.suffix("/delta-test.share")
        val outputStream = fs.create(profilePath)
        outputStream.writeBytes(testServer.profileString)
        outputStream.close()
        profilePath
      }
      client = DeltaSharingRestClient(profilePath.toString)
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
        StatefulDeterministic.genActions(DeltaState.empty, commandsGen)
      )(steps =>
        StatefulDeterministic
          .allStepsSuccessful(steps)
          .debug("Test finished") <*
          ZIO.attempt(
            FileSystem
              .get(hadoopConf)
              .rename(
                testTables.basePath,
                testTables.basePath.suffix("-deleted")
              )
          ) <*
          ZIO.attempt(
            FileSystem.get(hadoopConf).mkdirs(testTables.basePath)
          ) <*
          ZIO.succeed(testServerEnv.serverConfig.shares.get(0).schemas.clear())
      )
    } yield result
  }).provideSomeLayer(overallLayer.fresh) @@ TestAspect.timeout(10.minutes)
}
