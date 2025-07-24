package io.delta.sharing.server
// scalastyle:off
import io.delta.sharing.client.DeltaSharingRestClient
import io.delta.standalone.Operation
import io.delta.standalone.types._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import zio.{Differ => _, _}
import zio.test._
import zio.test.ZIOSpecDefault

import java.net.ServerSocket
import io.delta.sharing.server.config.ServerConfig
import io.delta.sharing.server.config.ShareConfig
import io.delta.sharing.server.config.SchemaConfig
import io.delta.sharing.server.config.TableConfig
import io.delta.sharing.server.config.Authorization

import scala.collection.mutable.ArrayBuffer
import io.delta.sharing.client.{model => clientModel}
import io.delta.sharing.client.DeltaSharingProfile

import javax.security.auth.Subject

import java.util.concurrent.TimeUnit
import org.apache.hadoop.security.UserGroupInformation
import io.delta.sharing.server.DockerLayer._
import java.time.Instant
import java.io.File
import AuthLayer.Auth

object PropertyTest extends ZIOSpecDefault {
  private val format = "delta"

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
      : ZLayer[Scope with TestTables with Configuration, Throwable, DeltaServerEnvironment] =
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
        hadoopConfiguration <- ZIO.service[Configuration]
        basePath = testTables.basePath
        serverConfig = ServerConfig(
          version = 1,
          shares = java.util.Arrays.asList(
            ShareConfig(
              "test-share",
              java.util.Arrays.asList(
                SchemaConfig(
                  "test-schema",
                  java.util.Arrays.asList(
                    TableConfig(
                      name = "table-$1",
                      location = s"$basePath/test-schema/table-*",
                      id = "",
                      historyShared = true,
                      startVersion = 0L
                    )
                  )
                )
              )
            )
          ),
          authorization = Authorization(token),
          ssl = null, // No SSL configuration provided.
          host = "localhost",
          port = port,
          endpoint = "/delta-sharing",
          preSignedUrlTimeoutSeconds = 3600L,
          deltaTableCacheSize = 10,
          stalenessAcceptable = true,
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

  val serverLayer: ZLayer[Scope with DeltaServerEnvironment, Throwable, TestServerConfig] =
    ZLayer.scoped {
      for {
        env <- ZIO.service[DeltaServerEnvironment]
        tsc <- ZIO.acquireRelease(
          for {
            server <- ZIO.attempt(DeltaSharingService.start(env.serverConfig))
            _      <- ZIO.logInfo(s"Server is running on port ${env.port}")
          } yield TestServerConfig(
            env.port,
            env.profileString,
            () => server.stop()
          )
        )(tsc => ZIO.attempt(tsc.stop.run()).orDie)
      } yield tsc
    }

  val hadoopFileSystemLayer: ZLayer[
    Scope with DockerEnv with Auth with Configuration,
    Throwable,
    FileSystem
  ] =
    ZLayer.scoped {
      ZIO.acquireRelease {
        for {
          endpoints <- ZIO.service[DockerEnv]
          auth <- ZIO.service[Auth]
          config <- ZIO.service[Configuration]
          fs <- ZIO.attempt {
            auth match {
              case AuthLayer.KerberosAuth(subject) =>
                // Initialize the security configuration and login using the subject if available
                UserGroupInformation.setConfiguration(config)
                UserGroupInformation.loginUserFromSubject(subject)
              case _ =>
                // No authentication needed
            }
            FileSystem.get(config)
          }
        } yield fs
      } { fs =>
        ZIO.attempt(fs.close()).orDie
      }
    }

  val testTablesLayer: ZLayer[
    Scope with FileSystem,
    Throwable,
    TestTables
  ] =
    ZLayer.scoped {
      ZIO.acquireRelease {
        for {
          fs <- ZIO.service[FileSystem]
          _ <- TestClock.setTime(Instant.now())
          timestamp <- Clock.currentTime(TimeUnit.MILLISECONDS)
          testDir: Path = new Path(
            s"${fs.getUri}/tmp/${format}-test-tables-$timestamp"
          )
          _ <- ZIO.attempt(fs.mkdirs(testDir))
        } yield TestTables(testDir)
      } { testTables =>
        val releaseEffectAny: ZIO[Any, Throwable, Unit] = for {
          _ <- ZIO.attempt {
            println(s"[DEBUG] basePath: ${testTables.basePath}, uri: ${testTables.basePath.toUri}")
            val defaultFs = FileSystem.get(hadoopConf)
            println(s"[DEBUG] defaultFS uri: ${defaultFs.getUri}")

            val pathFs = testTables.basePath.getFileSystem(hadoopConf)
            println(s"[DEBUG] pathFS uri: ${pathFs.getUri}")

            // Attempt to rename using the FileSystem associated with the basePath
            pathFs.rename(
              testTables.basePath,
              testTables.basePath.suffix("-deleted")
            )
            ()
          }
          _ <- ZIO.attempt {
            val pathFs = testTables.basePath.getFileSystem(hadoopConf)
            println(s"[DEBUG] mkdirs using pathFS uri: ${pathFs.getUri}")
            pathFs.mkdirs(testTables.basePath)
            ()
          }
        } yield ()
        val releaseEffectSafe: ZIO[Any, Nothing, Unit] = releaseEffectAny.orDie
        ZIO.serviceWithZIO[FileSystem](_ => releaseEffectSafe)
      }
    }

  // Get the stack descriptor based on the environment variable
  val stackDescriptor = StackRegistry.fromEnv()

  // Create a combined layer with all the components
  val overallLayer: ZLayer[
    Scope,
    Throwable,
    Scope
      with TestTables
      with DeltaServerEnvironment
      with TestServerConfig
      with DockerEnv
      with Auth
      with Configuration
      with FileSystem
      with SparkSession
  ] =
    ZLayer.make[
        Scope
        with TestTables
        with DeltaServerEnvironment
        with TestServerConfig
        with DockerEnv
        with Auth
        with Configuration
        with FileSystem
        with SparkSession
    ](
      Scope.default,
      DockerLayer.live(stackDescriptor),
      AuthLayer.live(stackDescriptor),
      HadoopConfLayer.live(stackDescriptor),
      SparkSessionLayer.live,
      testTablesLayer,
      serverConfigLayer,
      serverLayer,
      hadoopFileSystemLayer
    )

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
        currentVersion = currentVersion.map(_ + 1)
      )
    def getFiles(
        startingVersion: Option[Long],
        endingVersion: Option[Long]
    ): clientModel.DeltaTableFiles = {
      if (commits.flatMap(_.actions).size < 2) {
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
      val baseMetadata = firstCommit.actions(1).asInstanceOf[clientModel.Metadata]
      val metadata = baseMetadata.copy(version = startingVersion match {
        case Some(v) => v
        case None    => null
      })
      val files = ArrayBuffer[clientModel.AddFile]()
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
              files.append(a)
            case c: clientModel.AddCDCFile =>
              version = math.max(version, c.version)
              cdcFiles.append(c)
            case r: clientModel.RemoveFile =>
              version = math.max(version, r.version)
              removeFiles.append(r)
            case m: clientModel.Metadata =>
              version = math.max(version, m.version)
              additionalMetadatas.append(m)
            case p:clientModel.Protocol => ()
            case a: clientModel.AddFileForCDF =>
              version = math.max(version, a.version)
              files.append(
                clientModel.AddFile(
                  url = a.url,
                  id = a.id,
                  partitionValues = a.partitionValues,
                  size = a.size,
                  stats = a.stats,
                  version = null,
                  timestamp = null,
                  expirationTimestamp = null
                )
              )
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
        files = files.toSeq,
        addFiles = Seq.empty,
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
      val baseMetadata = firstCommit.actions(1).asInstanceOf[clientModel.Metadata]
      val metadata = baseMetadata.copy(version = startingVersion match {
        case Some(v) => v
        case None    => null
      })
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
                  version = commit.version,
                  timestamp = commit.timestamp.getOrElse(0L),
                  stats = a.stats
                )
              )
            case c: clientModel.AddCDCFile =>
              version = math.max(version, c.version)
              cdcFiles.append(c)
            case r: clientModel.RemoveFile =>
              version = math.max(version, r.version)
              removeFiles.append(r)
            case m: clientModel.Metadata =>
              version = math.max(version, m.version)
              if (includeHistoricalMetadata) additionalMetadatas.append(m)
            case a: clientModel.AddFileForCDF =>
              version = math.max(version, a.version)
              addFiles.append(a)
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
        files = Seq.empty,
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
      timestamp: Option[Long],
      message: String
  ) {}


  def spec: Spec[TestEnvironment with Scope, Any] = suite(
    "DeltaSharingOperations"
  )(test("delta sharing operations") {
    for {
      spark <- ZIO.service[SparkSession]
      testTables <- ZIO.service[TestTables]
      testServerEnv <- ZIO.service[DeltaServerEnvironment]
      testServer <- ZIO.service[TestServerConfig]
      fs <- ZIO.service[FileSystem]
      profilePath <- ZIO.attempt {
        val profilePath = testTables.basePath.suffix("/delta-test.share")
        val outputStream = fs.create(profilePath)
        outputStream.writeBytes(testServer.profileString)
        outputStream.close()
        profilePath
      }
      client = DeltaSharingRestClient(profilePath.toString)
      hadoopConf = spark.sparkContext.hadoopConfiguration
      commandsGen = { state: DeltaState =>
        List(
          CreateManagedTableCommand.gen(
            testTables.basePath,
            state,
            testServerEnv.serverConfig,
            hadoopConf
          ),
          AddDataCommand.gen(spark, state),
          ReadTableCommand.gen(client, state),
          ReadTableSparkCommand.gen(spark, profilePath.toString, state),
        )
      }
      result <- checkN(10)(
        StatefulDeterministic.genActions(DeltaState.empty, commandsGen)
      )(steps =>
        StatefulDeterministic
          .allStepsSuccessful(steps)
          .debug("Test finished") <*
          ZIO.attempt {
            println(s"[DEBUG] basePath: ${testTables.basePath}, uri: ${testTables.basePath.toUri}")
            println(s"[DEBUG] FileSystem uri: ${fs.getUri}")

            // Attempt to rename using the FileSystem
            fs.rename(
              testTables.basePath,
              testTables.basePath.suffix("-deleted")
            )
          } <*
          ZIO.attempt {
            println(s"[DEBUG] mkdirs using FileSystem uri: ${fs.getUri}")
            fs.mkdirs(testTables.basePath)
          }
      )
    } yield result
  }).provideSomeLayer(overallLayer.fresh) @@ TestAspect.timeout(10.minutes)
}
