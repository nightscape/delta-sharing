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

package io.delta.sharing.server

import java.io.File
import java.nio.file.Files
import java.time.LocalDateTime
import java.time.ZoneOffset
import io.delta.standalone.types._
import org.apache.hadoop.fs.Path
import io.delta.sharing.server.BaseTestResource._
import io.delta.sharing.server.config.{Authorization, SSLConfig, SchemaConfig, ServerConfig, ShareConfig, TableConfig}

import java.util.UUID

// scalastyle:off maxLineLength
object BaseTestResource {
  // Default schema used by most tables
  val structType = new StructType(Array(
      new StructField("id", new LongType(), false),
      new StructField("data", new StringType(), false),
      new StructField("category", new StringType(), false)
    ))

  // Schema for table1 with timestamp and date fields
  val table1Schema = new StructType(Array(
    new StructField("eventTime", new TimestampType(), true),
    new StructField("date", new DateType(), true)
  ))

  // Schema for Azure and GCP tables
  val cloudTableSchema = new StructType(Array(
    new StructField("c1", new StringType(), true),
    new StructField("c2", new StringType(), true)
  ))

  // Schema for CDF tables
  val cdfTableSchema = new StructType(Array(
    new StructField("name", new StringType(), true),
    new StructField("age", new IntegerType(), true),
    new StructField("birthday", new DateType(), true)
  ))

  val now = LocalDateTime.now()
  val defaultMetadata: Map[String, String] = Map("delta.checkpointInterval" -> "1", "delta.enableExpiredLogCleanup" -> "false")
  def defaultActions(metadataConfig: Map[String, String] = defaultMetadata): Seq[Action] = Seq(Create(metadataConfig = metadataConfig), AddData(), Bump())

  trait Action
  case class Create(
    timestamp: LocalDateTime = now.minusMinutes(2),
    metadataConfig: Map[String, String] = defaultMetadata,
    metadataId: Option[String] = None
  ) extends Action
  case class AddData(timestamp: LocalDateTime = now.minusMinutes(1), numRecords: Int = 4, id: String = UUID.randomUUID().toString) extends Action
  case class UpdateConfiguration(
    timestamp: LocalDateTime,
    changes: Map[String, String]
  ) extends Action
  case class GenerateCDC(
    timestamp: LocalDateTime,
    changes: Seq[CDCChange]
  ) extends Action
  case class RemovePath(path: String) extends Action
  case class Bump(timestamp: LocalDateTime = now) extends Action

  case class CDCChange(
    path: String,
    timestamp: LocalDateTime,
    operation: String // "add", "remove", or "update"
  )
}

trait BaseTestResource {
  def format: String
  def createAndPopulateTable(
    path: String,
    actions: Seq[Action] = BaseTestResource.defaultActions(),
    structType: StructType = BaseTestResource.structType
  ): String
  val testAuthorizationToken = "dapi5e3574ec767ca1548ae5bbed1a2dc04d"
  def setupTestTables(): File = {
    val testConfigFile = Files.createTempFile(s"${format}-sharing", ".yaml").toFile
    testConfigFile.deleteOnExit()

    // Create test tables with same structure as TestResource
    val basePath = Files.createTempDirectory(s"${format}-test-tables").toString

    // Create and populate tables with specific schemas where needed
    val yesterday = LocalDateTime.now.minusDays(1)
    val y2000 = LocalDateTime.parse("2000-01-01T00:00:01")
    val y2022 = LocalDateTime.parse("2022-01-01T00:00:01")

    val shares = java.util.Arrays.asList(
      ShareConfig("share1",
        java.util.Arrays.asList(
          SchemaConfig(
            "default",
            java.util.Arrays.asList(
              TableConfig("table1", createAndPopulateTable(s"$basePath/table1",
                actions = Seq(Create(yesterday, metadataId = Some("ed96aa41-1d81-4b7f-8fb5-846878b4b0cf")), AddData(yesterday, numRecords = 1, id = "061cb3683a467066995f8cdaabd8667d"), AddData(yesterday, numRecords = 1, id = "e268cbf70dbaa6143e7e9fa3e2d3b00e")),
                structType = BaseTestResource.table1Schema),
                "00000000-0000-0000-0000-000000000001"),
              TableConfig("table3", createAndPopulateTable(s"$basePath/table3"),
                "00000000-0000-0000-0000-000000000003"),
              TableConfig("table7", createAndPopulateTable(s"$basePath/table7"),
                "00000000-0000-0000-0000-000000000007")
            )
          )
        )
      ),
      ShareConfig("share2",
        java.util.Arrays.asList(
          SchemaConfig("default", java.util.Arrays.asList(
            TableConfig("table2", createAndPopulateTable(s"$basePath/table2"),
              "00000000-0000-0000-0000-000000000002")
          ))
        )
      ),
      ShareConfig("share3",
        java.util.Arrays.asList(
          SchemaConfig(
            "default",
            java.util.Arrays.asList(
              TableConfig("table4", createAndPopulateTable(s"$basePath/table4"),
                "00000000-0000-0000-0000-000000000004"),
              TableConfig("table5", createAndPopulateTable(s"$basePath/table5"),
                "00000000-0000-0000-0000-000000000005")
            )
          )
        )
      ),
      ShareConfig("share4",
        java.util.Arrays.asList(
          SchemaConfig(
            "default",
            java.util.Arrays.asList(
              TableConfig("test_gzip", createAndPopulateTable(s"$basePath/test_gzip"),
                "00000000-0000-0000-0000-000000000099")
            )
          )
        )
      ),
      ShareConfig("share5",
        java.util.Arrays.asList(
          SchemaConfig(
            "default", // empty schema
            java.util.Arrays.asList()
          )
        )
      ),
      ShareConfig("share6",
        java.util.Arrays.asList()
      ),
      ShareConfig("share7",
        java.util.Arrays.asList(
          SchemaConfig(
            "schema1",
            java.util.Arrays.asList(
              TableConfig("table8", createAndPopulateTable(s"$basePath/table8"),
                "00000000-0000-0000-0000-000000000008")
            )
          ),
          SchemaConfig(
            "schema2",
            java.util.Arrays.asList(
              TableConfig("table9", createAndPopulateTable(s"$basePath/table9"),
                "00000000-0000-0000-0000-000000000009")
            )
          )
        )
      ),
      ShareConfig("share_azure",
        java.util.Arrays.asList(
          SchemaConfig(
            "default",
            java.util.Arrays.asList(
              TableConfig("table_wasb", createAndPopulateTable(s"$basePath/table_wasb",
                structType = BaseTestResource.cloudTableSchema),
                "00000000-0000-0000-0000-000000000098"),
              TableConfig("table_abfs", createAndPopulateTable(s"$basePath/table_abfs",
                structType = BaseTestResource.cloudTableSchema),
                "00000000-0000-0000-0000-000000000097")
            )
          )
        )
      ),
      ShareConfig("share_gcp",
        java.util.Arrays.asList(
          SchemaConfig(
            "default",
            java.util.Arrays.asList(
              TableConfig("table_gcs", createAndPopulateTable(s"$basePath/table_gcs",
                structType = BaseTestResource.cloudTableSchema),
                "00000000-0000-0000-0000-000000000096")
            )
          )
        )
      ),
      ShareConfig("share8",
        java.util.Arrays.asList(
          SchemaConfig(
            "default",
            java.util.Arrays.asList(
              TableConfig(
                "cdf_table_cdf_enabled",
                createAndPopulateTable(s"$basePath/cdf_table_cdf_enabled",
                  actions = Seq(
                    // Version 0: Initial creation with CDF enabled
                    Create(
                      timestamp = LocalDateTime.parse("2022-01-01T00:00:01"),
                      metadataConfig = defaultMetadata + ("delta.enableChangeDataFeed" -> "true"),
                      metadataId = Some("16736144-3306-4577-807a-d3f899b77670")
                    ),

                    // Version 1: Three files added (INSERT)
                    AddData(
                      timestamp = LocalDateTime.ofEpochSecond(1651272635, 0, ZoneOffset.UTC),
                      numRecords = 573 // Size from test file
                    ),

                    // Version 2: One file removed (DELETE)
                    RemovePath("data/part-00000-1.snappy.parquet"),
                    GenerateCDC(
                      timestamp = LocalDateTime.ofEpochSecond(1651272655, 0, ZoneOffset.UTC),
                      changes = Seq(CDCChange(
                        path = "cdc-00000-1.snappy.parquet",
                        timestamp = LocalDateTime.ofEpochSecond(1651272655, 0, ZoneOffset.UTC),
                        operation = "remove"
                      ))
                    ),

                    // Version 3: Update (one remove, one add)
                    RemovePath("data/part-00000-2.snappy.parquet"),
                    AddData(
                      timestamp = LocalDateTime.ofEpochSecond(1651272660, 0, ZoneOffset.UTC),
                      numRecords = 573
                    ),
                    GenerateCDC(
                      timestamp = LocalDateTime.ofEpochSecond(1651272660, 0, ZoneOffset.UTC),
                      changes = Seq(CDCChange(
                        path = "cdc-00000-2.snappy.parquet",
                        timestamp = LocalDateTime.ofEpochSecond(1651272660, 0, ZoneOffset.UTC),
                        operation = "update"
                      ))
                    ),

                    // Version 4: Change CDF config to false
                    UpdateConfiguration(
                      timestamp = LocalDateTime.ofEpochSecond(1651272665, 0, ZoneOffset.UTC),
                      changes = Map("delta.enableChangeDataFeed" -> "false")
                    ),

                    // Version 5: Change CDF config back to true
                    UpdateConfiguration(
                      timestamp = LocalDateTime.ofEpochSecond(1651272670, 0, ZoneOffset.UTC),
                      changes = Map("delta.enableChangeDataFeed" -> "true")
                    )
                  ),
                  structType = BaseTestResource.cdfTableSchema),
                "00000000-0000-0000-0000-000000000095",
                historyShared = true
              ),
              TableConfig(
                "cdf_table_with_partition",
                createAndPopulateTable(s"$basePath/cdf_table_with_partition",
                  actions = Seq(Create(yesterday), AddData(yesterday), Bump(yesterday), Bump(yesterday), Bump(yesterday)),
                  structType = BaseTestResource.cdfTableSchema),
                "00000000-0000-0000-0000-000000000094",
                historyShared = true,
                startVersion = 1
              ),
              TableConfig(
                "cdf_table_with_vacuum",
                createAndPopulateTable(s"$basePath/cdf_table_with_vacuum",
                  actions = Seq(Create(yesterday), AddData(yesterday), Bump(yesterday), Bump(yesterday), Bump(yesterday)),
                  structType = BaseTestResource.cdfTableSchema),
                "00000000-0000-0000-0000-000000000093",
                historyShared = true
              ),
              TableConfig(
                "cdf_table_missing_log",
                createAndPopulateTable(s"$basePath/cdf_table_missing_log",
                  structType = BaseTestResource.cdfTableSchema),
                "00000000-0000-0000-0000-000000000092",
                historyShared = true
              ),
              TableConfig(
                "streaming_table_with_optimize",
                createAndPopulateTable(s"$basePath/streaming_table_with_optimize",
                  structType = BaseTestResource.cdfTableSchema),
                "00000000-0000-0000-0000-000000000091",
                historyShared = true
              ),
              TableConfig(
                "streaming_table_metadata_protocol",
                createAndPopulateTable(s"$basePath/streaming_table_metadata_protocol",
                  structType = BaseTestResource.cdfTableSchema),
                "00000000-0000-0000-0000-000000000090",
                historyShared = true
              ),
              TableConfig(
                "streaming_notnull_to_null",
                createAndPopulateTable(s"$basePath/streaming_notnull_to_null",
                  structType = BaseTestResource.cdfTableSchema),
                "00000000-0000-0000-0000-000000000089",
                historyShared = true
              ),
              TableConfig(
                "streaming_null_to_notnull",
                createAndPopulateTable(s"$basePath/streaming_null_to_notnull",
                  structType = BaseTestResource.cdfTableSchema),
                "00000000-0000-0000-0000-000000000088",
                historyShared = true
              ),
              TableConfig(
                "streaming_cdf_null_to_notnull",
                createAndPopulateTable(s"$basePath/streaming_cdf_null_to_notnull",
                  structType = cdfTableSchema),
                "00000000-0000-0000-0000-000000000087",
                historyShared = true
              ),
              TableConfig(
                "streaming_cdf_table",
                createAndPopulateTable(s"$basePath/streaming_cdf_table",
                  structType = cdfTableSchema),
                "00000000-0000-0000-0000-000000000086",
                historyShared = true
              ),
              TableConfig(
                "table_reader_version_increased",
                createAndPopulateTable(s"$basePath/table_reader_version_increased"),
                "00000000-0000-0000-0000-000000000085",
                historyShared = true
              ),
              TableConfig(
                "table_with_no_metadata",
                createAndPopulateTable(s"${basePath}/table_with_no_metadata",
                  actions = defaultActions() ++ Seq(RemovePath("_delta_log"))),
                "00000000-0000-0000-0000-000000000084",
                historyShared = true
              ),
              TableConfig(
                "table_data_loss_with_checkpoint",
                createAndPopulateTable(s"$basePath/table_data_loss_with_checkpoint",
                  actions = Seq(
                    Create(metadataConfig = defaultMetadata + ("delta.enableChangeDataFeed" -> "true")),
                    AddData(),
                    AddData(),
                    RemovePath("_delta_log/00000000000000000001.json")
                  ),
                  structType = cdfTableSchema),
                "00000000-0000-0000-0000-000000000083",
                historyShared = true
              ),
              TableConfig(
                "table_data_loss_no_checkpoint",
                createAndPopulateTable(s"$basePath/table_data_loss_no_checkpoint",
                  actions = defaultActions(metadataConfig = defaultMetadata + ("delta.enableChangeDataFeed" -> "true")) ++
                    Seq(AddData(yesterday), RemovePath("_delta_log/00000000000000000001.json")),
                  structType = cdfTableSchema),
                "00000000-0000-0000-0000-000000000082",
                historyShared = true
              ),
              TableConfig(
                "table_with_cm_name",
                createAndPopulateTable(s"$basePath/table_with_cm_name"),
                "00000000-0000-0000-0000-000000000081"
              ),
              TableConfig(
                "table_with_cm_id",
                createAndPopulateTable(s"$basePath/table_with_cm_id"),
                "00000000-0000-0000-0000-000000000080"
              ),
              TableConfig(
                "deletion_vectors_with_dvs_dv_property_on",
                createAndPopulateTable(s"$basePath/deletion_vectors_with_dvs_dv_property_on"),
                "00000000-0000-0000-0000-000000000079"
              ),
              TableConfig(
                "dv_and_cm_table",
                createAndPopulateTable(s"$basePath/dv_and_cm_table"),
                "00000000-0000-0000-0000-000000000078"
              )
            )
          )
        )
      )
    )

    val serverConfig = new ServerConfig()
    serverConfig.setVersion(1)
    serverConfig.setShares(shares)
    serverConfig.setAuthorization(Authorization(testAuthorizationToken))
    serverConfig.setPort(8085)
    serverConfig.setSsl(SSLConfig(selfSigned = true, null, null, null))
    serverConfig.setEvaluatePredicateHints(true)
    serverConfig.setEvaluateJsonPredicateHints(true)
    serverConfig.save(testConfigFile.getCanonicalPath)
    testConfigFile
  }
}

