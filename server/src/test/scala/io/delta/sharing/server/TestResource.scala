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
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import org.apache.commons.io.FileUtils

import io.delta.sharing.server.config._

object TestResource {
  def env(key: String): String = {
    sys.env.getOrElse(key, throw new IllegalArgumentException(s"Cannot find $key in sys env"))
  }

  object AWS {
    val bucket = "delta-exchange-test"
  }

  object Azure {
    val accountName = "deltasharingtest"
    val container = "delta-sharing-test-container"
  }

  object GCP {
    val bucket = "delta-sharing-dev"
  }

  val TEST_PORT = 12345

  val testAuthorizationToken = "dapi5e3574ec767ca1548ae5bbed1a2dc04d"

  def maybeSetupGoogleServiceAccountCredentials: Unit = {
    // Only setup Google Service Account credentials when it is provided through env variable.
    if (sys.env.get("GOOGLE_SERVICE_ACCOUNT_KEY").exists(_.length > 0)
        && sys.env.get("GOOGLE_APPLICATION_CREDENTIALS").exists(_.length > 0)) {
      val serviceAccountKey = sys.env("GOOGLE_SERVICE_ACCOUNT_KEY")
      val credFilePath = new File(sys.env("GOOGLE_APPLICATION_CREDENTIALS"))
      credFilePath.deleteOnExit()
      FileUtils.writeStringToFile(credFilePath, serviceAccountKey, UTF_8, false)
    }
  }
  val basePath = s"s3a://${AWS.bucket}/delta-exchange-test"

  def setupTestTables(): File = {
    val testConfigFile = Files.createTempFile("delta-sharing", ".yaml").toFile
    testConfigFile.deleteOnExit()
    maybeSetupGoogleServiceAccountCredentials
    val shares = java.util.Arrays.asList(
      ShareConfig("share1",
        java.util.Arrays.asList(
          SchemaConfig(
            "default",
            java.util.Arrays.asList(
              TableConfig(
                "table1",
                s"$basePath/table1",
                "00000000-0000-0000-0000-000000000001"
              ),
              TableConfig(
                "table3",
                s"$basePath/table3",
                "00000000-0000-0000-0000-000000000003"
              ),
              TableConfig(
                "table7",
                s"$basePath/table7",
                "00000000-0000-0000-0000-000000000007"
              )
            )
          )
        )
      ),
      ShareConfig("share2",
        java.util.Arrays.asList(
          SchemaConfig("default", java.util.Arrays.asList(
            TableConfig(
              "table2",
              s"$basePath/table2",
              "00000000-0000-0000-0000-000000000002"
            )
          )
          )
        )),
      ShareConfig("share3",
        java.util.Arrays.asList(
          SchemaConfig(
            "default",
            java.util.Arrays.asList(
              TableConfig(
                "table4",
                s"$basePath/table4",
                "00000000-0000-0000-0000-000000000004"
              ),
              TableConfig(
                "table5",
                s"$basePath/table5",
                "00000000-0000-0000-0000-000000000005"
              )
            )
          )
        )
      ),
      ShareConfig("share4",
        java.util.Arrays.asList(
          SchemaConfig(
            "default",
            java.util.Arrays.asList(
              // table made with spark.sql.parquet.compression.codec=gzip
              TableConfig(
                "test_gzip",
                s"s3a://${AWS.bucket}/compress-test/table1",
                "00000000-0000-0000-0000-000000000099"
              )
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
              TableConfig(
                "table8",
                s"$basePath/table8",
                "00000000-0000-0000-0000-000000000008"
              )
            )
          ),
          SchemaConfig(
            "schema2",
            java.util.Arrays.asList(
              TableConfig(
                "table9",
                s"$basePath/table9",
                "00000000-0000-0000-0000-000000000009"
              )
            )
          )
        )
      ),
      // scalastyle:off maxLineLength
      ShareConfig("share_azure",
        java.util.Arrays.asList(
          SchemaConfig(
            "default",
            java.util.Arrays.asList(
              TableConfig(
                "table_wasb",
                s"wasbs://${Azure.container}@${Azure.accountName}.blob.core.windows.net/delta-sharing-test/table1",
                "00000000-0000-0000-0000-000000000098"
              ),
              TableConfig(
                "table_abfs",
                s"abfss://${Azure.container}@${Azure.accountName}.dfs.core.windows.net/delta-sharing-test/table1",
                "00000000-0000-0000-0000-000000000097"
              )
            )
          )
        )
      ),
      // scalastyle:on
      ShareConfig("share_gcp",
        java.util.Arrays.asList(
          SchemaConfig(
            "default",
            java.util.Arrays.asList(
              TableConfig(
                "table_gcs",
                s"gs://${GCP.bucket}/delta-sharing-test/table1",
                "00000000-0000-0000-0000-000000000096"
              )
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
                "timestampntz_cdf_table",
                s"s3a://${AWS.bucket}/delta-exchange-test/timestampntz_cdf_table",
                "00000000-0000-0000-0000-000000000096",
                historyShared = true
              ),
              TableConfig(
                "cdf_table_cdf_enabled",
                s"$basePath/cdf_table_cdf_enabled",
                "00000000-0000-0000-0000-000000000095",
                historyShared = true
              ),
              TableConfig(
                "cdf_table_with_partition",
                s"$basePath/cdf_table_with_partition",
                "00000000-0000-0000-0000-000000000094",
                historyShared = true,
                startVersion = 1
              ),
              TableConfig(
                "cdf_table_with_vacuum",
                s"$basePath/cdf_table_with_vacuum",
                "00000000-0000-0000-0000-000000000093",
                historyShared = true
              ),
              TableConfig(
                "cdf_table_missing_log",
                s"$basePath/cdf_table_missing_log",
                "00000000-0000-0000-0000-000000000092",
                historyShared = true
              ),
              TableConfig(
                "streaming_table_with_optimize",
                s"$basePath/streaming_table_with_optimize",
                "00000000-0000-0000-0000-000000000091",
                historyShared = true
              ),
              TableConfig(
                "streaming_table_metadata_protocol",
                s"$basePath/streaming_table_metadata_protocol",
                "00000000-0000-0000-0000-000000000090",
                historyShared = true
              ),
              TableConfig(
                "streaming_notnull_to_null",
                s"$basePath/streaming_notnull_to_null",
                "00000000-0000-0000-0000-000000000089",
                historyShared = true
              ),
              TableConfig(
                "streaming_null_to_notnull",
                s"$basePath/streaming_null_to_notnull",
                "00000000-0000-0000-0000-000000000088",
                historyShared = true
              ),
              TableConfig(
                "streaming_cdf_null_to_notnull",
                s"$basePath/streaming_cdf_null_to_notnull",
                "00000000-0000-0000-0000-000000000087",
                historyShared = true
              ),
              TableConfig(
                "streaming_cdf_table",
                s"$basePath/streaming_cdf_table",
                "00000000-0000-0000-0000-000000000086",
                historyShared = true
              ),
              TableConfig(
                "table_reader_version_increased",
                s"$basePath/table_reader_version_increased",
                "00000000-0000-0000-0000-000000000085",
                historyShared = true
              ),
              TableConfig(
                "table_with_no_metadata",
                s"$basePath/table_with_no_metadata",
                "00000000-0000-0000-0000-000000000084",
                historyShared = true
              ),
              TableConfig(
                "table_data_loss_with_checkpoint",
                s"$basePath/table_data_loss_with_checkpoint",
                "00000000-0000-0000-0000-000000000083",
                historyShared = true
              ),
              TableConfig(
                "table_data_loss_no_checkpoint",
                s"$basePath/table_data_loss_no_checkpoint",
                "00000000-0000-0000-0000-000000000082",
                historyShared = true
              ),
              TableConfig(
                "table_with_cm_name",
                s"$basePath/table_with_cm_name",
                "00000000-0000-0000-0000-000000000081"
              ),
              TableConfig(
                "table_with_cm_id",
                s"$basePath/table_with_cm_id",
                "00000000-0000-0000-0000-000000000080"
              ),
              TableConfig(
                "deletion_vectors_with_dvs_dv_property_on",
                s"$basePath/deletion_vectors_with_dvs_dv_property_on",
                "00000000-0000-0000-0000-000000000079"
              ),
              TableConfig(
                "dv_and_cm_table",
                s"$basePath/dv_and_cm_table",
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
    serverConfig.setPort(TEST_PORT)
    serverConfig.setSsl(SSLConfig(selfSigned = true, null, null, null))
    serverConfig.setEvaluatePredicateHints(true)
    serverConfig.setEvaluateJsonPredicateHints(true)

    serverConfig.save(testConfigFile.getCanonicalPath)
    testConfigFile
  }
}
