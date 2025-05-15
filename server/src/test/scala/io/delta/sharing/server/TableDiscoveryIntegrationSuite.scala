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
import java.util.Collections

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.funsuite.AnyFunSuite

import io.delta.sharing.server.config.{ServerConfig, ShareConfig, SchemaConfig, TableConfig}

class TableDiscoveryIntegrationSuite extends AnyFunSuite {

  private def createTempDir(): File = {
    Files.createTempDirectory("delta-discovery-integration-test").toFile
  }

  private def createTestServerConfig(): ServerConfig = {
    val serverConfig = new ServerConfig()
    serverConfig.setVersion(1)
    serverConfig
  }

  test("SharedTableManager should discover tables using regex patterns") {
    val tempDir = createTempDir()
    val hadoopConf = new Configuration()
    val fs = FileSystem.get(hadoopConf)

    try {
      // Create directory structure that matches discovery pattern
      val basePath = new Path(tempDir.getAbsolutePath)
      val teamAlphaPath = new Path(basePath, "discovery/team-alpha/table-users")
      val teamBetaPath = new Path(basePath, "discovery/team-beta/table-events")

      // Create _delta_log directories to simulate Delta tables
      fs.mkdirs(new Path(teamAlphaPath, "_delta_log"))
      fs.mkdirs(new Path(teamBetaPath, "_delta_log"))

      // Create server config with discovery pattern
      val discoveryTableConfig = new TableConfig()
      discoveryTableConfig.location = s"^$basePath/discovery/([^/]+)/table-([^/]+)/_delta_log"
      discoveryTableConfig.name = "discovered_${1}_${2}"
      discoveryTableConfig.historyShared = true
      discoveryTableConfig.startVersion = 0L
      discoveryTableConfig.id = "" // Set empty ID for discovery configs

      val schemaConfig = new SchemaConfig()
      schemaConfig.name = "test-schema"
      schemaConfig.tables = java.util.Arrays.asList(discoveryTableConfig)

      val shareConfig = new ShareConfig()
      shareConfig.name = "test-share"
      shareConfig.schemas = java.util.Arrays.asList(schemaConfig)

      val serverConfig = createTestServerConfig()
      serverConfig.shares = java.util.Arrays.asList(shareConfig)

      // Create SharedTableManager and test discovery
      val sharedTableManager = new SharedTableManager(serverConfig)

      // Debug: directly test discovery service
      val discoveryService = new TableDiscoveryService(serverConfig)
      println(s"Testing discovery pattern: ${discoveryTableConfig.location}")
      println(s"Created directories:")
      println(s"  - ${teamAlphaPath}")
      println(s"  - ${teamBetaPath}")

      // Test if pattern matches our paths
      val pattern = java.util.regex.Pattern.compile(discoveryTableConfig.location)
      val testPath1 = s"${teamAlphaPath}/_delta_log"
      val testPath2 = s"${teamBetaPath}/_delta_log"
      println(s"Pattern matches '$testPath1': ${pattern.matcher(testPath1).matches()}")
      println(s"Pattern matches '$testPath2': ${pattern.matcher(testPath2).matches()}")

      val discoveredTables = discoveryService.discoverTables(discoveryTableConfig)
      println(s"Discovery service found ${discoveredTables.size} tables:")
      discoveredTables.foreach(table => println(s"  - ${table.name} at ${table.location}"))

      // List tables in the schema - should include discovered tables
      val (tables, _) = sharedTableManager.listTables("test-share", "test-schema")

      // Debug output
      println(s"Found ${tables.size} tables:")
      tables.foreach(table => println(s"  - ${table.name.getOrElse("null")} (id: ${table.id.getOrElse("null")})"))

      // Verify discovered tables are present
      val tableNames = tables.map(_.name.get).toSet
      println(s"Table names: $tableNames")
      assert(tableNames.contains("discovered_team-alpha_users"))
      assert(tableNames.contains("discovered_team-beta_events"))

      // Verify we can retrieve individual discovered tables
      val alphaTable = sharedTableManager.getTable("test-share", "test-schema", "discovered_team-alpha_users")
      assert(alphaTable.name == "discovered_team-alpha_users")
      assert(alphaTable.location.endsWith("discovery/team-alpha/table-users"))

      val betaTable = sharedTableManager.getTable("test-share", "test-schema", "discovered_team-beta_events")
      assert(betaTable.name == "discovered_team-beta_events")
      assert(betaTable.location.endsWith("discovery/team-beta/table-events"))

      println(s"Successfully discovered ${tables.size} tables")

    } finally {
      fs.close()
      FileUtils.deleteDirectory(tempDir)
    }
  }

  test("TableDiscoveryService should handle mixed static and discovered tables") {
    val tempDir = createTempDir()
    val hadoopConf = new Configuration()
    val fs = FileSystem.get(hadoopConf)

    try {
      // Create directory structure
      val basePath = new Path(tempDir.getAbsolutePath)
      val discoveredTablePath = new Path(basePath, "discovery/team-gamma/table-metrics")
      fs.mkdirs(new Path(discoveredTablePath, "_delta_log"))

      // Create static table config
      val staticTableConfig = new TableConfig()
      staticTableConfig.name = "static-table"
      staticTableConfig.location = s"^$basePath/static/table"
      staticTableConfig.historyShared = true

      // Create discovery table config
      val discoveryTableConfig = new TableConfig()
      discoveryTableConfig.location = s"^$basePath/discovery/([^/]+)/table-([^/]+)/_delta_log"
      discoveryTableConfig.name = "discovered_${1}_${2}"
      discoveryTableConfig.historyShared = true
      discoveryTableConfig.id = "" // Set empty ID for discovery configs

      val schemaConfig = new SchemaConfig()
      schemaConfig.name = "test-schema"
      schemaConfig.tables = java.util.Arrays.asList(staticTableConfig, discoveryTableConfig)

      val shareConfig = new ShareConfig()
      shareConfig.name = "test-share"
      shareConfig.schemas = java.util.Arrays.asList(schemaConfig)

      val serverConfig = createTestServerConfig()
      serverConfig.shares = java.util.Arrays.asList(shareConfig)

      // Test table listing
      val sharedTableManager = new SharedTableManager(serverConfig)
      val (tables, _) = sharedTableManager.listTables("test-share", "test-schema")

      // Should have both static and discovered tables
      val tableNames = tables.map(_.name.get).toSet
      assert(tableNames.contains("static-table"))
      assert(tableNames.contains("discovered_team-gamma_metrics"))
      assert(tables.size == 2)

      println("Successfully mixed static and discovered tables")

    } finally {
      fs.close()
      FileUtils.deleteDirectory(tempDir)
    }
  }
}
