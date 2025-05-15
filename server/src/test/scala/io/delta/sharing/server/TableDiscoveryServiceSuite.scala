package io.delta.sharing.server

import java.io.File
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.scalatest.funsuite.AnyFunSuite

import io.delta.sharing.server.config.{ ServerConfig, TableConfig }

class TableDiscoveryServiceSuite extends AnyFunSuite {

  private def createTempDir(): File = {
    Files.createTempDirectory("delta-discovery-test").toFile
  }

  private def createTestServerConfig(): ServerConfig = {
    val serverConfig = new ServerConfig()
    serverConfig.setVersion(1)
    serverConfig
  }

  test("generateTableId should be deterministic") {
    val service = new TableDiscoveryService(createTestServerConfig())
    val location = "s3://bucket/path/to/table"

    val id1 = service.generateTableId(location)
    val id2 = service.generateTableId(location)

    assert(id1 == id2, "Generated IDs should be deterministic")
    assert(id1.startsWith("discovered-"), "ID should have 'discovered-' prefix")
    assert(id1.length == 19, "ID should be 'discovered-' + 8 hex chars")
  }

  test("generateTableId should be different for different locations") {
    val service = new TableDiscoveryService(createTestServerConfig())
    val location1 = "s3://bucket/path/to/table1"
    val location2 = "s3://bucket/path/to/table2"

    val id1 = service.generateTableId(location1)
    val id2 = service.generateTableId(location2)

    assert(id1 != id2, "Different locations should generate different IDs")
  }


  test("TableConfig validation should pass for valid discovery config") {
    val tableConfig = TableConfig(
      location = "s3://bucket/team-*/table-*",
      name = "team_$1_$2",
      historyShared = true
    )

    tableConfig.checkConfig()
  }

  test(
    "TableConfig validation should fail when location indicates discovery but name does not contain match groups"
  ) {
    val tableConfig = TableConfig(
      name = "my-share",
      location = "s3://bucket/team-*/table-*"
    )

    val exception = intercept[IllegalArgumentException] {
      tableConfig.checkConfig()
    }
    assert(exception.getMessage.contains("placeholders"))
  }

  test("TableConfig validation should pass for valid static config") {
    val tableConfig = TableConfig(
      name = "static-table",
      location = "s3://bucket/static/table"
    )

    // Should not throw exception
    tableConfig.checkConfig()
  }

  test(
    "discoverTables should find all _delta_log directories in directory structure"
  ) {
    val tempDir = createTempDir()
    try {
      // Create a directory structure with _delta_log directories
      val structure = Map(
        "team-marketing/table-users/_delta_log" -> "marketing-users",
        "team-sales/table-customers/_delta_log" -> "sales-customers",
        "team-engineering/table-metrics/_delta_log" -> "engineering-metrics",
        "team-marketing/table-products/_delta_log" -> "marketing-products",
        "team-sales/table-orders/_delta_log" -> "sales-orders"
      )

      structure.foreach { case (path, _) =>
        val dir = new File(tempDir, path)
        dir.mkdirs()
        // Create a simple file in each _delta_log to make it a valid delta table
        new File(dir, "00000000000000000000.json").createNewFile()
      }

      val service = new TableDiscoveryService(createTestServerConfig())
      val tableConfig = TableConfig(
        location =
          s"${tempDir.getAbsolutePath.replace("\\", "/")}/team-*/table-*",
        name = "team_$1_$2",
        historyShared = true
      )

      val discoveredTables = service.discoverTables(tableConfig)

      // Verify all expected tables are discovered
      assert(
        discoveredTables.size == 5,
        s"Expected 5 tables, found ${discoveredTables.size}"
      )

      val expectedTableNames = Set(
        "team_marketing_users",
        "team_sales_customers",
        "team_engineering_metrics",
        "team_marketing_products",
        "team_sales_orders"
      )

      val discoveredTableNames = discoveredTables.map(_.name).toSet
      assert(
        discoveredTableNames == expectedTableNames,
        s"Expected table names: $expectedTableNames, found: $discoveredTableNames"
      )

    } finally {
      FileUtils.deleteDirectory(tempDir)
    }
  }

  test("discoverTables should handle nested directory structures") {
    val tempDir = createTempDir()
    try {
      // Create a more complex nested structure
      val structure = Map(
        "org/team-a/project-x/table-data/_delta_log" -> "team-a-project-x-data",
        "org/team-b/project-y/table-results/_delta_log" -> "team-b-project-y-results",
        "org/team-a/project-z/table-logs/_delta_log" -> "team-a-project-z-logs"
      )

      // Create the directory structure
      structure.foreach { case (path, _) =>
        val dir = new File(tempDir, path)
        dir.mkdirs()
        // Create a simple file in each _delta_log to make it a valid delta table
        new File(dir, "00000000000000000000.json").createNewFile()
      }

      val service = new TableDiscoveryService(createTestServerConfig())
      val tableConfig = TableConfig(
        location =
          s"${tempDir.getAbsolutePath.replace("\\", "/")}/org/team-*/project-*/table-*",
        name = "team_$1_project_$2_$3",
        historyShared = true
      )

      val discoveredTables = service.discoverTables(tableConfig)

      // Verify all expected tables are discovered
      assert(
        discoveredTables.size == 3,
        s"Expected 3 tables, found ${discoveredTables.size}"
      )

      val expectedTableNames = Set(
        "team_a_project_x_data",
        "team_b_project_y_results",
        "team_a_project_z_logs"
      )

      val discoveredTableNames = discoveredTables.map(_.name).toSet
      assert(
        discoveredTableNames == expectedTableNames,
        s"Expected table names: $expectedTableNames, found: $discoveredTableNames"
      )

    } finally {
      FileUtils.deleteDirectory(tempDir)
    }
  }

  test("discoverTables should ignore directories without _delta_log") {
    val tempDir = createTempDir()
    try {
      // Create directories with and without _delta_log
      val validStructure = Map(
        "team-marketing/table-users/_delta_log" -> "marketing-users",
        "team-sales/table-customers/_delta_log" -> "sales-customers"
      )

      val invalidStructure = Map(
        "team-marketing/table-users/other-data" -> "should-be-ignored",
        "team-sales/table-customers/backup" -> "should-be-ignored",
        "team-marketing/table-users" -> "should-be-ignored" // no _delta_log
      )

      // Create valid delta table directories
      validStructure.foreach { case (path, _) =>
        val dir = new File(tempDir, path)
        dir.mkdirs()
        new File(dir, "00000000000000000000.json").createNewFile()
      }

      // Create invalid directories (should be ignored)
      invalidStructure.foreach { case (path, _) =>
        val dir = new File(tempDir, path)
        dir.mkdirs()
      }

      val service = new TableDiscoveryService(createTestServerConfig())
      val tableConfig = TableConfig(
        location =
          s"${tempDir.getAbsolutePath.replace("\\", "/")}/team-*/table-*",
        name = "team_$1_$2",
        historyShared = true
      )

      val discoveredTables = service.discoverTables(tableConfig)

      assert(
        discoveredTables.size == 2,
        s"Expected 2 tables, found ${discoveredTables.size}"
      )

      val expectedTableNames =
        Set("team_marketing_users", "team_sales_customers")
      val discoveredTableNames = discoveredTables.map(_.name).toSet
      assert(
        discoveredTableNames == expectedTableNames,
        s"Expected table names: $expectedTableNames, found: $discoveredTableNames"
      )

    } finally {
      FileUtils.deleteDirectory(tempDir)
    }
  }
}
