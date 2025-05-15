package io.delta.sharing.server

import java.security.MessageDigest
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import io.delta.sharing.server.config.{ServerConfig, TableConfig}

/** Service for discovering Delta tables dynamically using glob patterns. Scans
  * file systems to find tables matching configured glob patterns and generates
  * table configurations on-the-fly.
  */
class TableDiscoveryService(serverConfig: ServerConfig) {

  private val hadoopConf = new Configuration()

  /** Discovers tables matching the given table configuration glob pattern.
    * @param tableConfig
    *   Configuration with glob pattern in location field
    * @return
    *   Sequence of discovered table configurations
    */
  def discoverTables(tableConfig: TableConfig): Seq[TableConfig] = {
    if (!tableConfig.isDynamic) {
      Seq.empty
    } else {
      scanForTables(tableConfig)
    }
  }

  /** Generates a deterministic ID from a table location.
    * @param location
    *   The actual table location
    * @return
    *   A deterministic ID string
    */
  def generateTableId(location: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val hash = digest.digest(location.getBytes("UTF-8"))
    val hexString = hash.map("%02x".format(_)).mkString
    s"discovered-${hexString.take(8)}"
  }


  private def scanForTables(tableConfig: TableConfig): Seq[TableConfig] = {
    val basePath = extractBasePath(tableConfig.location)

    val fs = FileSystem.get(new Path(basePath).toUri, hadoopConf)

    val globPatternPath = new Path(tableConfig.location)
    val fileStatusList = Option(fs.globStatus(globPatternPath))
      .getOrElse(Array.empty)

    fileStatusList
      .filter(p => fs.isDirectory(new Path(p.getPath(), "_delta_log")))
      .map { status =>
        val path = status.getPath()

        val pathRegex = tableConfig.location.replaceAll("\\*", "([^/]+)")
        val expandedName = path.toString.replaceAll(".*" + pathRegex, tableConfig.name)

        TableConfig(
          location = path.toString,
          id = generateTableId(path.toString),
          historyShared = tableConfig.historyShared,
          startVersion = tableConfig.startVersion,
          name = expandedName
        )
      }
      .toSeq
  }

  // Removed scanDirectory method as it's no longer needed

  private def extractBasePath(pattern: String): String = {
    // Find the last path separator in the pattern
    val separatorIndex = pattern.lastIndexOf('/')

    if (separatorIndex == -1) {
      // No path separators, use root
      "/"
    } else if (separatorIndex == 0) {
      // Pattern starts with '/', use root
      "/"
    } else {
      pattern.substring(0, separatorIndex)
    }
  }
}
