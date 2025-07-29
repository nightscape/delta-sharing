package io.delta.sharing.server.examples

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation

import java.io.{BufferedReader, InputStreamReader}
import java.net.URI
import java.nio.charset.StandardCharsets
import scala.util.{Failure, Success, Try}

/**
 * Simplified example demonstrating Knox WebHDFS programmatically.
 * Leverages existing docker-knox configurations with minimal custom code.
 */
object KnoxWebHdfsExample {

  def main(args: Array[String]): Unit = {
    println("Knox WebHDFS Example - Minimal Configuration")
    println("=" * 50)

    val example = new KnoxWebHdfsExample()

    try {
      val fs = example.initializeFileSystem()
      example.performOperations(fs)
      fs.close()
      println("\nExample completed successfully!")

    } catch {
      case e: Exception =>
        println(s"Failed: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}

class KnoxWebHdfsExample {

  /**
   * Initialize FileSystem with minimal configuration leveraging docker-knox configs.
   * This is as close to `FileSystem.get(new Configuration())` as possible.
   */
  def initializeFileSystem(): FileSystem = {
    println("Initializing with minimal configuration...")

    // Configuration() automatically loads XML files from test classpath (via serverTestSettings)
    // All authentication, Knox credentials, and SSL settings now come from XML files!
    val config = new Configuration()

    // Only initialization needed - UserGroupInformation with simple auth (from XML)
    UserGroupInformation.setConfiguration(config)
    val username = config.get("knox.webhdfs.username", "hadoop")  // Get username from XML config
    UserGroupInformation.createRemoteUser(username)

    // Debug: Check if XML values are loaded
    println("=== Configuration Debug ===")
    println(s"fs.knoxswebhdfs.impl: ${config.get("fs.knoxswebhdfs.impl")}")
    println(s"knox.webhdfs.context: ${config.get("knox.webhdfs.context")}")
    println(s"ssl.client.truststore.location: ${config.get("ssl.client.truststore.location")}")
    println(s"hadoop.security.authentication: ${config.get("hadoop.security.authentication")}")
    println(s"dfs.webhdfs.enabled: ${config.get("dfs.webhdfs.enabled")}")
    println(s"knox.webhdfs.username: ${config.get("knox.webhdfs.username")}")
    println(s"knox.webhdfs.password: ${config.get("knox.webhdfs.password")}")
    println(s"knox.webhdfs.verify.hostname: ${config.get("knox.webhdfs.verify.hostname")}")

    // Knox URI using context from hdfs-site.xml
    val context = config.get("knox.webhdfs.context", "/gateway/sandbox")
    val knoxUri = new URI(s"knoxswebhdfs://knox-gateway.localtest.me:8443$context/webhdfs")
    println(s"Connecting to: $knoxUri")

    val fs = FileSystem.get(knoxUri, config)
    println(s"Connected! FileSystem: ${fs.getClass.getSimpleName}")

    fs
  }

  /**
   * Perform various file system operations through Knox
   */
  def performOperations(fs: FileSystem): Unit = {
    println("\nPerforming file system operations...")

    // 1. List root directory (equivalent to curl ls /)
    listDirectory(fs, new Path("/"))

    // 2. Create /tmp directory if it doesn't exist (equivalent to curl MKDIRS /tmp)
    val tmpDir = new Path("/tmp")
    ensureDirectoryExists(fs, tmpDir)

    // 3. List /tmp directory (equivalent to curl ls /tmp)
    listDirectory(fs, tmpDir)

    // 4. Create a test file (equivalent to curl CREATE /tmp/test.txt)
    val testFile = new Path("/tmp/knox-test.txt")
    createTestFile(fs, testFile, "Hello from Knox WebHDFS via Scala!")

    // 5. Read the test file (equivalent to curl OPEN /tmp/test.txt)
    readFile(fs, testFile)

    // 6. List /tmp directory again to see our new file
    listDirectory(fs, tmpDir)

    // 7. Get file status (equivalent to curl GETFILESTATUS /tmp/test.txt)
    getFileStatus(fs, testFile)
  }

  /**
   * List contents of a directory
   */
  def listDirectory(fs: FileSystem, path: Path): Unit = {
    println(s"\n--- Listing directory: $path ---")

    Try {
      val fileStatuses = fs.listStatus(path)
      if (fileStatuses.isEmpty) {
        println("Directory is empty")
      } else {
        fileStatuses.foreach { status =>
          val fileType = if (status.isDirectory) "DIR" else "FILE"
          val size = if (status.isDirectory) "-" else status.getLen.toString
          val owner = status.getOwner
          val group = status.getGroup
          val permissions = status.getPermission
          val modTime = new java.util.Date(status.getModificationTime)

          println(f"$fileType%4s $permissions%10s $owner%10s $group%10s $size%10s $modTime%tc ${status.getPath.getName}")
        }
      }
    } match {
      case Success(_) => // Success already handled above
      case Failure(e) => println(s"Error listing directory $path: ${e.getMessage}")
    }
  }

  /**
   * Ensure a directory exists (create if it doesn't)
   */
  def ensureDirectoryExists(fs: FileSystem, path: Path): Unit = {
    println(s"\n--- Ensuring directory exists: $path ---")

    Try {
      if (!fs.exists(path)) {
        val created = fs.mkdirs(path)
        if (created) {
          println(s"Successfully created directory: $path")
        } else {
          println(s"Failed to create directory: $path")
        }
      } else {
        println(s"Directory already exists: $path")
      }
    } match {
      case Success(_) => // Success already handled above
      case Failure(e) => println(s"Error ensuring directory $path exists: ${e.getMessage}")
    }
  }

  /**
   * Create a test file with content
   */
  def createTestFile(fs: FileSystem, path: Path, content: String): Unit = {
    println(s"\n--- Creating file: $path ---")

    Try {
      val outputStream = fs.create(path, true) // true = overwrite if exists
      outputStream.write(content.getBytes(StandardCharsets.UTF_8))
      outputStream.close()
      println(s"Successfully created file: $path")
      println(s"Content: $content")
    } match {
      case Success(_) => // Success already handled above
      case Failure(e) => println(s"Error creating file $path: ${e.getMessage}")
    }
  }

  /**
   * Read content from a file
   */
  def readFile(fs: FileSystem, path: Path): Unit = {
    println(s"\n--- Reading file: $path ---")

    Try {
      val inputStream = fs.open(path)
      val reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))

      val content = new StringBuilder
      var line: String = null
      while ({ line = reader.readLine(); line != null }) {
        content.append(line).append("\n")
      }

      reader.close()
      inputStream.close()

      println(s"File content:")
      println(content.toString().trim)
    } match {
      case Success(_) => // Success already handled above
      case Failure(e) => println(s"Error reading file $path: ${e.getMessage}")
    }
  }

  /**
   * Get file status information
   */
  def getFileStatus(fs: FileSystem, path: Path): Unit = {
    println(s"\n--- File status for: $path ---")

    Try {
      val status = fs.getFileStatus(path)
      println(s"Path: ${status.getPath}")
      println(s"Length: ${status.getLen} bytes")
      println(s"Owner: ${status.getOwner}")
      println(s"Group: ${status.getGroup}")
      println(s"Permission: ${status.getPermission}")
      println(s"Modification time: ${new java.util.Date(status.getModificationTime)}")
      println(s"Access time: ${new java.util.Date(status.getAccessTime)}")
      println(s"Is directory: ${status.isDirectory}")
      println(s"Block size: ${status.getBlockSize}")
      println(s"Replication: ${status.getReplication}")
    } match {
      case Success(_) => // Success already handled above
      case Failure(e) => println(s"Error getting file status for $path: ${e.getMessage}")
    }
  }
}
