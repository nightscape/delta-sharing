package io.delta.sharing.server

import zio._
import AuthLayer.Auth
import AuthLayer.KerberosAuth
import DockerLayer.ServiceEndpoints
import org.apache.hadoop.conf.Configuration
import java.io.{File, PrintWriter}
import java.nio.file.{Files, Path}
import org.apache.hadoop.fs.{FileSystem => HadoopFileSystem}
import java.net.URLClassLoader

/**
 * Layer that provides Hadoop configuration tailored to the specific environment.
 * This layer ensures that configurations work consistently even when code uses
 * new Configuration() directly, by setting HADOOP_CONF_DIR.
 */
object HadoopConfLayer {

  /**
   * Creates a Hadoop configuration layer based on the stack descriptor.
   * Uses the existing configuration files in the example directories.
   */
  def live(desc: StackDescriptor): ZLayer[ServiceEndpoints with Auth, Throwable, Configuration] =
    ZLayer.scoped {
      for {
        endpoints <- ZIO.service[ServiceEndpoints]
        auth <- ZIO.service[Auth]

        // Get the path to the existing hadoop configuration directory
        hadoopConfDir = desc.composeDir.resolve("configs/hadoop")
        _ <- ZIO.logInfo(s"Using Hadoop configuration from: $hadoopConfDir")

        // Verify that the configuration directory exists
        _ <- ZIO.attempt {
          val confDir = hadoopConfDir.toFile
          if (!confDir.exists()) {
            throw new IllegalStateException(s"Hadoop configuration directory does not exist: $hadoopConfDir")
          }
          if (!confDir.isDirectory()) {
            throw new IllegalStateException(s"Path is not a directory: $hadoopConfDir")
          }
        }

        // Set the environment variables to point to the configuration directory
        _ <- ZIO.attempt {
          val path = hadoopConfDir.resolve("core-site.xml")
          val customLoader = new URLClassLoader(
            Array(hadoopConfDir.toUri.toURL),
            Thread.currentThread().getContextClassLoader()
          )
          Thread.currentThread().setContextClassLoader(customLoader)
          //java.lang.System.setProperty("HADOOP_CONF_DIR", hadoopConfDir.toString)
          //java.lang.System.setProperty("hadoop.conf.dir", hadoopConfDir.toString)
        }

        // Create and configure a new Hadoop Configuration
        conf <- ZIO.attempt {
          val conf = new Configuration()
          // Set the correct namenode address with the current host/port
          val defaultFsUri = if (desc.usesKnox && endpoints.knoxGatewayUrl.isDefined) {
            val knoxUrl = endpoints.knoxGatewayUrl.get
            knoxUrl
              .replace("https://", "knoxswebhdfs://")
              .replace("http://", "knoxwebhdfs://")
          } else {
            s"hdfs://${endpoints.namenodeHost}:${endpoints.namenodePort}"
          }
          conf.set("fs.defaultFS", defaultFsUri)

          // For Kerberos setups, ensure correct principal names with the container name
          if (desc.needsKerberosLogin) {
            // Update security settings that may need the container name
            val realm = desc.kerberosRealm.getOrElse("HADOOP.LOCAL")
            conf.set("hadoop.security.authentication", "kerberos")
            conf.set("hadoop.rpc.protection", "privacy")
            conf.set("dfs.namenode.kerberos.principal", s"nn/${endpoints.namenodeContainerName}@$realm")
            conf.set("dfs.web.authentication.kerberos.principal", s"HTTP/${endpoints.namenodeContainerName}@$realm")
          }

          conf
        }

        _ <- ZIO.logInfo(s"Created Hadoop configuration with fs.defaultFS = ${conf.get("fs.defaultFS")}")
      } yield conf
    }
}
