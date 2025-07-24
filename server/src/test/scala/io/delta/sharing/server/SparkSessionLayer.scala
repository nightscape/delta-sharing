package io.delta.sharing.server

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import zio._
import org.apache.hadoop.security.UserGroupInformation
import AuthLayer.Auth
import scala.collection.JavaConverters._

/**
 * Layer that provides a properly configured SparkSession.
 * This ensures that Spark is created with the correct Hadoop configuration
 * from the HadoopConfLayer, including support for HDFS, Kerberos, and Knox.
 */
object SparkSessionLayer {

  /**
   * Creates a SparkSession layer that depends on Hadoop Configuration and Auth.
   * The SparkSession will be properly configured with all necessary settings
   * for HDFS access, including Kerberos and Knox gateway support.
   */
  def live: ZLayer[Scope with Configuration with Auth, Throwable, SparkSession] =
    ZLayer.scoped {
      for {
        hadoopConf <- ZIO.service[Configuration]
        auth <- ZIO.service[Auth]
        _ <- ZIO.logInfo("Creating SparkSession with Hadoop configuration")
        
        spark <- ZIO.acquireRelease {
          ZIO.attempt {
            // Create SparkSession builder
            val builder = SparkSession.builder()
              .master("local[4]")
              .appName("DeltaSharingTest")
              
            // Get the default FS from Hadoop configuration
            val defaultFs = hadoopConf.get("fs.defaultFS")
            if (defaultFs != null) {
              builder.config("spark.hadoop.fs.defaultFS", defaultFs)
            }
            
            // Copy important Hadoop settings to Spark configuration
            // This ensures Spark executors will have the correct settings
            val importantKeys = Seq(
              "fs.defaultFS",
              "hadoop.security.authentication",
              "hadoop.rpc.protection",
              "dfs.namenode.kerberos.principal",
              "dfs.web.authentication.kerberos.principal",
              "dfs.datanode.kerberos.principal",
              "dfs.client.use.datanode.hostname",
              "dfs.namenode.https-address",
              "dfs.namenode.http-address",
              "knox.webhdfs.context"
            )
            
            importantKeys.foreach { key =>
              val value = hadoopConf.get(key)
              if (value != null) {
                builder.config(s"spark.hadoop.$key", value)
              }
            }
            
            // For Knox configurations
            if (hadoopConf.get("knox.webhdfs.context") != null) {
              // Knox-specific filesystem implementation classes
              builder.config("spark.hadoop.fs.knoxwebhdfs.impl", "org.apache.hadoop.hdfs.web.WebHdfsFileSystem")
              builder.config("spark.hadoop.fs.knoxswebhdfs.impl", "org.apache.hadoop.hdfs.web.SWebHdfsFileSystem")
            }
            
            // Handle Kerberos authentication if needed
            auth match {
              case AuthLayer.KerberosAuth(subject) =>
                // Ensure Kerberos is properly configured for Spark
                builder.config("spark.hadoop.hadoop.security.authentication", "kerberos")
                builder.config("spark.hadoop.hadoop.rpc.protection", "privacy")
                
                // Set the UGI before creating Spark to ensure proper auth
                UserGroupInformation.setConfiguration(hadoopConf)
                UserGroupInformation.loginUserFromSubject(subject)
                
              case _ => // No special auth needed
            }
            
            // Create the SparkSession
            val spark = builder.getOrCreate()

            // After creation, update the Hadoop configuration in the SparkContext
            // This ensures any runtime changes are reflected
            val sparkHadoopConf = spark.sparkContext.hadoopConfiguration
            hadoopConf.iterator().asScala.foreach { entry =>
              sparkHadoopConf.set(entry.getKey, entry.getValue)
            }
            // Spark overwrites the global static UserGroupInformation
            // Let's set it back
            UserGroupInformation.setConfiguration(hadoopConf)

            spark
          }
        } { spark =>
          ZIO.attempt {
            spark.stop()
          }.orDie
        }
      } yield spark
    }
}