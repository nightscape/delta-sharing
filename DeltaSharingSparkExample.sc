// HADOOP_CONF_DIR="./examples/docker-knox/configs/hadoop" KNOX_WEBHDFS_CONTEXT="/gateway/sandbox" KNOX_WEBHDFS_USERNAME="hadoop" KNOX_WEBHDFS_PASSWORD="hadoop-password" _JAVA_OPTIONS="-Djavax.net.ssl.trustStore=./examples/docker-knox/truststore/truststore.p12 -Djavax.net.ssl.trustStorePassword=thekeystorespasswd" scala-cli -w DeltaSharingSparkExample.sc
//> using scala 2.12
//> using repositories m2Local
//> using dep io.delta:delta-sharing-spark_2.12:1.1.11
//> using dep org.apache.spark:spark-sql_2.12:3.3.4
//> using dep dev.mauch:knox-webhdfs:0.0.6

import org.apache.spark.sql.SparkSession

val profilePath = "./examples/docker-knox/configs/delta-sharing/profile.share"

val shareName = "test-share"
val schemaName = "test-schema"
val tableName = "test-table"

val spark = SparkSession.builder()
  .appName("DeltaSharingSparkExample")
  .master("local[*]")
  .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

val tableIdentifier = s"${profilePath}#${shareName}.${schemaName}.${tableName}"
val df = spark.read.format("deltaSharing").load(tableIdentifier)
df.printSchema()
df.show()
val count = df.count()
println(s"\nTotal rows in table: $count")
