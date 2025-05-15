import org.apache.spark.sql.SparkSession
import io.delta.implicits._
import io.delta.tables._
import org.apache.spark.sql.functions._
val sparkConf = spark.sparkContext.getConf

// Initial data
spark.sql("DROP TABLE IF EXISTS default.test_table PURGE")

// Delete entire HDFS directory at file:///tmp/delta-test/tmp/delta-test/
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI

val hdfsUri = "file:///"
val fs = FileSystem.get(new URI(hdfsUri), spark.sparkContext.hadoopConfiguration)
val deltaTestDir = new Path(s"$hdfsUri/tmp/delta-test/")
if (fs.exists(deltaTestDir)) {
    fs.delete(deltaTestDir, true)
    println(s"Deleted directory: $deltaTestDir")
} else {
    println(s"Directory $deltaTestDir does not exist.")
}

spark.sql("""
CREATE TABLE IF NOT EXISTS default.test_table (
  id INT,
  value STRING
)
USING DELTA
PARTITIONED BY (id)
LOCATION 'file:///tmp/delta-test/delta-test-table'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
val data1 = Seq((1, "first"), (2, "second")).toDF("id", "value")
data1.write.format("delta").option("delta.enableChangeDataFeed", "true").option("userMetadata", "1234").mode("append").saveAsTable("default.test_table")

// Add more data
val data2 = Seq((3, "third"), (4, "fourth")).toDF("id", "value")
data2.write.format("delta").option("userMetadata", "5678").mode("append").saveAsTable("default.test_table")

// Update some data
val deltaTable = DeltaTable.forPath("file:///tmp/delta-test/delta-test-table")
deltaTable.updateExpr(
  "id = 1",
  Map("value" -> "'updated first'")
)

// Show final state and history
println("Final table contents:")
spark.read.format("delta").load("file:///tmp/delta-test/delta-test-table").show(false)

println("\nTable history:")
deltaTable.history().show(false)
