#!/usr/bin/env bash

# Create temporary directory for Delta table
BASE_DIR="${BASE_DIR:-/tmp}"
TEST_DIR="$BASE_DIR/delta-test"
TEST_TABLE_DIR="$TEST_DIR/delta-test-table"
rm -rf $TEST_DIR
mkdir -p $TEST_TABLE_DIR

# Create logging configuration
cat > $TEST_DIR/logback.xml << EOL
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
    <encoder>
      <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
    </encoder>
  </appender>

  <logger name="com.linecorp.armeria.logging.access" level="INFO" additivity="false">
    <appender-ref ref="CONSOLE"/>
  </logger>

  <logger name="io.delta" level="DEBUG" additivity="false">
    <appender-ref ref="CONSOLE"/>
  </logger>

  <root level="INFO">
    <appender-ref ref="CONSOLE"/>
  </root>
</configuration>
EOL

# Create config file for delta-sharing server
cat > $TEST_DIR/config.yaml << EOL
version: 1
shares:
- name: "default"
  schemas:
  - name: "default"
    tables:
    - name: "test_table"
      location: "$TEST_TABLE_DIR"
      id: "11111111-1111-1111-1111-111111111111"
      historyShared: true
host: "localhost"
port: 8000
endpoint: "/delta-sharing"
preSignedUrlTimeoutSeconds: 3600
deltaTableCacheSize: 10
stalenessAcceptable: false
evaluatePredicateHints: false
evaluateJsonPredicateHints: true
evaluateJsonPredicateHintsV2: true
EOL

# Create a Scala script for Spark operations
cat > $TEST_DIR/spark_ops.scala << EOL
import org.apache.spark.sql.SparkSession
import io.delta.implicits._
import io.delta.tables._
import org.apache.spark.sql.functions._

// Create SparkSession with Delta configurations
val spark = {
  SparkSession.builder()
    .appName("Delta Table Creation")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
    .getOrCreate()
}
// Initial data
spark.sql("DROP TABLE IF EXISTS default.test_table")
spark.sql("""
CREATE TABLE IF NOT EXISTS default.test_table (
  id INT,
  value STRING
)
USING DELTA
LOCATION '$TEST_TABLE_DIR'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
val data1 = Seq((1, "first"), (2, "second")).toDF("id", "value")
data1.write.format("delta").option("delta.enableChangeDataFeed", "true").mode("append").saveAsTable("default.test_table")

// Add more data
val data2 = Seq((3, "third"), (4, "fourth")).toDF("id", "value")
data2.write.format("delta").mode("append").saveAsTable("default.test_table")

// Update some data
val deltaTable = DeltaTable.forPath("$TEST_TABLE_DIR")
deltaTable.updateExpr(
  "id = 1",
  Map("value" -> "'updated first'")
)

// Show final state and history
println("Final table contents:")
spark.read.format("delta").load("$TEST_TABLE_DIR").show()

println("\nTable history:")
deltaTable.history().show()

spark.stop()
System.exit(0)
EOL

# Run Spark operations
spark-shell \
    --packages io.delta:delta-spark_2.12:3.3.0 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    -i $TEST_DIR/spark_ops.scala

# Run Delta Sharing server with mounted volume, debug logging, and remote debugging enabled
docker run -p 8000:8000 -p 5005:5005 \
    --name delta-sharing-server --rm \
    --mount type=bind,source=$BASE_DIR,target=$BASE_DIR \
    --mount type=bind,source=$TEST_DIR/config.yaml,target=/config/delta-sharing-server-config.yaml \
    --mount type=bind,source=$TEST_DIR/logback.xml,target=/opt/docker/conf/logback.xml \
    -e JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005" \
    deltaio/delta-sharing-server:latest \
    --config /config/delta-sharing-server-config.yaml
