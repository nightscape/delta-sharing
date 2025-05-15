# Delta Sharing Enhancements

This fork introduces enhancements to the Delta Sharing server, focusing on enterprise deployment capabilities, improved testing infrastructure, and modernized build tooling.

## Enhancements

### Dynamic Table Discovery
* Enables automatic discovery of Delta tables using glob patterns instead of requiring manual table configuration
  * Scans file systems using configured glob patterns (e.g., `hdfs://namenode/data/*/tables/*`)
  * Generates deterministic table IDs using SHA-256 hashing
  * Supports dynamic table name expansion using regex capture groups
  * Automatically detects `_delta_log` directories to identify valid Delta tables
* Configuration example in [delta-sharing-server.yaml](./delta-sharing-server.yaml)
* Files: [TableDiscoveryService.scala](./server/src/main/scala/io/delta/sharing/server/TableDiscoveryService.scala), [ServerConfig.scala](./server/src/main/scala/io/delta/sharing/server/config/ServerConfig.scala), [SharedTableManager.scala](./server/src/main/scala/io/delta/sharing/server/SharedTableManager.scala)
- Tests: [TableDiscoveryServiceSuite.scala](./server/src/test/scala/io/delta/sharing/server/TableDiscoveryServiceSuite.scala), [TableDiscoveryIntegrationSuite.scala](./server/src/test/scala/io/delta/sharing/server/TableDiscoveryIntegrationSuite.scala)

### HDFS Delegation Token Support
* Enables Delta Sharing server to work with Kerberized HDFS clusters
* Docker Examples:
  * [examples/docker-krb5/](./examples/docker-krb5/) - Basic Kerberos setup
  * [examples/docker-knox/](./examples/docker-knox/) - Full Knox gateway integration
* Files: [DeltaSharingService.scala](./server/src/main/scala/io/delta/sharing/server/DeltaSharingService.scala), [CloudFileSigner.scala](./server/src/main/scala/io/delta/sharing/server/common/CloudFileSigner.scala), [DeltaSharedTable.scala](./server/src/main/scala/io/delta/standalone/internal/DeltaSharedTable.scala)

### Knox Gateway Integration
* Complete Docker Compose setup demonstrating Delta Sharing with Apache Knox
  * Kerberos KDC configuration
  * Knox service definitions for Delta Sharing and WebHDFS
  * LDAP authentication setup
  * Full topology configurations
* Files: [examples/docker-knox/](./examples/docker-knox/)

## Testing Infrastructure

### Property-Based Testing Framework
* Comprehensive stateful property-based testing using ZIO Test
  * Models Delta Sharing operations as state transitions
  * Tests API consistency across share/schema/table operations
  * Validates CDF (Change Data Feed) query responses
  * Supports deterministic replay for debugging
* Files: [PropertyTest.scala](./server/src/test/scala/io/delta/sharing/server/PropertyTest.scala), [Stateful.scala](./server/src/test/scala/io/delta/sharing/server/Stateful.scala), [StatefulDeterministic.scala](./server/src/test/scala/io/delta/sharing/server/StatefulDeterministic.scala)

### Test Infrastructure Layers
* Advanced test resource management to allow testing in various scenarios
  * Docker
  * Docker + Kerberos
  * Docker + Kerberos + Knox
* Each includes:
  * Docker Compose configuration
  * Hadoop/HDFS configuration files
  * Init scripts for namenode/datanode
  * Spark data import scripts
  * Delta Sharing server configuration
* Files: [DockerLayer.scala](./server/src/test/scala/io/delta/sharing/server/DockerLayer.scala), [HadoopConfLayer.scala](./server/src/test/scala/io/delta/sharing/server/HadoopConfLayer.scala), [AuthLayer.scala](./server/src/test/scala/io/delta/sharing/server/AuthLayer.scala)
