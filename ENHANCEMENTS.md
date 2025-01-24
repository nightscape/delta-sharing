# Delta Sharing Enhancements

This fork introduces enhancements to the Delta Sharing server, focusing on enterprise deployment capabilities, improved testing infrastructure, and modernized build tooling.

## Testing Infrastructure

### Property-Based Testing Framework
* Comprehensive stateful property-based testing using ZIO Test
  * Models Delta Sharing operations as state transitions
  * Tests API consistency across share/schema/table operations
  * Validates CDF (Change Data Feed) query responses
  * Supports deterministic replay for debugging
* Files: [PropertyTest.scala](./server/src/test/scala/io/delta/sharing/server/PropertyTest.scala), [Stateful.scala](./server/src/test/scala/io/delta/sharing/server/Stateful.scala), [StatefulDeterministic.scala](./server/src/test/scala/io/delta/sharing/server/StatefulDeterministic.scala)
