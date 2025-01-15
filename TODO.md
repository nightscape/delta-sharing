# Test Failure Summary

## Core Issues
1. Version/Snapshot ID Mismatches
   - Multiple tests expecting specific version numbers but receiving different ones
   - Example: "[-1]" did not equal "[2]"
   - Example: "[4]" did not equal "[7813340856593805102]"
   ```
   [info] - table1 - head - /shares/{share}/schemas/{schema}/tables/{table} *** FAILED ***
   [info]   "[-1]" did not equal "[2]" (DeltaSharingServiceSuite.scala:422)
   ```

2. CDF (Change Data Feed) Issues
   - Cannot enable incremental scan
   - Multiple 400 errors on /changes endpoints
   ```
   java.io.IOException: Server returned HTTP response code: 400 for URL: .../tables/cdf_table_with_partition/changes?startingVersion=1&endingVersion=3
   at java.base/sun.net.www.protocol.http.HttpURLConnection.getInputStream0(HttpURLConnection.java:1924)
   at java.base/sun.net.www.protocol.http.HttpURLConnection.getInputStream(HttpURLConnection.java:1520)
   ```

3. Array Index Errors
   - Multiple "Index 1 out of bounds for length 1" errors
   - Affects column mapping metadata tests and deletion vector tests
   ```
   java.lang.ArrayIndexOutOfBoundsException: Index 1 out of bounds for length 1
   at io.delta.sharing.server.DeltaSharingServiceSuite.$anonfun$new$77(DeltaSharingServiceSuite.scala:1514)
   at io.delta.sharing.server.DeltaSharingServiceSuite.$anonfun$new$77$adapted(DeltaSharingServiceSuite.scala:1510)
   at scala.collection.immutable.List.foreach(List.scala:431)
   ```

4. HTTP Response Errors
   - Multiple 400 Bad Request responses where 200 OK was expected
   - Several cases where 400 was expected but 200 was received
   ```
   java.io.IOException: Server returned HTTP response code: 400 for URL: https://localhost:8085/delta-sharing/shares/share1/schemas/default/tables/table1/metadata
   ```

5. Protocol/Format Response Mismatches
   - DeltaResponseSingleAction format mismatches
   ```
   DeltaResponseSingleAction(null,null,DeltaFormatResponseProtocol(DeltaProtocol(2,5,None,None))) did not equal DeltaResponseSingleAction(null,DeltaResponseMetadata(null,null),null)
   ```

6. Cloud Storage Integration Issues
   - Azure support test failures
   - Example: "[0]" did not equal "[8857161178873847034]"
   - GCP support test failures
   - Example: "[0]" did not equal "[217583862275277879]"

7. Pagination and Query Issues
   - Invalid page token handling
   - Array size mismatches in results
   ```
   ArrayBuffer(...) had size 3 instead of expected size 2 (IcebergSharedTableManagerSuite.scala:418)
   ```

## Test Statistics
- Total tests: 99
- Succeeded: 34
- Failed: 65
- Time: 9.4 seconds

## Affected Test Suites
1. io.delta.sharing.server.IcebergSharedTableManagerSuite
2. io.delta.sharing.server.IcebergSharingServiceSuite
