package io.delta.standalone

import io.delta.standalone.internal.util.Clock
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

package object internal {
  def deltaLogForTableWithClock(hadoopConf: Configuration, path: String, clock: Clock): DeltaLog = {
    DeltaLogImpl.forTable(hadoopConf, path, clock = clock)
  }
  def deltaLogForTableWithClock(hadoopConf: Configuration, path: Path, clock: Clock): DeltaLog = {
    DeltaLogImpl.forTable(hadoopConf, path, clock = clock)
  }
}
