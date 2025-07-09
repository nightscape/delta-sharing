/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
