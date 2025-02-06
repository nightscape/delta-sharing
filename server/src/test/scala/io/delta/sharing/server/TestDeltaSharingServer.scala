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
// scalastyle:off
package io.delta.sharing.server

import java.io.File
import java.lang.management.ManagementFactory

import org.apache.commons.io.FileUtils

import io.delta.sharing.server.config.ServerConfig
import java.util.function.IntConsumer
import org.apache.hadoop.security.GroupMappingServiceProvider
import org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback
import _root_.com.linecorp.armeria.server.Server

// scalastyle:off
/**
 * This is a special test class for the client projects to test end-to-end experience. It will
 * generate configs for testing and start the server.
 */
object TestDeltaSharingServer extends TestServer {
  def main(args: Array[String]): Unit = {
    start(args(0), (port: Int) => {
      println(s"Server is running on port $port")
    })
  }
  def start(pidFileName: String, portCallback: IntConsumer): Unit = {
    val pid = ManagementFactory.getRuntimeMXBean().getName().split("@")(0)
    val pidFile = new File(pidFileName)
    // scalastyle:off println
    println(s"Writing pid $pid to $pidFile")
    // scalastyle:off on
    FileUtils.writeStringToFile(pidFile, pid)
    if (true || sys.env.get("AWS_ACCESS_KEY_ID").exists(_.length > 0)) {
      val serverConfigPath = LocalDeltaTestResource.setupTestTables().getCanonicalPath
      val stopServer = startWithConfig(serverConfigPath)
      // Run at most 420 seconds and exit. This is to ensure we can exit even if the parent process
      // hits any error.
      Thread.sleep(420000)
      stopServer.run()
    } else {
      throw new IllegalArgumentException("Cannot find AWS_ACCESS_KEY_ID in sys.env")
    }
  }
  def startWithConfig(serverConfigPath: String): Runnable = {
    val serverConfig = ServerConfig.load(serverConfigPath)
    println("serverConfigPath=" + serverConfigPath)
    println("serverConfig=" + serverConfig)
    val server = DeltaSharingService.start(serverConfig)
    val stopServer = new Runnable {
      def run(): Unit = {
        server.stop()
      }
    }
    stopServer
  }
}
