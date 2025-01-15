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

package io.delta.sharing.server

import java.util.concurrent.TimeUnit

import com.google.common.cache.CacheBuilder
import io.delta.standalone.internal.DeltaSharedTable

import io.delta.sharing.kernel.internal.DeltaSharedTableKernel
import io.delta.sharing.kernel.internal.IcebergSharedTableKernel
import io.delta.sharing.server.config.{ServerConfig, TableConfig}

/**
 * A class to load Delta tables from `TableConfig`. It also caches the loaded
 * tables internally to speed up the loading.
 */
class DeltaSharedTableLoader(serverConfig: ServerConfig) {
  private val deltaSharedTableCache = {
    CacheBuilder
      .newBuilder()
      .expireAfterAccess(60, TimeUnit.MINUTES)
      .maximumSize(serverConfig.deltaTableCacheSize)
      .build[String, DeltaSharedTableProtocol]()
  }

  def loadTable(
      tableConfig: TableConfig,
      useKernel: Boolean = false
  ): DeltaSharedTableProtocol = {
    try {
      val deltaSharedTable =
        deltaSharedTableCache.get(
          tableConfig.location,
          () => {
            createSharedTable(tableConfig, useKernel)
          }
        )
      if (!serverConfig.stalenessAcceptable) {
        deltaSharedTable.update()
      }
      deltaSharedTable
    } catch {
      case CausedBy(e: DeltaSharingUnsupportedOperationException) => throw e
      case e: Throwable => throw e
    }
  }

  private def createSharedTable(
      tableConfig: TableConfig,
      useKernel: Boolean
  ): DeltaSharedTableProtocol = {
    if (useKernel) {
      // Check table format and create appropriate implementation
      val format = serverConfig.defaultFormat
      format match {
        case "iceberg" =>
          new IcebergSharedTableKernel(
            tableConfig,
            serverConfig.preSignedUrlTimeoutSeconds,
            serverConfig.evaluatePredicateHints,
            serverConfig.evaluateJsonPredicateHints,
            serverConfig.evaluateJsonPredicateHintsV2,
            serverConfig.queryTablePageSizeLimit,
            serverConfig.queryTablePageTokenTtlMs,
            serverConfig.refreshTokenTtlMs
          )
        case "delta" | _ =>
          new DeltaSharedTableKernel(
            tableConfig,
            serverConfig.preSignedUrlTimeoutSeconds,
            serverConfig.evaluatePredicateHints,
            serverConfig.evaluateJsonPredicateHints,
            serverConfig.evaluateJsonPredicateHintsV2,
            serverConfig.queryTablePageSizeLimit,
            serverConfig.queryTablePageTokenTtlMs,
            serverConfig.refreshTokenTtlMs
          )
      }
    } else {
      new DeltaSharedTable(
        tableConfig,
        serverConfig.preSignedUrlTimeoutSeconds,
        serverConfig.evaluatePredicateHints,
        serverConfig.evaluateJsonPredicateHints,
        serverConfig.evaluateJsonPredicateHintsV2,
        serverConfig.queryTablePageSizeLimit,
        serverConfig.queryTablePageTokenTtlMs,
        serverConfig.refreshTokenTtlMs
      )
    }
  }
}
