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

package io.delta.sharing.client

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpHead
import org.apache.spark.delta.sharing.PreSignedUrlFetcher
import org.apache.spark.internal.Logging

import io.delta.sharing.client.util.ConfUtils

/**
 * A custom FileSystem implementation for HTTPS URLs that uses RandomAccessHttpInputStream
 * to support seeking operations needed for Parquet footer reading.
 *
 * This can be registered via Hadoop configuration to override the default HTTPS FileSystem:
 * - spark.hadoop.fs.https.impl=io.delta.sharing.client.DeltaSharingHttpsFileSystem
 * - spark.hadoop.fs.AbstractFileSystem.https.impl=io.delta.sharing.client.DeltaSharingHttpsAbstractFileSystem
 */
private[sharing] class DeltaSharingHttpsFileSystem extends FileSystem with Logging {

  lazy private val numRetries = ConfUtils.numRetries(getConf)
  lazy private val maxRetryDurationMillis = ConfUtils.maxRetryDurationMillis(getConf)
  lazy private val timeoutInSeconds = ConfUtils.timeoutInSeconds(getConf)
  lazy private val httpClient = createHttpClient()

  private[sharing] def createHttpClient() = {
    val proxyConfigOpt = ConfUtils.getProxyConfig(getConf)
    val maxConnections = ConfUtils.maxConnections(getConf)
    val neverUseHttps = ConfUtils.getNeverUseHttps(getConf)
    val config = RequestConfig.custom()
      .setConnectTimeout(timeoutInSeconds * 1000)
      .setConnectionRequestTimeout(timeoutInSeconds * 1000)
      .setSocketTimeout(timeoutInSeconds * 1000).build()

    logDebug(s"Creating delta sharing https httpClient with timeoutInSeconds: $timeoutInSeconds.")
    val clientBuilder = DeltaSharingFileSystemHttpClientBuilder.create()
      .setMaxConnTotal(maxConnections)
      .setMaxConnPerRoute(maxConnections)
      .setDefaultRequestConfig(config)
      .disableAutomaticRetries()

    if (neverUseHttps) {
      clientBuilder.setDisableHttps()
    }

    proxyConfigOpt.foreach { proxyConfig =>
      val proxy = new org.apache.http.HttpHost(proxyConfig.host, proxyConfig.port)
      clientBuilder.setProxy(proxy)
      clientBuilder.setNoProxyHosts(proxyConfig.noProxyHosts)
    }
    clientBuilder.build()
  }

  override def getScheme: String = "https"

  override def getUri(): URI = URI.create("https:///")

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    val uri = f.toUri

    logInfo(s"Opening HTTPS URL ${uri} using RandomAccessHttpInputStream for seeking support")

    // Create a simple fetcher that just returns the original URL
    val fetcher = new PreSignedUrlFetcher(
      null, // No cache ref needed for direct URLs
      uri.toString, // Use URL as table path
      "direct-url", // Simple file ID
      0 // No refresh needed for direct URLs
    ) {
      override def getUrl(): String = uri.toString
    }

    // Get content length via HEAD request
    val contentLength = getContentLength(uri)

    val httpInputStream = new RandomAccessHttpInputStream(
      httpClient,
      fetcher,
      contentLength,
      statistics,
      numRetries,
      maxRetryDurationMillis
    )
    new FSDataInputStream(httpInputStream)
  }

  private def getContentLength(uri: URI): Long = {
    val headRequest = new HttpHead(uri)
    val response = httpClient.execute(headRequest)
    try {
      val entity = response.getEntity
      if (entity != null) {
        entity.getContentLength
      } else {
        // Check Content-Length header
        val contentLengthHeader = response.getFirstHeader("Content-Length")
        if (contentLengthHeader != null) {
          contentLengthHeader.getValue.toLong
        } else {
          // Default to a large value if we can't determine size
          Long.MaxValue
        }
      }
    } catch {
      case e: Exception =>
        logWarning(s"Failed to get content length for ${uri}: ${e.getMessage}")
        Long.MaxValue
    } finally {
      if (response != null) {
        response.close()
      }
    }
  }

  override def create(
      f: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream =
    throw new UnsupportedOperationException("create not supported for HTTPS FileSystem")

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream =
    throw new UnsupportedOperationException("append not supported for HTTPS FileSystem")

  override def rename(src: Path, dst: Path): Boolean =
    throw new UnsupportedOperationException("rename not supported for HTTPS FileSystem")

  override def delete(f: Path, recursive: Boolean): Boolean =
    throw new UnsupportedOperationException("delete not supported for HTTPS FileSystem")

  override def listStatus(f: Path): Array[FileStatus] =
    throw new UnsupportedOperationException("listStatus not supported for HTTPS FileSystem")

  override def setWorkingDirectory(new_dir: Path): Unit = {
    // No-op for HTTPS FileSystem
  }

  override def getWorkingDirectory: Path = new Path(getUri)

  override def mkdirs(f: Path, permission: FsPermission): Boolean =
    throw new UnsupportedOperationException("mkdirs not supported for HTTPS FileSystem")

  override def getFileStatus(f: Path): FileStatus = {
    logDebug(s"Getting file status for HTTPS URL: ${f}")
    val resolved = makeQualified(f)

    // Get actual file size via HEAD request
    val contentLength = getContentLength(f.toUri)

    new FileStatus(contentLength, false, 0, 1, 0, f)
  }

  override def finalize(): Unit = {
    try super.finalize() finally close()
  }

  override def close(): Unit = {
    try super.close() finally httpClient.close()
  }
}
