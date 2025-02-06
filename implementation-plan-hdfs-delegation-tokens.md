# Implementation Plan: HDFS Delegation Tokens Support for Delta Sharing Server

## Overview

This plan outlines the steps needed to extend the Delta Sharing server, currently implemented for cloud storage, to support HDFS storage using Hadoop Delegation Tokens for authentication. The goal is for the server to return URLs for Parquet files hosted on HDFS, with built-in authentication tokens that allow clients to fetch these files securely. This updated plan also integrates additional details regarding authentication and authorization, ensuring that only users with proper bearer tokens can access the data they are allowed to see.

Reference: [Hadoop Delegation Tokens Explained](https://blog.cloudera.com/hadoop-delegation-tokens-explained/)

## 1. Configuration Changes

- Add a new configuration option for storage type (e.g., `storage.type`) with possible values "cloud" (default) and "hdfs".
- For HDFS mode, include additional configuration parameters such as:
  - `hdfs.namenode.uri`
  - `hdfs.kerberos.principal`
  - `hdfs.keytab.path`
  - Optional parameters like token renewal interval and timeout settings.
- Include configuration for authorization: the server must maintain a mapping between bearer tokens and the shares/schemas/tables that a recipient is allowed to access. Bearer tokens, provided in the profile file or request headers, are used for both authentication and authorization.

## 2. Abstract Token Provider

- Introduce an interface or abstract class (e.g., `TokenProvider`) that defines methods to generate and renew authentication tokens for file access URLs.
- Refactor the existing cloud token generation logic to implement this interface, ensuring both cloud and HDFS implementations follow the same contract.

## 3. New HDFS Delegation Token Provider

- Create a new implementation, e.g., `HdfsDelegationTokenProvider`, that:
  - Uses Hadoop's `UserGroupInformation` (UGI) for Kerberos authentication.
  - Obtains delegation tokens by invoking the `getDelegationToken` API on a Hadoop `FileSystem` instance.
  - Handles token caching and renewal using a background process, renewing tokens before they expire.
- Ensure that if the cached token is close to expiry, it is refreshed automatically to maintain uninterrupted access.

## 4. URL Generation and CloudFileSigner Integration

- Refactor the URL generation mechanism to integrate with the existing `CloudFileSigner` interface used by the server.
- Create a new file signer implementation for HDFS, e.g., `HdfsFileSigner`, that leverages the `HdfsDelegationTokenProvider` to acquire a delegation token and appends it as a query parameter to the HDFS file URL.
- The signed URL should also include an expiration timestamp ensuring that clients are given a time-limited URL for accessing the data.

## 5. Integration into Delta Sharing Server

- Identify and update server components that currently determine the file signer based on the storage type.
- Refactor these components so that when the configuration is set to "hdfs", the server instantiates `HdfsFileSigner` by providing it with the Hadoop `Configuration`, an instance of `HdfsDelegationTokenProvider`, and the pre-signed URL timeout.
- Ensure that token generation, renewal, and URL signing are performed in a manner consistent with the Delta Sharing protocol.

## 6. Authorization and Access Control

- **Bearer Token Validation:** The Delta Sharing protocol requires every REST request to include an `Authorization: Bearer {token}` header. The server must validate the provided bearer token, checking its authenticity, expiration, and signature (per RFC6750).

- **Mapping Tokens to Data Access:** Once validated, the bearer token is used to identify the recipient. The server is responsible for mapping this token to a set of allowed shares, schemas, and tables. Only the metadata and file URLs for authorized datasets should be returned to the recipient.

- **Enforcement:** If a request is made with an invalid or unauthorized token, the server must reject the request with appropriate HTTP error codes (e.g., 401 Unauthorized or 403 Forbidden).

- **Profile File Role:** The Profile File, which includes fields such as `endpoint`, `bearerToken`, and optionally `expirationTime`, is the source of the client's credentials. The server uses these credentials to enforce access control and ensure that users view only the datasets they are permitted to see.

## 7. Testing

- Create comprehensive integration tests that simulate HDFS access in a Kerberos-enabled environment (e.g., a Cloudera cluster).
  - Validate that delegation tokens are correctly generated, attached to file URLs, and renewed as expected.
  - Verify that the server properly validates bearer tokens and restricts data access based on the recipient's permissions.
  - Test error conditions such as expired tokens, unauthorized token access, and token renewal failures.

## 8. Documentation and Deployment

- Update the Delta Sharing server documentation to include details on HDFS support:
  - Explain the new configuration options and required credentials (e.g., Kerberos keytab, principal, and HDFS URI).
  - Document the authorization flow in detail, describing how the bearer token is used to authenticate and authorize recipient access to specific shares and tables.
  - Provide security best practices for managing Kerberos credentials and token lifetimes.
- Ensure that deployment scripts and CI/CD pipelines are updated to test both cloud and HDFS storage modes with proper authorization enforcement.

## 9. Next Steps

- Conduct a thorough code review of the new changes.
- Merge and deploy the updated server in a staging environment.
- Monitor the token renewal process and authorization enforcement in production and handle edge cases as needed.

## Concrete implementation

Below is a sketch of key Scala classes to support HDFS delegation token generation and integration with CloudFileSigner:

```scala
package com.delta.sharing.auth

// Abstract TokenProvider trait defining the contract for token generation and renewal.
trait TokenProvider {
  def generateToken(filePath: String): String
  def renewToken(): Unit
}
```

```scala
package com.delta.sharing.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.UserGroupInformation
import com.delta.sharing.auth.TokenProvider

// HdfsDelegationTokenProvider implementation using Hadoop's delegation token mechanism.
class HdfsDelegationTokenProvider(conf: Configuration) extends TokenProvider {
  private var cachedToken: Option[String] = None

  // Obtain a delegation token for the given file's URL.
  override def generateToken(filePath: String): String = {
    // Set up the Hadoop configuration and authentication (e.g., Kerberos)
    val fs = FileSystem.get(conf)
    val delegationToken = fs.getDelegationToken("renewer");

    // Obtain delegation tokens via UGI and FileSystem APIs (this is pseudocode)
    // e.g., UserGroupInformation.getCurrentUser.addDelegationTokens("service", fs)
    val token = "delegationTokenPlaceholder" // Replace with actual token logic
    cachedToken = Some(token)
    token
  }

  // Renew the delegation token before it expires.
  override def renewToken(): Unit = {
    // Invalidate the current token and fetch a new one.
    cachedToken = None
    generateToken("")
  }
}
```

```scala
package io.delta.sharing.server.common

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.delta.sharing.auth.TokenProvider

// PreSignedUrl holds the signed URL and its expiration timestamp in milliseconds.
case class PreSignedUrl(url: String, expirationTimestamp: Long)

// CloudFileSigner trait used by the Delta Sharing Server for generating signed URLs.
trait CloudFileSigner {
  def sign(path: Path): PreSignedUrl
}

// HdfsFileSigner implements CloudFileSigner for HDFS by leveraging Hadoop delegation tokens.
class HdfsFileSigner(
    conf: Configuration,
    tokenProvider: TokenProvider,
    preSignedUrlTimeoutSeconds: Long) extends CloudFileSigner {
  override def sign(path: Path): PreSignedUrl = {
    val token = tokenProvider.generateToken(path.toString)
    val url = s"${path.toUri.toString}?delegationToken=${token}"
    PreSignedUrl(url, System.currentTimeMillis() + preSignedUrlTimeoutSeconds * 1000)
  }
}
```

## References

- Cloudera Blog: [Hadoop Delegation Tokens Explained](https://blog.cloudera.com/hadoop-delegation-tokens-explained/)
- Delta Sharing Protocol (refer to sections on bearer token usage and profile file format): [PROTOCOL.md](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md)
