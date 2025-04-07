package io.delta.sharing.server

import com.dimafeng.testcontainers.ExposedService
import java.nio.file.{Path, Paths}

/**
 * Configuration descriptor for a specific example stack (docker, docker-krb5, docker-knox).
 *
 * @param composeDir The directory containing the docker-compose.yml file
 * @param exposedServices List of services to expose from docker-compose
 * @param kerberosRealm Optional Kerberos realm (if this stack uses Kerberos)
 * @param usesKnox Whether this stack uses Knox Gateway
 * @param needsKerberosLogin Whether this stack requires Kerberos authentication
 */
final case class StackDescriptor(
  composeDir: Path,
  exposedServices: List[ExposedService],
  kerberosRealm: Option[String],
  usesKnox: Boolean,
  needsKerberosLogin: Boolean
)

/**
 * Registry of available example stacks.
 * This is the single source of truth for stack configurations.
 */
object StackRegistry {
  val examplesDir = Seq(Paths.get("examples"), Paths.get("../examples")).find(_.toFile.exists()).get
  val plain = StackDescriptor(
    examplesDir.resolve("docker"),
    List(
      ExposedService("namenode", 8020),
      ExposedService("datanode", 9865)
    ),
    kerberosRealm = None,
    usesKnox = false,
    needsKerberosLogin = false
  )

  val krb = StackDescriptor(
    examplesDir.resolve("docker-krb5"),
    List(
      ExposedService("kerberos-server", 88),
      ExposedService("kerberos-server", 749),
      ExposedService("namenode", 8020),
      ExposedService("datanode", 9865)
    ),
    kerberosRealm = Some("HADOOP.LOCAL"),
    usesKnox = false,
    needsKerberosLogin = true
  )

  val knox = StackDescriptor(
    examplesDir.resolve("docker-knox"),
    List(
      ExposedService("kerberos-server", 88),
      ExposedService("kerberos-server", 749),
      ExposedService("namenode", 8020),
      ExposedService("datanode", 9865),
      ExposedService("knox-gateway", 8443)
    ),
    kerberosRealm = None,
    usesKnox = true,
    needsKerberosLogin = false
  )

  /**
   * Get the stack descriptor based on the TEST_STACK environment variable.
   * If not set, defaults to the plain stack.
   */
  def fromEnv(): StackDescriptor =
    sys.env.get("TEST_STACK").map(_.toLowerCase) match {
      case Some("krb") => krb
      case Some("knox") => knox
      case _ => plain // default
    }
}
