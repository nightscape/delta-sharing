package io.delta.sharing.server
// scalastyle:off
import com.dimafeng.testcontainers.{DockerComposeContainer, ExposedService}
import org.testcontainers.images.builder.ImageFromDockerfile
import zio.testcontainers._
import zio.{Task, ZIO, ZLayer, Scope}

import java.time.Duration
import java.util.Properties
import java.io.File
import java.nio.file.{Files, Paths}
import java.io.PrintWriter
import java.io.FileWriter

import com.dimafeng.testcontainers.WaitingForService
import org.testcontainers.containers.wait.strategy.WaitStrategy
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy
import org.testcontainers.containers.output.OutputFrame
import com.dimafeng.testcontainers.ServiceLogConsumer

import javax.security.auth.login.LoginContext
import javax.security.auth.Subject
import javax.security.auth.callback._

object DockerLayer {

  // Type alias for convenience
  type DockerEnv = ServiceEndpoints

  /**
   * Service endpoints accessible after docker-compose has started.
   */
  case class ServiceEndpoints(
      kerberosKdcHost: String,
      kerberosKdcPort: Int,
      kerberosAdminHost: String,
      kerberosAdminPort: Int,
      namenodeHost: String,
      namenodePort: Int,
      namenodeContainerName: String,
      knoxGatewayUrl: Option[String] = None
  )

  /**
   * Creates a layer that provides ServiceEndpoints based on the stack descriptor.
   * Uses either the pre-started environment or launches a new one via docker-compose.
   */
  def live(desc: StackDescriptor): ZLayer[Scope, Throwable, DockerEnv] =
    if (sys.env.contains("NAMENODE_HOST")) {
      preStarted(desc)
    } else {
      viaCompose(desc)
    }

  /**
   * Creates a layer that provides ServiceEndpoints by starting the docker-compose environment.
   */
  private def viaCompose(desc: StackDescriptor): ZLayer[Scope, Throwable, DockerEnv] =
    ZLayer.fromZIO {
      ZIO.logInfo(s"Starting docker-compose from ${desc.composeDir}") *>
      ZIOTestcontainers
        .toZIO(
          new DockerComposeContainer(
            desc.composeDir.resolve("docker-compose.yml").toFile,
            desc.exposedServices,
            tailChildContainers = true
          )
        )
        .map { docker =>
          val endpoints = ServiceEndpoints(
            kerberosKdcHost = docker.getServiceHost("kerberos-server", 88),
            kerberosKdcPort = docker.getServicePort("kerberos-server", 88),
            kerberosAdminHost = docker.getServiceHost("kerberos-server", 749),
            kerberosAdminPort = docker.getServicePort("kerberos-server", 749),
            namenodeHost = docker.getServiceHost("namenode", 8020),
            namenodePort = docker.getServicePort("namenode", 8020),
            namenodeContainerName =
              docker.getContainerByServiceName("namenode").get.getContainerId,
            knoxGatewayUrl = if (desc.usesKnox) {
              val host = docker.getServiceHost("knox-gateway", 8443)
              if (host.nonEmpty) Some(s"https://$host:8443/") else None
            } else None
          )

          ZIO.logInfo(s"Docker environment started with namenode at ${endpoints.namenodeHost}:${endpoints.namenodePort}") *>
          ZIO.succeed(endpoints)
        }.flatten
    }

  /**
   * Creates a layer that provides ServiceEndpoints from a pre-started environment.
   */
  private def preStarted(desc: StackDescriptor): ZLayer[Scope, Throwable, DockerEnv] =
    ZLayer.fromZIO {
      ZIO.logInfo("Using pre-started environment") *>
      ZIO.succeed {
        ServiceEndpoints(
          kerberosKdcHost = "localhost",
          kerberosKdcPort = 88,
          kerberosAdminHost = "localhost",
          kerberosAdminPort = 749,
          namenodeHost = "localhost",
          namenodePort = 8020,
          namenodeContainerName = sys.env("NAMENODE_HOST"),
          knoxGatewayUrl = if (desc.usesKnox) sys.env.get("KNOX_GATEWAY_URL") else None
        )
      }
    }

  def createKrb5Conf(
      kdcHost: String,
      kdcPort: Int,
      realm: String,
      adminPort: Option[Int] = None
  ): File = {
    val adminStuff = adminPort
      .map { port =>
        s"    admin_server = $kdcHost:$port\n"
      }
      .getOrElse("")
    val confContent =
      s"""[libdefaults]
           |  default_realm = $realm
           |  dns_lookup_kdc = false
           |  dns_lookup_realm = false
           |  ticket_lifetime = 24h
           |  renew_lifetime = 7d
           |  forwardable = true
           |  rdns = false
           |  udp_preference_limit = 1
           |
           |[realms]
           |  $realm = {
           |    kdc = $kdcHost:$kdcPort
           |    $adminStuff
           |  }
           |
           |[domain_realm]
           |  .${realm.toLowerCase} = $realm
           |  ${realm.toLowerCase} = $realm
           |""".stripMargin

    val tempFile = Files.createTempFile("krb5-", ".conf").toFile
    val writer = new PrintWriter(tempFile)
    try {
      writer.write(confContent)
    } finally {
      writer.close()
    }
    tempFile
  }

  def createJaasConf(principal: String): File = {
    val confContent =
      s"""KrbLogin {
           |  com.sun.security.auth.module.Krb5LoginModule required
           |  useKeyTab=true
           |  principal="$principal"
           |  storeKey=true
           |  useTicketCache=false
           |  debug=true;
           |};
           |""".stripMargin

    val tempFile = Files.createTempFile("jaas-", ".conf").toFile
    val writer = new PrintWriter(tempFile)
    try {
      writer.write(confContent)
    } finally {
      writer.close()
    }
    tempFile
  }

  /**
   * A simple CallbackHandler for Kerberos password-based login.
   */
  class NamePasswordKrbCallbackHandler(username: String, password: String)
      extends CallbackHandler {
    override def handle(callbacks: Array[Callback]): Unit = {
      callbacks.foreach {
        case nc: javax.security.auth.callback.NameCallback =>
          nc.setName(username)
        case pc: javax.security.auth.callback.PasswordCallback =>
          pc.setPassword(password.toCharArray)
        case other =>
          throw new UnsupportedOperationException(
            s"Unsupported callback: ${other.getClass.getName}"
          )
      }
    }
  }
}
