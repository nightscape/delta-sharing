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

  case class ServiceEndpoints(
      kerberosKdcHost: String,
      kerberosKdcPort: Int,
      kerberosAdminHost: String,
      kerberosAdminPort: Int,
      namenodeHost: String,
      namenodePort: Int,
      namenodeContainerName: String
  )

  val hadoopTestContainerLayer: ZLayer[Scope, Throwable, ServiceEndpoints] =
    ZLayer.fromZIO {
      ZIOTestcontainers
        .toZIO(
          new DockerComposeContainer(
            new File("server/src/test/resources/docker-compose.yml"),
            List(
              new ExposedService("kerberos-server", 88),
              new ExposedService("kerberos-server", 749),
              new ExposedService("namenode", 8020),
              new ExposedService("datanode", 9865)
            ),
            tailChildContainers = true
          )
        )
        .map { docker =>
          ServiceEndpoints(
            kerberosKdcHost = docker.getServiceHost("kerberos-server", 88),
            kerberosKdcPort = docker.getServicePort("kerberos-server", 88),
            kerberosAdminHost = docker.getServiceHost("kerberos-server", 749),
            kerberosAdminPort = docker.getServicePort("kerberos-server", 749),
            namenodeHost = docker.getServiceHost("namenode", 8020),
            namenodePort = docker.getServicePort("namenode", 8020),
            namenodeContainerName =
              docker.getContainerByServiceName("namenode").get.getContainerId
          )
        }
    }
  val hadoopPreStartedLayer: ZLayer[Scope, Throwable, ServiceEndpoints] =
    ZLayer.succeed {
      ServiceEndpoints(
        kerberosKdcHost = "localhost",
        kerberosKdcPort = 88,
        kerberosAdminHost = "localhost",
        kerberosAdminPort = 749,
        namenodeHost = "localhost",
        namenodePort = 8020,
        namenodeContainerName = sys.env("NAMENODE_HOST")
      )
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
    println(s"Creating JAAS config for $principal")
    println(confContent)
    val tempFile = Files.createTempFile("jaas-", ".conf").toFile
    val writer = new PrintWriter(tempFile)
    try {
      writer.write(confContent)
    } finally {
      writer.close()
    }
    tempFile
  }

  /** ZLayer that logs in via Kerberos using JAAS and returns the authenticated
    * Subject.
    *
    * This layer
    *   - sets the necessary system properties for Kerberos,
    *   - creates temporary configuration files using createKrb5Conf and
    *     createJaasConf,
    *   - logs in using a LoginContext with a password callback.
    */
  val kerberosSubjectLayer: ZLayer[ServiceEndpoints, Throwable, Subject] =
    ZLayer.fromZIO {
      for {
        endpoints <- ZIO.service[ServiceEndpoints]
        config <- ZIO.attempt {
          val kerberosHost = endpoints.kerberosKdcHost
          val kerberosPort = endpoints.kerberosKdcPort

          val realm = "HADOOP.LOCAL"
          val principal = s"tester@$realm"
          val password = "tester"
          // Enable Kerberos debug logging if needed
          // System.setProperty("sun.security.krb5.debug", "true")
          // Create temporary krb5.conf and JAAS config files
          val krb5Conf = createKrb5Conf(kerberosHost, kerberosPort, realm)
          val jaasConf = createJaasConf(principal)
          System.setProperty(
            "java.security.krb5.conf",
            krb5Conf.getAbsolutePath
          )
          System.setProperty(
            "java.security.auth.login.config",
            jaasConf.getAbsolutePath
          )

          // Create a LoginContext using a custom callback handler for password-based login
          val loginContext = new LoginContext(
            "KrbLogin",
            new NamePasswordKrbCallbackHandler(principal, password)
          )
          loginContext.login()
          println("Login successful. Subject: " + loginContext.getSubject)
          loginContext.getSubject
        }
      } yield config
    }

  /** A simple CallbackHandler for Kerberos password-based login.
    *
    * This implementation handles NameCallback and PasswordCallback by setting
    * the provided username and password.
    */
  class NamePasswordKrbCallbackHandler(username: String, password: String)
      extends CallbackHandler {
    override def handle(callbacks: Array[Callback]): Unit = {
      println("Handling callbacks")
      callbacks.foreach {
        case nc: javax.security.auth.callback.NameCallback =>
          println(s"NameCallback: ${nc.getName}")
          nc.setName(username)
        case pc: javax.security.auth.callback.PasswordCallback =>
          pc.setPassword(password.toCharArray)
          println("PasswordCallback set")
        case other =>
          throw new UnsupportedOperationException(
            s"Unsupported callback: ${other.getClass.getName}"
          )
      }
    }
  }
}
