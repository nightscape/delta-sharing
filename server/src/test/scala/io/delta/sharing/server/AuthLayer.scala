package io.delta.sharing.server

import zio._
import DockerLayer.ServiceEndpoints
import javax.security.auth.Subject
import java.io.{File, PrintWriter}
import java.nio.file.Files

/**
 * Authentication layer that abstracts over different authentication mechanisms.
 */
object AuthLayer {
  /**
   * Sealed trait representing different authentication types.
   */
  sealed trait Auth
  final case class NoAuth() extends Auth
  final case class KerberosAuth(subject: Subject) extends Auth

  /**
   * Creates an authentication layer based on the stack descriptor.
   */
  def live(desc: StackDescriptor): ZLayer[ServiceEndpoints, Throwable, Auth] =
    if (desc.needsKerberosLogin) kerberos(desc) else ZLayer.succeed(NoAuth())

  /**
   * Creates a kerberos authentication layer.
   */
  private def kerberos(desc: StackDescriptor): ZLayer[ServiceEndpoints, Throwable, Auth] =
    ZLayer.scoped {
      for {
        endpoints <- ZIO.service[ServiceEndpoints]
        realm = desc.kerberosRealm.getOrElse("HADOOP.LOCAL")
        principal = s"tester@$realm"
        password = "tester"
        krb5Conf <- ZIO.attempt(
          createKrb5Conf(
            endpoints.kerberosKdcHost,
            endpoints.kerberosKdcPort,
            realm,
            Some(endpoints.kerberosAdminPort)
          )
        )
        jaasConf <- ZIO.attempt(createJaasConf(principal))
        _ <- ZIO.attempt {
          java.lang.System.setProperty("java.security.krb5.conf", krb5Conf.getAbsolutePath)
          java.lang.System.setProperty("java.security.auth.login.config", jaasConf.getAbsolutePath)
        }
        loginContext <- ZIO.attempt {
          val context = new javax.security.auth.login.LoginContext(
            "KrbLogin",
            new DockerLayer.NamePasswordKrbCallbackHandler(principal, password)
          )
          context.login()
          context
        }
        subject = loginContext.getSubject
        _ <- ZIO.logInfo(s"Login successful. Subject: $subject")
      } yield KerberosAuth(subject)
    }

  /**
   * Creates a krb5.conf file with the given KDC configuration.
   */
  private def createKrb5Conf(
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

  /**
   * Creates a JAAS configuration file for the given principal.
   */
  private def createJaasConf(principal: String): File = {
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
}
