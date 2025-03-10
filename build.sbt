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

import sbt.ExclusionRule

ThisBuild / parallelExecution := false

val previousSparkVersion = "3.5.3"
val latestSparkVersion = "4.0.0"
val scala212 = "2.12.18"
val scala213 = "2.13.13"
val scalaTestVersion = "3.2.15"

def sparkVersionFor(scalaVer: String): String = scalaVer match {
  case v if v.startsWith("2.12") => previousSparkVersion
  case v if v.startsWith("2.13") => latestSparkVersion
  case _ => sys.error(s"Unsupported Scala version: $scalaVer")
}

// Define common Hadoop exclusions to avoid dependency conflicts
lazy val hadoopExclusions = Seq(
  ExclusionRule("org.apache.hadoop", "hadoop-common"),
  ExclusionRule("org.apache.hadoop", "hadoop-hdfs-client"),
  ExclusionRule("org.apache.hadoop", "hadoop-yarn-api"),
  ExclusionRule("org.apache.hadoop", "hadoop-yarn-common"),
  ExclusionRule("org.apache.hadoop", "hadoop-yarn-client"),
  //ExclusionRule("org.apache.hadoop", "hadoop-mapreduce-client-core"),
  //ExclusionRule("org.apache.hadoop", "hadoop-mapreduce-client-common"),
  ExclusionRule("org.apache.hadoop", "hadoop-common"),
  ExclusionRule("org.apache.hadoop.thirdparty", "hadoop-shaded-guava"),
  ExclusionRule("org.apache.hadoop.thirdparty", "hadoop-shaded-protobuf_3_7")
)

// Define exclusions for log4j, JAX-RS, and Jersey conflicts
lazy val additionalExclusions = Seq(
  // Log4j conflicts
  ExclusionRule("ch.qos.reload4j", "reload4j"),
  ExclusionRule("org.apache.logging.log4j", "log4j-1.2-api"),
  // JAX-RS conflicts
  ExclusionRule("jakarta.ws.rs", "jakarta.ws.rs-api"),
  ExclusionRule("javax.ws.rs", "jsr311-api"),
  // Jersey conflicts
  ExclusionRule("com.sun.jersey", "jersey-server"),
  ExclusionRule("org.glassfish.jersey.core", "jersey-server")
)

lazy val commonSettings = Seq(
  organization := "io.delta",
  fork := true,
  // Configurations to speed up tests and reduce memory footprint
  Test / javaOptions ++= Seq(
    "-Dspark.ui.enabled=false",
    "-Dspark.ui.showConsoleProgress=false",
    "-Dspark.databricks.delta.snapshotPartitions=2",
    "-Dspark.sql.shuffle.partitions=5",
    "-Dspark.sql.sources.parallelPartitionDiscovery.parallelism=5",
    "-Dspark.delta.sharing.network.sslTrustAll=true",
    s"-Dazure.account.key=${sys.env.getOrElse("AZURE_TEST_ACCOUNT_KEY", "")}",
    "-Xmx1024m"
  )
)

lazy val java8Settings = Seq(
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  scalacOptions += "-target:jvm-1.8",
)

lazy val java17Settings = Seq(
  javacOptions ++= Seq("--release", "17"),
  Test / javaOptions ++= Seq(
    // Copied from SparkBuild.scala to support Java 17 for unit tests (see apache/spark#34153)
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
  ),
)

lazy val root = (project in file(".")).aggregate(client, spark, server)

lazy val client = (project in file("client")) settings(
  name := "delta-sharing-client",
  scalaVersion := scala213,
  crossScalaVersions := Seq(scala212, scala213),
  commonSettings,
  java17Settings,
  scalaStyleSettings,
  releaseSettings,
  libraryDependencies ++= {
    val sv = scalaVersion.value
    val sparkVer = sparkVersionFor(sv)
    Seq(
      "org.apache.httpcomponents" % "httpclient" % "4.5.14",
      "org.apache.spark" %% "spark-sql" % sparkVer % "provided",
      "org.apache.spark" %% "spark-catalyst" % sparkVer % "test" classifier "tests",
      "org.apache.spark" %% "spark-core" % sparkVer % "test" classifier "tests",
      "org.apache.spark" %% "spark-sql" % sparkVer % "test" classifier "tests",
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "org.scalatestplus" %% "mockito-4-11" % "3.2.18.0" % "test"
    )
  },
  Compile / sourceGenerators += Def.task {
    val file = (Compile / sourceManaged).value / "io" / "delta" / "sharing" / "client" / "package.scala"
    IO.write(file,
      s"""package io.delta.sharing
         |
         |package object client {
         |  val VERSION = "${version.value}"
         |}
         |""".stripMargin)
    Seq(file)
  },
  // Use scala-2.12 and scala-2.13 folders for version-specific overrides
  Compile / unmanagedSourceDirectories ++= {
    val sv = scalaVersion.value
    val base = (Compile / sourceDirectory).value
    sv match {
      case v if v.startsWith("2.12") => Seq(base / "scala-2.12")
      case v if v.startsWith("2.13") => Seq(base / "scala-2.13")
      case _ => Seq.empty
    }
  },
  Test / unmanagedSourceDirectories ++= {
    val sv = scalaVersion.value
    val base = (Test / sourceDirectory).value
    sv match {
      case v if v.startsWith("2.12") => Seq(base / "scala-2.12")
      case v if v.startsWith("2.13") => Seq(base / "scala-2.13")
      case _ => Seq.empty
    }
  }
)

lazy val spark = (project in file("spark")) dependsOn(client) settings(
  name := "delta-sharing-spark",
  scalaVersion := scala213,
  crossScalaVersions := Seq(scala213),
  commonSettings,
  java17Settings,
  scalaStyleSettings,
  releaseSettings,
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % latestSparkVersion % "provided",
    "org.apache.spark" %% "spark-catalyst" % latestSparkVersion % "test" classifier "tests",
    "org.apache.spark" %% "spark-core" % latestSparkVersion % "test" classifier "tests",
    "org.apache.spark" %% "spark-sql" % latestSparkVersion % "test" classifier "tests",
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
  ),
  Compile / sourceGenerators += Def.task {
    val file = (Compile / sourceManaged).value / "io" / "delta" / "sharing" / "spark" / "package.scala"
    IO.write(file,
      s"""package io.delta.sharing
         |
         |package object spark {
         |  val VERSION = "${version.value}"
         |}
         |""".stripMargin)
    Seq(file)
  }
)

lazy val generateJibClasspathFile = taskKey[File]("Generate Jib Classpath File")

lazy val server = (project in file("server"))
  .enablePlugins(JavaAppPackaging)
  .dependsOn(client % "test->test")
  .configs(IntegrationTest)
  .settings(
  name := "delta-sharing-server",
  scalaVersion := scala213,
  commonSettings,
  java8Settings,
  scalaStyleSettings,
  releaseSettings,
  dockerUsername := Some("deltaio"),
  dockerBuildxPlatforms := Seq("linux/arm64", "linux/amd64"),
  scriptClasspath ++= Seq("../conf"),
  // Keep the merge strategy as a fallback for any remaining conflicts
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case "module-info.class" => MergeStrategy.discard
    case "reference.conf" => MergeStrategy.concat
    case "application.conf" => MergeStrategy.concat
    case "plugin.properties" => MergeStrategy.concat
    case "log4j.properties" => MergeStrategy.concat
    case "git.properties" => MergeStrategy.concat
    case "overview.html" => MergeStrategy.discard
    // Handle Hadoop duplicate classes
    case PathList("org", "apache", "hadoop", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", "spark", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.first
    // Handle Log4j conflicts
    case PathList("org", "apache", "log4j", xs @ _*) => MergeStrategy.first
    case PathList("ch", "qos", "reload4j", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", "logging", "log4j", xs @ _*) => MergeStrategy.first
    // Handle JAX-RS conflicts
    case PathList("javax", "ws", "rs", xs @ _*) => MergeStrategy.first
    case PathList("jakarta", "ws", "rs", xs @ _*) => MergeStrategy.first
    // Handle Jersey conflicts
    case PathList("com", "sun", "research", "ws", "wadl", xs @ _*) => MergeStrategy.first
    case PathList("jersey", "repackaged", xs @ _*) => MergeStrategy.first
    // Other common conflicts
    case PathList("com", "google", xs @ _*) => MergeStrategy.first
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.first
    case PathList("com", "codahale", xs @ _*) => MergeStrategy.first
    case PathList("com", "yammer", xs @ _*) => MergeStrategy.first
    case PathList("org", "slf4j", xs @ _*) => MergeStrategy.first
    case PathList("io", "netty", xs @ _*) => MergeStrategy.first
    case PathList("org", "aopalliance", xs @ _*) => MergeStrategy.first
    case PathList("javax", "inject", xs @ _*) => MergeStrategy.first
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("javax", "activation", xs @ _*) => MergeStrategy.first
    case PathList("javax", "annotation", xs @ _*) => MergeStrategy.first
    case PathList("javax", "xml", xs @ _*) => MergeStrategy.first
    case PathList("org", "objenesis", xs @ _*) => MergeStrategy.first
    case PathList("scala", xs @ _*) => MergeStrategy.first
    case PathList("net", "jpountz", xs @ _*) => MergeStrategy.first
    case PathList("org", "tukaani", xs @ _*) => MergeStrategy.first
    case PathList("org", "xerial", xs @ _*) => MergeStrategy.first
    case PathList("org", "joda", xs @ _*) => MergeStrategy.first
    case PathList("org", "json4s", xs @ _*) => MergeStrategy.first
    case PathList("org", "xml", xs @ _*) => MergeStrategy.first
    case PathList("org", "w3c", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", "hadoop", "thirdparty", xs @ _*) => MergeStrategy.first
    case x =>
      val oldStrategy = (assembly / assemblyMergeStrategy).value
      oldStrategy(x)
  },
  libraryDependencies ++= Seq(
    // Pin versions for jackson libraries as the new version of `jackson-module-scala` introduces a
    // breaking change making us not able to use `delta-standalone`.
    "com.fasterxml.jackson.core" % "jackson-core" % "2.15.2",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2",
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.15.2",
    "org.json4s" %% "json4s-jackson" % "3.7.0-M11" excludeAll(
      (Seq(
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module")
      ) ++ additionalExclusions): _*
    ),
    "com.linecorp.armeria" %% "armeria-scalapb" % "1.9.2" excludeAll(
      (Seq(
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module"),
        ExclusionRule("org.json4s")
      ) ++ additionalExclusions): _*
    ),
    "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf" excludeAll(
      (Seq(
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module"),
        ExclusionRule("org.json4s")
      ) ++ additionalExclusions): _*
    ),
    // Apply Hadoop exclusions to avoid duplicate classes
    "org.apache.hadoop" % "hadoop-aws" % "3.3.4" excludeAll(
      (Seq(
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module"),
        ExclusionRule("com.google.guava", "guava"),
        ExclusionRule("com.amazonaws", "aws-java-sdk-bundle")
      ) ++ hadoopExclusions ++ additionalExclusions): _*
    ),
    "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.189" excludeAll(
      additionalExclusions: _*
    ),
    // Apply Hadoop exclusions to avoid duplicate classes
    "org.apache.hadoop" % "hadoop-azure" % "3.3.4" excludeAll(
      (Seq(
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module"),
        ExclusionRule("com.google.guava", "guava")
      ) ++ hadoopExclusions ++ additionalExclusions): _*
    ),
    "com.google.cloud" % "google-cloud-storage" % "2.2.2" excludeAll(
      (Seq(
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module")
      ) ++ additionalExclusions): _*
    ),
    "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop2-2.2.4" excludeAll(
      (Seq(
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module")
      ) ++ hadoopExclusions ++ additionalExclusions): _*
    ),
    // Apply Hadoop exclusions to avoid duplicate classes
    // "org.apache.hadoop" % "hadoop-common" % "3.3.4" excludeAll(
    //   (Seq(
    //     ExclusionRule("com.fasterxml.jackson.core"),
    //     ExclusionRule("com.fasterxml.jackson.module"),
    //     ExclusionRule("com.google.guava", "guava")
    //   ) ++ hadoopExclusions ++ additionalExclusions): _*
    // ),
    // Apply Hadoop exclusions to avoid duplicate classes
    "org.apache.hadoop" % "hadoop-client" % "3.3.4" excludeAll(
      (Seq(
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module"),
        ExclusionRule("com.google.guava", "guava")
      ) ++ hadoopExclusions ++ additionalExclusions): _*
    ),
    "org.apache.parquet" % "parquet-hadoop" % "1.12.3" excludeAll(
      (Seq(
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module"),
        ExclusionRule("com.google.guava", "guava")
      ) ++ hadoopExclusions ++ additionalExclusions): _*
    ),
    "io.delta" %% "delta-standalone" % "3.2.0" excludeAll(
      (Seq(
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module"),
        ExclusionRule("com.google.guava", "guava")
      ) ++ hadoopExclusions ++ additionalExclusions): _*
    ),
    "io.delta" % "delta-kernel-api" % "3.2.0" excludeAll(
      (Seq(
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module"),
        ExclusionRule("com.google.guava", "guava")
      ) ++ hadoopExclusions ++ additionalExclusions): _*
    ),
    "io.delta" % "delta-kernel-defaults" % "3.2.0" excludeAll(
      (Seq(
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module"),
        ExclusionRule("com.google.guava", "guava")
      ) ++ hadoopExclusions ++ additionalExclusions): _*
    ),
    "org.apache.spark" %% "spark-sql" % "3.3.4" excludeAll(
      (Seq(
        ExclusionRule("org.slf4j"),
        ExclusionRule("io.netty"),
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module"),
        ExclusionRule("org.json4s"),
        ExclusionRule("com.google.guava", "guava")
      ) ++ hadoopExclusions ++ additionalExclusions): _*
    ),
    "org.slf4j" % "slf4j-api" % "1.6.1",
    "org.slf4j" % "slf4j-simple" % "1.6.1",
    "net.sourceforge.argparse4j" % "argparse4j" % "0.9.0",

    "org.apache.parquet" % "parquet-avro" % "1.12.3" % "test" excludeAll(additionalExclusions: _*),
    "org.scalatest" %% "scalatest" % "3.2.19" % "test" excludeAll(additionalExclusions: _*),
    "dev.zio" %% "zio-test" % "2.1.14" % "test" excludeAll(additionalExclusions: _*),
    "dev.zio" %% "zio-test-sbt" % "2.1.14" % "test" excludeAll(additionalExclusions: _*),
    "com.github.jatcwang" %% "difflicious-core" % "0.4.3" % "test" excludeAll(additionalExclusions: _*),,
    "org.bouncycastle" % "bcprov-jdk15on" % "1.70" % "test",
    "org.bouncycastle" % "bcpkix-jdk15on" % "1.70" % "test"
    "com.github.sideeffffect" %% "zio-testcontainers" % "0.6.0" % "test" excludeAll(additionalExclusions: _*),
    "com.dimafeng" %% "testcontainers-scala-core" % "0.41.8" % "test" excludeAll(additionalExclusions: _*),
    "org.testcontainers" % "testcontainers" % "1.20.4" % "test" excludeAll(additionalExclusions: _*),
  ),
  Compile / PB.targets := Seq(
    scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
  ),
  jibBaseImage := "openjdk:11",
  jibOrganization := "nightscape",
  jibName := "delta-sharing-server",
  generateJibClasspathFile := {
    println("Generating jib classpath file")
    val cp: Seq[File] = (Runtime / fullClasspath).value.map(_.data)
    val cpString = (cp.map(f => s"/app/libs/${f.getName}") ++ Seq("/app/resources", "/app/classes", "/app/dependency/*")).mkString(":")
    val outFile = target.value / "jib-classpath-file"
    IO.write(outFile, cpString)
    outFile
  },
  jibMappings := {
    val cpFile = generateJibClasspathFile.value
    Seq(
      cpFile -> "/app/classpath/jib-classpath-file"
    )
  },
  jibJavaAddToClasspath := List(target.value / "jib-classpath-file"),
  jibArgs := List("-c", "/server-config.yaml"),
  jibTcpPorts := List(8000),
  jibUseCurrentTimestamp := true,
  jibExtraMappings := {
    Seq(
      generateJibClasspathFile.value -> "/app/classpath/jib-classpath-file",
    )
  },
  jibEntrypoint := Some(List(
    "java",
    "-cp",
    "@/app/classpath/jib-classpath-file",
    "io.delta.sharing.server.DeltaSharingService"
  )),
  aggregate in findJarContainingClass := false,
  findJarContainingClass := {
    // Parse command-line arguments; expects a fully-qualified class name.
    val args: Seq[String] = spaceDelimited("<class-name>").parsed
    if (args.isEmpty)
      sys.error("Usage: server:findJarContainingClass <fully-qualified-class-name>")

    // Convert the fully qualified class name to a resource path.
    val className = args.head
    val classFile = className//.replace('.', '/') + ".class"

    // Here the key point: this picks up the server module's runtime classpath.
    val cpFiles: Seq[File] = (Runtime / fullClasspath).value.map(_.data)
    println(s"cpFiles: ${cpFiles.mkString("\n")}")
    println(s"Searching for $classFile in server module's runtime classpath")

    streams.value.log.info(
      s"Searching for JARs that contain $classFile in server module's runtime classpath"
    )

    // Scan each JAR for the specific class.
    cpFiles.filter(_.getName.endsWith(".jar")).foreach { jar =>
      val jarFile = new java.util.jar.JarFile(jar)
      try {
        //println(s"jarFile: ${jarFile.entries().asScala.mkString("\n")}")
        val matches = jarFile.entries().asScala.filter(_.toString.matches(classFile))
        if (matches.nonEmpty) {
          streams.value.log.info(s"Found in: ${jar.getAbsolutePath}\n${matches.mkString("\n")}")
        }
      } finally {
        jarFile.close()
      }
    }
  }
)

/*
 ***********************
 * ScalaStyle settings *
 ***********************
 */
ThisBuild / scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

lazy val scalaStyleSettings = Seq(
  compileScalastyle := (Compile / scalastyle).toTask("").value,
  (Compile / compile) := ((Compile / compile) dependsOn compileScalastyle).value,
  testScalastyle := (Test / scalastyle).toTask("").value,
  (Test / test) := ((Test / test) dependsOn testScalastyle).value
)

/*
 ********************
 * Release settings *
 ********************
 */
import ReleaseTransformations._

lazy val releaseSettings = Seq(
  publishMavenStyle := true,
  publishArtifact := true,
  Test / publishArtifact := false,

  publishTo := {
    val ossrhBase = "https://ossrh-staging-api.central.sonatype.com/"
    if (isSnapshot.value) {
      Some("snapshots" at ossrhBase + "content/repositories/snapshots")
    } else {
      Some("releases" at ossrhBase + "service/local/staging/deploy/maven2")
    }
  },

  releasePublishArtifactsAction := PgpKeys.publishSigned.value,

  releaseCrossBuild := true,

  licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),

  pomExtra :=
    <url>https://github.com/delta-io/delta-sharing</url>
      <scm>
        <url>git@github.com:delta-io/delta-sharing.git</url>
        <connection>scm:git:git@github.com:delta-io/delta-sharing.git</connection>
      </scm>
      <developers>
        <developer>
          <id>marmbrus</id>
          <name>Michael Armbrust</name>
          <url>https://github.com/marmbrus</url>
        </developer>
        <developer>
          <id>jose-torres</id>
          <name>Jose Torres</name>
          <url>https://github.com/jose-torres</url>
        </developer>
        <developer>
          <id>ueshin</id>
          <name>Takuya UESHIN</name>
          <url>https://github.com/ueshin</url>
        </developer>
        <developer>
          <id>mateiz</id>
          <name>Matei Zaharia</name>
          <url>https://github.com/mateiz</url>
        </developer>
        <developer>
          <id>zsxwing</id>
          <name>Shixiong Zhu</name>
          <url>https://github.com/zsxwing</url>
        </developer>
        <developer>
          <id>linzhou-db</id>
          <name>Lin Zhou</name>
          <url>https://github.com/linzhou-db</url>
        </developer>
        <developer>
          <id>chakankardb</id>
          <name>Abhijit Chakankar</name>
          <url>https://github.com/chakankardb</url>
        </developer>
        <developer>
          <id>charlenelyu-db</id>
          <name>Charlene Lyu</name>
          <url>https://github.com/charlenelyu-db</url>
        </developer>
        <developer>
          <id>zhuansunxt</id>
          <name>Xiaotong Sun</name>
          <url>https://github.com/zhuansunxt</url>
        </developer>
        <developer>
          <id>wchau</id>
          <name>William Chau</name>
          <url>https://github.com/wchau</url>
        </developer>
      </developers>
)

// Looks like some of release settings should be set for the root project as well.
publishArtifact := false  // Don't release the root project
publish := {}
publishTo := Some("snapshots" at "https://ossrh-staging-api.central.sonatype.com/content/repositories/snapshots")
releaseCrossBuild := false
// crossScalaVersions must be set to Nil on the root project
crossScalaVersions := Nil
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommandAndRemaining("+publishSigned"),
  setNextVersion,
  commitNextVersion
)

// Top-level declaration of the task key is fine:
val findJarContainingClass = inputKey[Unit](
  "Find JARs on the runtime classpath that contain the given class"
)

// In the root project settings:
aggregate in findJarContainingClass := false
