package io.delta.sharing.server

// scalastyle:off
import difflicious._
import difflicious.implicits._
import io.delta.sharing.client.model.Table
import io.delta.sharing.client.{DeltaSharingClient, DeltaSharingRestClient}
import io.delta.standalone.Operation
import io.delta.standalone.actions._
import io.delta.standalone.internal._
import io.delta.standalone.internal.util.newManualClock
import io.delta.standalone.types._
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{
  DataType => SparkDataType,
  StructType => SparkStructType
}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import zio.{Differ => _, _}
import zio.test._

import java.io.File
import java.net.ServerSocket
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.collection.JavaConverters._
import io.delta.sharing.server.config.ServerConfig
import io.delta.sharing.server.config.TableConfig
import io.delta.sharing.server.config.SchemaConfig
import io.delta.sharing.server.config.ShareConfig
import io.delta.sharing.server.config.Authorization
import org.apache.hadoop.fs.RemoteIterator
import scala.collection.mutable.ArrayBuffer
import io.delta.sharing.client.{model => clientModel}
import io.delta.sharing.client.DeltaSharingProfile
import zio.test.TestAspect._
import zio.test.Assertion._

object SimplePropertyTest extends ZIOSpecDefault {
  val exampleLayer = ZLayer.succeed {
    println("Creating example layer")
    "example"
  }

  def spec = suite("checks")(
    test("can interact with services in the environment") {
      check(Gen.fromZIO(ZIO.serviceWithZIO[CounterService](_.get.debug("GETTING")))) { n =>
        (for {
          service <- ZIO.service[CounterService]
          c <- service.get //.debug("c")
          _ <- service.increment(n + 1)
          m <- service.get //.debug("m")
        } yield assert(m)(equalTo(c + n + 1)))
      }.provide(CounterService.live.fresh)
    }
      //.@@(checksZIO(resetCounterServiceAspect))
  ) @@ sequential

  trait CounterService {
    def get: UIO[Int]
    def increment(amount: Int): UIO[Unit]
    def reset: UIO[Unit]
  }

  object CounterService {
    val live: ULayer[CounterService] = ZLayer {
      for {
        ref <- Ref.make[Int](0)
      } yield new CounterService {
        def get: UIO[Int] = ref.get
        def increment(amount: Int): UIO[Unit] = ref.update(_ + amount)
        def reset: UIO[Unit] = ref.set(0)
      }
    }
  }

  val resetCounterServiceAspect = ZIO.serviceWith[CounterService] { cs =>
    new TestAspect.CheckAspect {
      def apply[R, E, A](zio: ZIO[R, E, A])(implicit trace: Trace) =
        zio <* cs.reset
    }
  }
}
