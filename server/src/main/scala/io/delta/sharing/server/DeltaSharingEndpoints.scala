package io.delta.sharing.server

import sttp.tapir.{Schema => TapirSchema, Tapir}
import sttp.tapir.generic.auto._
import io.delta.sharing.server.protocol._
import scalapb.UnknownFieldSet
import sttp.tapir.json.json4s.TapirJson4s
import org.json4s.Formats
import org.json4s.DefaultFormats
import org.json4s.Serialization
import com.google.protobuf.ByteString

object DeltaSharingTapir extends Tapir
  with TapirJson4s

import DeltaSharingTapir._
object DeltaSharingEndpoints {
  // Common error response model
  case class ErrorResponse(errorCode: String, message: String)
  object ErrorResponse {
    //implicit val rw: ReadWriter[ErrorResponse] = macroRW
    def StatusNotFound(message: String) = ErrorResponse("404 notFound", message)
    def StatusBadRequest(message: String) = ErrorResponse("400 badRequest", message)
    def StatusForbidden(message: String) = ErrorResponse("403 forbidden", message)
    def StatusUnauthorized(message: String) = ErrorResponse("401 unauthorized", message)
    def StatusInternalServerError(message: String) = ErrorResponse("500 backendError", message)
    def StatusNoContent = ErrorResponse("204", "")
  }
  // Common error responses

  // Response models
  //implicit val rwUnknownFieldSet: ReadWriter[UnknownFieldSet] = readwriter[Map[String, Any]].bimap[UnknownFieldSet]()
  //implicit val rwUnknownField: ReadWriter[UnknownFieldSet.Field] = macroRW
  //implicit val rwShare: ReadWriter[Share] = macroRW
  //implicit val rwListSharesResponse: ReadWriter[ListSharesResponse] = macroRW

  //implicit val rwSchema: ReadWriter[Schema] = macroRW
  //implicit val rwListSchemasResponse: ReadWriter[ListSchemasResponse] = macroRW

  //implicit val rwTable: ReadWriter[Table] = macroRW
  //implicit val rwListTablesResponse: ReadWriter[ListTablesResponse] = macroRW


  // Response models for table metadata and queries
  case class QueryTableRequest(
    predicateHints: Seq[String] = Nil,
    jsonPredicateHints: Option[Map[String, String]] = None,
    limitHint: Option[Long] = None,
    version: Option[Long] = None,
    timestamp: Option[String] = None,
    startingVersion: Option[Long] = None,
    endingVersion: Option[Long] = None,
    maxFiles: Option[Int] = None,
    pageToken: Option[String] = None,
    includeRefreshToken: Option[Boolean] = None,
    refreshToken: Option[String] = None,
    idempotencyKey: Option[String] = None
  )
  object QueryTableRequest {
    implicit val schema: TapirSchema[QueryTableRequest] =
      TapirSchema.derived
  }

  case class GetQueryInfoRequest(
    maxFiles: Option[Int] = None,
    pageToken: Option[String] = None
  )
  object GetQueryInfoRequest {
    implicit val schema: TapirSchema[GetQueryInfoRequest] =
      TapirSchema.derived
  }

  implicit val serialization: Serialization = org.json4s.native.Serialization
  implicit val formats: Formats = DefaultFormats

  implicit val schemaUnknownFieldSet: TapirSchema[UnknownFieldSet] = TapirSchema.derived
  implicit val schemaUnknownField: TapirSchema[UnknownFieldSet.Field] = TapirSchema.derived
  implicit val schemaByteString: TapirSchema[ByteString] = TapirSchema.schemaForByteString.map(schema => schema.map(ByteString.wrap(schema)))
  implicit val schemaListSharesResponse: TapirSchema[ListSharesResponse] = TapirSchema.derived
  implicit val schemaShare: TapirSchema[Share] = TapirSchema.derived
  implicit val schemaListSchemasResponse: TapirSchema[ListSchemasResponse] = TapirSchema.derived
  implicit val schemaListTablesResponse: TapirSchema[ListTablesResponse] = TapirSchema.derived
  // Base endpoint with common security
  private val baseEndpoint = endpoint
    .securityIn(auth.bearer[String]())
    .errorOut(jsonBody[ErrorResponse])

  // List Shares endpoint
  val listShares = baseEndpoint.get
    .in("shares")
    .in(query[Option[Int]]("maxResults").default(Some(500)))
    .in(query[Option[String]]("pageToken"))
    .out(jsonBody[ListSharesResponse])
    .description("List available shares")
    .tag("shares")

  val aShare = "shares" / path[String]("share")
  // Get Share endpoint
  val getShare = baseEndpoint.get
    .in(aShare)
    .out(jsonBody[Share])
    .description("Get share details")
    .tag("shares")

  // List Schemas endpoint
  val listSchemas = baseEndpoint.get
    .in(aShare / "schemas")
    .in(query[Option[Int]]("maxResults").default(Some(500)))
    .in(query[Option[String]]("pageToken"))
    .out(jsonBody[ListSchemasResponse])
    .description("List schemas in a share")
    .tag("schemas")

  val aSchema = aShare / "schemas" / path[String]("schema")
  // List Tables endpoint
  val listTables = baseEndpoint.get
    .in(aSchema / "tables")
    .in(query[Option[Int]]("maxResults").default(Some(500)))
    .in(query[Option[String]]("pageToken"))
    .out(jsonBody[ListTablesResponse])
    .description("List tables in a schema")
    .tag("tables")

  val aTable = aSchema / "tables" / path[String]("table")
  // Get Table Version endpoint
  val getTableVersion = baseEndpoint.get
    .in(aTable / "version")
    .in(query[Option[String]]("startingTimestamp"))
    .out(header[Long]("Delta-Table-Version"))
    .description("Get table version")
    .tag("tables")

  // Table metadata endpoint
  val getTableMetadata = baseEndpoint.get
    .in(aTable / "metadata")
    .in(query[Option[Long]]("version"))
    .in(query[Option[String]]("timestamp"))
    .out(header[Long]("Delta-Table-Version"))
    .out(jsonBody[String])
    .description("Get table metadata")
    .tag("tables")

  // Query table endpoint
  val queryTable = baseEndpoint.post
    .in(aTable / "query")
    .in(jsonBody[QueryTableRequest])
    .out(header[Long]("Delta-Table-Version"))
    .out(jsonBody[String])
    .description("Query table data")
    .tag("tables")

  // Query status endpoint
  val getQueryStatus = baseEndpoint.post
    .in(aTable / "queries" / path[String]("queryId"))
    .in(jsonBody[GetQueryInfoRequest])
    .out(header[Long]("Delta-Table-Version"))
    .out(jsonBody[String])
    .description("Get query status")
    .tag("tables")

  // CDF (Change Data Feed) endpoint
  val getTableChanges = baseEndpoint.get
    .in(aTable / "changes")
    .in(query[Option[String]]("startingVersion"))
    .in(query[Option[String]]("endingVersion"))
    .in(query[Option[String]]("startingTimestamp"))
    .in(query[Option[String]]("endingTimestamp"))
    .in(query[Option[Boolean]]("includeHistoricalMetadata"))
    .in(query[Option[Int]]("maxFiles"))
    .in(query[Option[String]]("pageToken"))
    .out(header[Long]("Delta-Table-Version"))
    .out(jsonBody[String])
    .description("Get table changes (CDF)")
    .tag("tables")

  // All endpoints combined
  val all = List(
    listShares,
    getShare,
    listSchemas,
    listTables,
    getTableVersion,
    getTableMetadata,
    queryTable,
    getQueryStatus,
    getTableChanges
  )
}

