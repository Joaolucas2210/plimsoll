//! Arrow Flight SQL front door (ADR 0124).
//!
//! A gateway, not a second query path: every statement is posted to this
//! node's own `/v1/sql` router in process, with the caller's
//! `authorization` (and `traceparent`), and its Arrow result is re-framed
//! as Flight data. Authentication, grants, row filters, admission, the
//! deadline, cancellation (a dropped stream drops the body), the query log
//! and traces are the HTTP API's, unchanged.
//!
//! Handles are the SQL text itself: nothing is kept between calls, so any
//! coordinator behind a load balancer can serve any call.

use std::pin::Pin;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, CommandGetCatalogs, CommandGetDbSchemas,
    CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandPreparedStatementQuery,
    CommandPreparedStatementUpdate, CommandStatementQuery, CommandStatementUpdate,
    DoPutPreparedStatementResult, ProstMessageExt, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    Action, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse,
    IpcMessage, SchemaAsIpc, Ticket,
};
use axum::Router;
use axum::body::{Body, BodyDataStream};
use axum::http::{StatusCode, header};
use base64::Engine as _;
use futures::{Stream, StreamExt, TryStreamExt};
use plimsoll_core::datafusion::arrow::buffer::Buffer;
use plimsoll_core::datafusion::arrow::datatypes::{Schema, SchemaRef};
use plimsoll_core::datafusion::arrow::ipc::reader::{StreamDecoder, StreamReader};
use plimsoll_core::datafusion::arrow::ipc::writer::IpcWriteOptions;
use plimsoll_core::datafusion::arrow::record_batch::RecordBatch;
use prost::Message;
use serde_json::{Value, json};
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};
use tower::ServiceExt;

use crate::auth::Auth;
use crate::tls::TlsFiles;

type Batches = Pin<Box<dyn Stream<Item = Result<RecordBatch, FlightError>> + Send>>;
type DataStream = <Gateway as FlightService>::DoGetStream;
/// Per (catalog, schema, table): its columns' name, type and nullability.
type Columns = std::collections::HashMap<(String, String, String), Vec<(String, String, bool)>>;

/// The Flight SQL service: `/v1/sql` behind gRPC.
#[derive(Clone)]
pub struct Gateway {
    router: Router,
    auth: Auth,
}

impl Gateway {
    pub fn new(router: Router, auth: Auth) -> Self {
        Self { router, auth }
    }

    /// Posts `sql` to `/v1/sql` as the caller; a non-200 answer is its
    /// typed error as a gRPC status.
    async fn post(
        &self,
        md: &MetadataMap,
        sql: &str,
        params: &[Value],
        format: &str,
    ) -> Result<Body, Status> {
        let mut body = json!({ "sql": sql, "format": format });
        if !params.is_empty() {
            body["params"] = json!(params);
        }
        // Development only (`PLIMSOLL_AUTH=disabled`): the user as sent.
        for (key, field) in [
            ("x-plimsoll-user", "user"),
            ("x-plimsoll-resource-group", "resource_group"),
        ] {
            if let Some(v) = md.get(key).and_then(|v| v.to_str().ok()) {
                body[field] = json!(v);
            }
        }
        let mut req =
            axum::http::Request::post("/v1/sql").header(header::CONTENT_TYPE, "application/json");
        for key in ["authorization", "traceparent"] {
            if let Some(v) = md.get(key).and_then(|v| v.to_str().ok()) {
                req = req.header(key, v);
            }
        }
        let req = req
            .body(Body::from(body.to_string()))
            .map_err(|e| Status::internal(e.to_string()))?;
        let res = self
            .router
            .clone()
            .oneshot(req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let status = res.status();
        if status == StatusCode::OK {
            return Ok(res.into_body());
        }
        let bytes = axum::body::to_bytes(res.into_body(), 64 << 10)
            .await
            .unwrap_or_default();
        let message = serde_json::from_slice::<Value>(&bytes)
            .ok()
            .and_then(|v| v["error"]["message"].as_str().map(str::to_owned))
            .unwrap_or_else(|| status.to_string());
        Err(status_of(status, message))
    }

    /// The statement's result as Arrow batches, with its schema even when
    /// it has no rows.
    async fn query(
        &self,
        md: &MetadataMap,
        sql: &str,
        params: &[Value],
    ) -> Result<(SchemaRef, Batches), Status> {
        let mut ipc = Ipc {
            body: self
                .post(md, sql, params, "arrow")
                .await?
                .into_data_stream(),
            decoder: StreamDecoder::new(),
            buf: Buffer::from_vec(Vec::<u8>::new()),
        };
        let first = ipc.next().await.transpose().map_err(Status::from)?;
        let schema = first
            .as_ref()
            .map(RecordBatch::schema)
            .or_else(|| ipc.decoder.schema())
            .ok_or_else(|| Status::internal("result without a schema"))?;
        let rest =
            futures::stream::unfold(
                ipc,
                |mut ipc| async move { ipc.next().await.map(|b| (b, ipc)) },
            );
        let batches = futures::stream::iter(first.map(Ok)).chain(rest);
        Ok((schema, Box::pin(batches)))
    }

    /// The statement's result schema, planned but not run: the statement
    /// under `LIMIT 0`, through the same checks as running it. One that is
    /// not a query (DDL, DML, `SHOW`) has no result schema to announce.
    ///
    /// ponytail: plans twice per statement (here and on `DoGet`); cache by
    /// SQL and snapshot if planning shows up in latencies.
    async fn schema(
        &self,
        md: &MetadataMap,
        sql: &str,
        params: &[Value],
    ) -> Result<SchemaRef, Status> {
        let probe = format!(
            "SELECT * FROM ({}) LIMIT 0",
            sql.trim().trim_end_matches(';')
        );
        // Unbound placeholders do not plan: no schema until bound.
        let body = match self.post(md, &probe, params, "arrow").await {
            Ok(body) => body,
            Err(s) if s.code() == tonic::Code::InvalidArgument => {
                return Ok(Schema::empty().into());
            }
            Err(s) => return Err(s),
        };
        let bytes = axum::body::to_bytes(body, 16 << 20)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        StreamReader::try_new(std::io::Cursor::new(bytes), None)
            .map(|r| r.schema())
            .map_err(|e| Status::internal(e.to_string()))
    }

    /// The rows of a statement run as JSON (catalog listings, updates).
    async fn rows(
        &self,
        md: &MetadataMap,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<Vec<Value>>, Status> {
        let bytes = axum::body::to_bytes(self.post(md, sql, params, "json").await?, 64 << 20)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let v: Value =
            serde_json::from_slice(&bytes).map_err(|e| Status::internal(e.to_string()))?;
        Ok(serde_json::from_value(v["rows"].clone()).unwrap_or_default())
    }

    /// Rows a DML statement changed; 0 for DDL.
    async fn update(&self, md: &MetadataMap, sql: &str, params: &[Value]) -> Result<i64, Status> {
        let rows = self.rows(md, sql, params).await?;
        Ok(rows
            .first()
            .and_then(|r| r.first())
            .and_then(Value::as_i64)
            .unwrap_or(0))
    }

    async fn stream(
        &self,
        md: &MetadataMap,
        sql: &str,
        params: &[Value],
    ) -> Result<Response<DataStream>, Status> {
        let (schema, batches) = self.query(md, sql, params).await?;
        Ok(Response::new(encode(schema, batches)))
    }

    /// `information_schema.columns` as the caller sees it, per table, in
    /// column order.
    async fn columns(&self, md: &MetadataMap) -> Result<Columns, Status> {
        let rows = self
            .rows(
                md,
                "SELECT table_catalog, table_schema, table_name, column_name, data_type, \
                 is_nullable FROM information_schema.columns \
                 ORDER BY table_catalog, table_schema, table_name, ordinal_position",
                &[],
            )
            .await?;
        let mut out = Columns::new();
        for r in rows {
            let text = |i: usize| r.get(i).and_then(Value::as_str).unwrap_or("").to_owned();
            out.entry((text(0), text(1), text(2))).or_default().push((
                text(3),
                text(4),
                text(5) == "YES",
            ));
        }
        Ok(out)
    }

    /// `information_schema.tables` as the caller sees it.
    async fn tables(&self, md: &MetadataMap) -> Result<Vec<[String; 4]>, Status> {
        let rows = self
            .rows(
                md,
                "SELECT table_catalog, table_schema, table_name, table_type \
                 FROM information_schema.tables",
                &[],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                std::array::from_fn(|i| r.get(i).and_then(Value::as_str).unwrap_or("").to_owned())
            })
            .collect())
    }
}

/// The HTTP API's error classes as gRPC codes.
fn status_of(status: StatusCode, message: String) -> Status {
    match status {
        StatusCode::UNAUTHORIZED => Status::unauthenticated(message),
        StatusCode::FORBIDDEN => Status::permission_denied(message),
        StatusCode::BAD_REQUEST => Status::invalid_argument(message),
        StatusCode::NOT_FOUND => Status::not_found(message),
        StatusCode::CONFLICT => Status::cancelled(message),
        StatusCode::UNPROCESSABLE_ENTITY | StatusCode::TOO_MANY_REQUESTS => {
            Status::resource_exhausted(message)
        }
        StatusCode::SERVICE_UNAVAILABLE => Status::unavailable(message),
        StatusCode::GATEWAY_TIMEOUT => Status::deadline_exceeded(message),
        StatusCode::NOT_IMPLEMENTED => Status::unimplemented(message),
        _ => Status::internal(message),
    }
}

/// An Arrow IPC body, decoded as it arrives.
struct Ipc {
    body: BodyDataStream,
    decoder: StreamDecoder,
    buf: Buffer,
}

impl Ipc {
    async fn next(&mut self) -> Option<Result<RecordBatch, FlightError>> {
        loop {
            while !self.buf.is_empty() {
                match self.decoder.decode(&mut self.buf) {
                    Ok(Some(batch)) => return Some(Ok(batch)),
                    Ok(None) => {}
                    Err(e) => return Some(Err(e.into())),
                }
            }
            match self.body.next().await {
                Some(Ok(bytes)) => self.buf = Buffer::from(bytes),
                // The HTTP API aborts the body when the query fails after
                // its first batch (ADR 0037); the reason is in its log.
                Some(Err(e)) => {
                    return Some(Err(FlightError::Tonic(Box::new(Status::aborted(format!(
                        "the query failed mid-stream: {e}"
                    ))))));
                }
                None => return self.decoder.finish().err().map(|e| Err(e.into())),
            }
        }
    }
}

fn encode(schema: SchemaRef, batches: Batches) -> DataStream {
    Box::pin(
        FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batches)
            .map_err(Status::from),
    )
}

fn one(batch: Result<RecordBatch, impl std::fmt::Display>) -> Result<Response<DataStream>, Status> {
    let batch = batch.map_err(|e| Status::internal(e.to_string()))?;
    let schema = batch.schema();
    Ok(Response::new(encode(
        schema,
        Box::pin(futures::stream::once(async { Ok(batch) })),
    )))
}

fn sql_text(handle: &[u8]) -> Result<String, Status> {
    String::from_utf8(handle.to_vec()).map_err(|_| Status::invalid_argument("handle is not SQL"))
}

/// A prepared statement's handle: its SQL and, once bound, its
/// parameters. Clients hand it back on every call (stateless).
#[derive(serde::Serialize, serde::Deserialize)]
struct Prepared {
    sql: String,
    #[serde(default)]
    params: Vec<Value>,
    /// A batch of several parameter rows (JDBC's `executeBatch`): an
    /// update runs once per row (ADR 0129).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    rows: Vec<Vec<Value>>,
}

impl Prepared {
    fn decode(handle: &[u8]) -> Result<Self, Status> {
        serde_json::from_slice(handle)
            .map_err(|_| Status::invalid_argument("unknown statement handle"))
    }

    fn encode(&self) -> Result<Vec<u8>, Status> {
        serde_json::to_vec(self).map_err(|e| Status::internal(e.to_string()))
    }
}

/// JDBC's and ODBC's `?` placeholders as `$1`, `$2`, ... in order, outside
/// string literals, quoted identifiers and comments. DataFusion reads
/// every bare `?` as the same parameter.
fn numbered(sql: &str) -> String {
    let (mut out, mut n) = (String::with_capacity(sql.len()), 0);
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' | '"' => {
                out.push(c);
                // A doubled quote is an escaped one: leaving and re-entering
                // the literal copies it unchanged.
                for d in chars.by_ref() {
                    out.push(d);
                    if d == c {
                        break;
                    }
                }
            }
            '-' if chars.peek() == Some(&'-') => {
                out.push(c);
                for d in chars.by_ref() {
                    out.push(d);
                    if d == '\n' {
                        break;
                    }
                }
            }
            '/' if chars.peek() == Some(&'*') => {
                out.push(c);
                let mut last = ' ';
                for d in chars.by_ref() {
                    out.push(d);
                    if last == '*' && d == '/' {
                        break;
                    }
                    last = d;
                }
            }
            '?' => {
                n += 1;
                out.push_str(&format!("${n}"));
            }
            _ => out.push(c),
        }
    }
    out
}

/// The rows of a bound parameter batch, as `/v1/sql` takes them (ADR
/// 0043): JSON scalars, cast there to what each placeholder needs.
fn bound(batches: &[RecordBatch]) -> Result<Vec<Vec<Value>>, Status> {
    use plimsoll_core::datafusion::common::ScalarValue;
    let mut rows = Vec::new();
    for batch in batches {
        for i in 0..batch.num_rows() {
            let row = batch
                .columns()
                .iter()
                .map(|col| {
                    let v = ScalarValue::try_from_array(col, i)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    Ok(match &v {
                        v if v.is_null() => Value::Null,
                        ScalarValue::Boolean(Some(b)) => Value::Bool(*b),
                        v if v.data_type().is_numeric() => serde_json::from_str(&v.to_string())
                            .unwrap_or_else(|_| json!(v.to_string())),
                        v => json!(v.to_string()),
                    })
                })
                .collect::<Result<Vec<_>, Status>>()?;
            rows.push(row);
        }
    }
    Ok(rows)
}

/// Columns as `information_schema.columns` lists them, as Arrow fields;
/// `None` when a type does not read back (the table is then planned).
fn fields(columns: &[(String, String, bool)]) -> Option<Schema> {
    use plimsoll_core::datafusion::arrow::datatypes::{DataType, Field};
    columns
        .iter()
        .map(|(name, ty, nullable)| {
            ty.parse::<DataType>()
                .ok()
                .map(|t| Field::new(name, t, *nullable))
        })
        .collect::<Option<Vec<_>>>()
        .map(Schema::new)
}

/// A `FlightInfo` whose one endpoint's ticket is `command`.
fn info(
    schema: &Schema,
    command: impl ProstMessageExt,
    descriptor: FlightDescriptor,
) -> Result<Response<FlightInfo>, Status> {
    let ticket = Ticket::new(command.as_any().encode_to_vec());
    Ok(Response::new(
        FlightInfo::new()
            .try_with_schema(schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_endpoint(FlightEndpoint::new().with_ticket(ticket))
            .with_descriptor(descriptor),
    ))
}

/// `Basic base64(user:token)`, padded or not (grpc-go sends it without).
fn basic(header: &str) -> Option<(String, String)> {
    use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
    const LENIENT: GeneralPurpose = GeneralPurpose::new(
        &base64::alphabet::STANDARD,
        GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
    );
    let decoded = LENIENT.decode(header.strip_prefix("Basic ")?.trim()).ok()?;
    let (user, token) = String::from_utf8(decoded)
        .ok()?
        .split_once(':')
        .map(|(u, t)| (u.to_owned(), t.to_owned()))?;
    Some((user, token))
}

fn sql_info() -> Result<SqlInfoData, Status> {
    let mut b = SqlInfoDataBuilder::new();
    b.append(SqlInfo::FlightSqlServerName, "Plimsoll");
    b.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
    b.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
    b.append(SqlInfo::FlightSqlServerReadOnly, false);
    b.build().map_err(|e| Status::internal(e.to_string()))
}

#[tonic::async_trait]
impl FlightSqlService for Gateway {
    type FlightService = Gateway;

    /// Basic `user:token` in, `Bearer token` out (JDBC's username and
    /// password). The token must be the user's.
    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        let (user, token) = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(basic)
            .ok_or_else(|| Status::unauthenticated("expected Basic user:token"))?;
        let (user, token) = (user.as_str(), token.as_str());
        let bearer = format!("Bearer {token}");
        match self.auth.principal(Some(&bearer)) {
            Ok(Some(p)) if p == user => {}
            Ok(None) => {} // auth disabled
            _ => return Err(Status::unauthenticated("invalid credentials")),
        }
        let value = MetadataValue::try_from(bearer.as_str())
            .map_err(|_| Status::unauthenticated("invalid credentials"))?;
        let out = HandshakeResponse {
            protocol_version: 0,
            payload: token.as_bytes().to_vec().into(),
        };
        let mut response: Response<Pin<Box<dyn Stream<Item = _> + Send>>> =
            Response::new(Box::pin(futures::stream::once(async { Ok(out) })));
        response.metadata_mut().insert("authorization", value);
        Ok(response)
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = self.schema(request.metadata(), &query.query, &[]).await?;
        let ticket = TicketStatementQuery {
            statement_handle: query.query.into_bytes().into(),
        };
        info(&schema, ticket, request.into_inner())
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<DataStream>, Status> {
        let sql = sql_text(&ticket.statement_handle)?;
        self.stream(request.metadata(), &sql, &[]).await
    }

    async fn do_put_statement_update(
        &self,
        command: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        self.update(request.metadata(), &command.query, &[]).await
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let prepared = Prepared {
            sql: numbered(&query.query),
            params: Vec::new(),
            rows: Vec::new(),
        };
        let schema = self.schema(request.metadata(), &prepared.sql, &[]).await?;
        let IpcMessage(dataset_schema) = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e: plimsoll_core::datafusion::arrow::error::ArrowError| {
                Status::internal(e.to_string())
            })?;
        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: prepared.encode()?.into(),
            dataset_schema,
            parameter_schema: Default::default(),
        })
    }

    /// Binds a parameter row: the new handle carries it.
    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        let mut prepared = Prepared::decode(&query.prepared_statement_handle)?;
        let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(
            request.into_inner().map_err(FlightError::from),
        )
        .try_collect()
        .await
        .map_err(Status::from)?;
        // One row binds the statement; several are a batch, for updates.
        let mut rows = bound(&batches)?;
        if rows.len() == 1 {
            prepared.params = rows.pop().unwrap_or_default();
        } else {
            prepared.params = Vec::new();
            prepared.rows = rows;
        }
        Ok(DoPutPreparedStatementResult {
            prepared_statement_handle: Some(prepared.encode()?.into()),
        })
    }

    /// Nothing to free: the handle is the statement.
    async fn do_action_close_prepared_statement(
        &self,
        _query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Ok(())
    }

    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let p = Prepared::decode(&query.prepared_statement_handle)?;
        if !p.rows.is_empty() {
            return Err(Status::invalid_argument(
                "a query takes one parameter row; batches are for updates",
            ));
        }
        let schema = self.schema(request.metadata(), &p.sql, &p.params).await?;
        info(&schema, query, request.into_inner())
    }

    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<DataStream>, Status> {
        let p = Prepared::decode(&query.prepared_statement_handle)?;
        self.stream(request.metadata(), &p.sql, &p.params).await
    }

    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let p = Prepared::decode(&query.prepared_statement_handle)?;
        if p.rows.is_empty() {
            return self.update(request.metadata(), &p.sql, &p.params).await;
        }
        // ponytail: one statement per row, in order, each its own commit
        // (no transactions); a `VALUES` join would make it one.
        let mut total = 0;
        for row in &p.rows {
            total += self.update(request.metadata(), &p.sql, row).await?;
        }
        Ok(total)
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let data = sql_info()?;
        let schema = query.clone().into_builder(&data).schema();
        info(&schema, query, request.into_inner())
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<DataStream>, Status> {
        one(query.into_builder(&sql_info()?).build())
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.into_builder().schema();
        info(&schema, query, request.into_inner())
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<DataStream>, Status> {
        let mut b = query.into_builder();
        let mut seen = std::collections::BTreeSet::new();
        for [catalog, ..] in self.tables(request.metadata()).await? {
            if seen.insert(catalog.clone()) {
                b.append(catalog);
            }
        }
        one(b.build())
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.clone().into_builder().schema();
        info(&schema, query, request.into_inner())
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<DataStream>, Status> {
        let mut b = query.into_builder();
        let mut seen = std::collections::BTreeSet::new();
        for [catalog, schema, ..] in self.tables(request.metadata()).await? {
            if seen.insert((catalog.clone(), schema.clone())) {
                b.append(catalog, schema);
            }
        }
        one(b.build())
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.clone().into_builder().schema();
        info(&schema, query, request.into_inner())
    }

    /// With `include_schema`, each table's columns come from one
    /// `information_schema.columns` read, as the caller sees them (ADR
    /// 0129).
    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<DataStream>, Status> {
        let md = request.metadata();
        let columns = if query.include_schema {
            self.columns(md).await?
        } else {
            Default::default()
        };
        let mut b = query.into_builder();
        let empty = Schema::empty();
        for [catalog, schema, table, kind] in self.tables(md).await? {
            let key = (catalog.clone(), schema.clone(), table.clone());
            let planned;
            let fields = match columns.get(&key).map(|c| fields(c)) {
                Some(Some(f)) => {
                    planned = f;
                    &planned
                }
                // A type that does not read back: plan the table instead.
                Some(None) => {
                    let sql = format!("SELECT * FROM \"{catalog}\".\"{schema}\".\"{table}\"");
                    planned = self.schema(md, &sql, &[]).await?.as_ref().clone();
                    &planned
                }
                None => &empty,
            };
            b.append(catalog, schema, table, kind, fields)
                .map_err(Status::from)?;
        }
        one(b.build())
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.into_builder().schema();
        info(&schema, query, request.into_inner())
    }

    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<DataStream>, Status> {
        let mut b = query.into_builder();
        for kind in ["BASE TABLE", "VIEW"] {
            b.append(kind);
        }
        one(b.build())
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// Serves Flight SQL on `listener` until `shutdown` resolves; TLS with
/// the node's certificate when it has one (read at boot).
pub async fn serve(
    gateway: Gateway,
    listener: tokio::net::TcpListener,
    tls: Option<&TlsFiles>,
    shutdown: impl std::future::Future<Output = ()> + Send,
) -> Result<(), String> {
    let mut server = Server::builder();
    if let Some(files) = tls {
        let read =
            |p: &std::path::Path| std::fs::read(p).map_err(|e| format!("{}: {e}", p.display()));
        let identity = Identity::from_pem(read(&files.cert)?, read(&files.key)?);
        server = server
            .tls_config(ServerTlsConfig::new().identity(identity))
            .map_err(|e| e.to_string())?;
    }
    server
        .add_service(FlightServiceServer::new(gateway))
        .serve_with_incoming_shutdown(
            tonic::transport::server::TcpIncoming::from(listener),
            shutdown,
        )
        .await
        .map_err(|e| e.to_string())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};

    use arrow_flight::sql::CommandGetTables;
    use arrow_flight::sql::client::FlightSqlServiceClient;
    use plimsoll_control::governance::{Admission, AdmissionConfig, Limits, QueryRegistry};
    use plimsoll_core::datafusion::arrow::array::{Array, Int64Array, StringArray};
    use plimsoll_core::engine::Engine;
    use plimsoll_core::memory::CeilingPool;
    use tonic::transport::Channel;

    use super::*;
    use crate::app::{AppState, router};
    use crate::metrics::PoolStats;

    /// A coordinator with the real engine, one table, and `ana`'s token
    /// `t1`, serving Flight SQL on an ephemeral port.
    async fn serve_one() -> FlightSqlServiceClient<Channel> {
        let pool = Arc::new(CeilingPool::new(64 << 20, 8 << 20).unwrap());
        let engine = Arc::new(Engine::new(pool, None).unwrap());
        engine
            .context()
            .sql("CREATE TABLE trips AS VALUES (1, 'lisbon'), (2, 'porto')")
            .await
            .unwrap();
        let hex: String = crate::auth::token_digest("t1")
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let auth = Auth::from_lookup(
            &|k| (k == "PLIMSOLL_AUTH_TOKENS").then(|| format!("ana:sha256:{hex}")),
            &|g| g == "default",
        )
        .unwrap();
        let limits = Limits {
            max_running: 4,
            max_queued: 4,
        };
        let cfg = AdmissionConfig {
            global: limits,
            per_group: limits,
            per_user: limits,
            groups: Default::default(),
        };
        let st = Arc::new(AppState {
            engine: Arc::new(crate::engine::CoreEngine(engine)),
            admission: Admission::new(cfg, Arc::new(QueryRegistry::new(100))),
            pool_stats: Arc::new(PoolStats::default),
            query_timeout: Duration::from_secs(10),
            ready: AtomicBool::new(true),
            serving: AtomicBool::new(true),
            started: Instant::now(),
            errors: Default::default(),
            fragments: Default::default(),
            latency: Default::default(),
            auth: auth.clone(),
            tracer: None,
            peers: None,
            coordination: None,
            slow_query: None,
            worker_token: None,
            fragment_body_max: 1 << 20,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let gateway = Gateway::new(router(st), auth);
        tokio::spawn(serve(gateway, listener, None, std::future::pending()));
        let channel = Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        FlightSqlServiceClient::new(channel)
    }

    async fn fetch(
        client: &mut FlightSqlServiceClient<Channel>,
        info: FlightInfo,
    ) -> Result<Vec<RecordBatch>, FlightError> {
        let ticket = info.endpoint[0].ticket.clone().unwrap();
        client.do_get(ticket).await?.try_collect().await
    }

    fn code(e: FlightError) -> tonic::Code {
        match e {
            FlightError::Tonic(s) => s.code(),
            other => panic!("not a status: {other}"),
        }
    }

    #[tokio::test]
    async fn statements_run_as_the_handshaken_user() {
        let mut client = serve_one().await;
        // Anonymous, then a wrong token: refused, as on `/v1/sql`.
        let err = client.execute("SELECT 1".into(), None).await.unwrap_err();
        assert_eq!(code(err), tonic::Code::Unauthenticated);
        assert!(client.handshake("ana", "nope").await.is_err());
        assert!(client.handshake("bob", "t1").await.is_err());
        client.handshake("ana", "t1").await.unwrap();

        // The schema is announced before anything runs.
        let sql = "SELECT column1 AS id, column2 AS city FROM trips ORDER BY id";
        let info = client.execute(sql.into(), None).await.unwrap();
        let schema = info.clone().try_decode_schema().unwrap();
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "city");
        let batches = fetch(&mut client, info).await.unwrap();
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 2);
        let city = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(city.value(1), "porto");

        // No rows: still the schema.
        let info = client
            .execute("SELECT column1 AS id FROM trips WHERE false".into(), None)
            .await
            .unwrap();
        assert_eq!(
            info.clone().try_decode_schema().unwrap().field(0).name(),
            "id"
        );
        assert!(
            fetch(&mut client, info)
                .await
                .unwrap()
                .iter()
                .all(|b| b.num_rows() == 0)
        );

        // The API's error classes come through as gRPC codes.
        let info = client.execute("SELEC nope".into(), None).await.unwrap();
        let err = fetch(&mut client, info).await.unwrap_err();
        assert_eq!(code(err), tonic::Code::InvalidArgument);

        // Updates report the count the statement returned.
        assert_eq!(
            client
                .execute_update("SELECT 7".into(), None)
                .await
                .unwrap(),
            7
        );
    }

    #[test]
    fn basic_credentials_with_or_without_padding() {
        // "ana:" + 64 hex chars needs padding; grpc-go leaves it out.
        let token = "ee07b204ff0db13ee312a85922f1d1ee8dcfa26b827ca7f20c936aac92635ba9";
        let padded = base64::engine::general_purpose::STANDARD.encode(format!("ana:{token}"));
        assert!(padded.ends_with('='));
        for header in [
            format!("Basic {padded}"),
            format!("Basic {}", padded.trim_end_matches('=')),
        ] {
            assert_eq!(basic(&header), Some(("ana".into(), token.into())));
        }
        assert_eq!(basic("Bearer abc"), None);
        assert_eq!(basic("Basic !!!"), None);
        assert_eq!(basic("Basic YW5h"), None); // "ana", no colon
    }

    #[test]
    fn question_marks_are_numbered_outside_literals_and_comments() {
        assert_eq!(
            numbered("SELECT * FROM t WHERE a = ? AND b IN (?, ?)"),
            "SELECT * FROM t WHERE a = $1 AND b IN ($2, $3)"
        );
        assert_eq!(
            numbered("SELECT '?', 'it''s ?', \"col?\" FROM t -- why?\nWHERE x = ? /* or ? */"),
            "SELECT '?', 'it''s ?', \"col?\" FROM t -- why?\nWHERE x = $1 /* or ? */"
        );
        assert_eq!(numbered("SELECT $1"), "SELECT $1");
    }

    #[tokio::test]
    async fn prepared_statements_bind_parameters() {
        use plimsoll_core::datafusion::arrow::array::{ArrayRef, Int64Array as I64};
        let mut client = serve_one().await;
        client.handshake("ana", "t1").await.unwrap();
        let mut prepared = client
            .prepare(
                "SELECT column2 AS city FROM trips WHERE column1 = ? AND column2 <> ?".into(),
                None,
            )
            .await
            .unwrap();
        let params = RecordBatch::try_from_iter([
            ("0", Arc::new(I64::from(vec![1])) as ArrayRef),
            (
                "1",
                Arc::new(StringArray::from(vec!["nowhere"])) as ArrayRef,
            ),
        ])
        .unwrap();
        prepared.set_parameters(params).unwrap();
        let info = prepared.execute().await.unwrap();
        // Bound, the statement plans: its schema comes with the info.
        assert_eq!(
            info.clone().try_decode_schema().unwrap().field(0).name(),
            "city"
        );
        let batches = fetch(&mut client, info).await.unwrap();
        let city = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!((city.len(), city.value(0)), (1, "lisbon"));

        // Updates take them too; a mismatched count is the API's error.
        let mut update = client.prepare("SELECT ? + 1".into(), None).await.unwrap();
        let six =
            RecordBatch::try_from_iter([("0", Arc::new(I64::from(vec![6])) as ArrayRef)]).unwrap();
        update.set_parameters(six).unwrap();
        assert_eq!(update.execute_update().await.unwrap(), 7);
        let mut two = client.prepare("SELECT ? + ?".into(), None).await.unwrap();
        let one =
            RecordBatch::try_from_iter([("0", Arc::new(I64::from(vec![1])) as ArrayRef)]).unwrap();
        two.set_parameters(one).unwrap();
        // A batch of rows is JDBC's executeBatch: one run per row, counts
        // summed; a query takes one row only.
        let mut batch = client.prepare("SELECT ? + 1".into(), None).await.unwrap();
        let rows =
            RecordBatch::try_from_iter([("0", Arc::new(I64::from(vec![1, 2, 3])) as ArrayRef)])
                .unwrap();
        batch.set_parameters(rows.clone()).unwrap();
        assert_eq!(batch.execute_update().await.unwrap(), 2 + 3 + 4);
        batch.set_parameters(rows).unwrap();
        assert_eq!(
            code(batch.execute().await.unwrap_err()),
            tonic::Code::InvalidArgument
        );
        let info = two.execute().await.unwrap();
        assert_eq!(
            code(fetch(&mut client, info).await.unwrap_err()),
            tonic::Code::InvalidArgument
        );
    }

    #[tokio::test]
    async fn prepared_statements_and_metadata() {
        let mut client = serve_one().await;
        client.handshake("ana", "t1").await.unwrap();

        let mut prepared = client
            .prepare("SELECT count(*) AS n FROM trips".into(), None)
            .await
            .unwrap();
        assert_eq!(prepared.dataset_schema().unwrap().field(0).name(), "n");
        let info = prepared.execute().await.unwrap();
        let batches = fetch(&mut client, info).await.unwrap();
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(n.value(0), 2);
        prepared.close().await.unwrap();

        let info = client
            .get_tables(CommandGetTables {
                table_name_filter_pattern: Some("trips".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        let batches = fetch(&mut client, info).await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let tables = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(tables.value(0), "trips");

        // With their columns, as `information_schema` types them.
        let info = client
            .get_tables(CommandGetTables {
                table_name_filter_pattern: Some("trips".into()),
                include_schema: true,
                ..Default::default()
            })
            .await
            .unwrap();
        let batches = fetch(&mut client, info).await.unwrap();
        let ipc = batches[0]
            .column_by_name("table_schema")
            .unwrap()
            .as_any()
            .downcast_ref::<plimsoll_core::datafusion::arrow::array::BinaryArray>()
            .unwrap()
            .value(0)
            .to_vec();
        let schema = Schema::try_from(IpcMessage(ipc.into())).unwrap();
        let described: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| format!("{}:{}", f.name(), f.data_type()))
            .collect();
        assert_eq!(described, ["column1:Int64", "column2:Utf8"]);

        let info = client.get_catalogs().await.unwrap();
        let catalogs = fetch(&mut client, info).await.unwrap();
        assert!(catalogs[0].num_rows() >= 1);

        let info = client
            .get_sql_info(vec![SqlInfo::FlightSqlServerName])
            .await
            .unwrap();
        assert_eq!(fetch(&mut client, info).await.unwrap()[0].num_rows(), 1);
    }
}
