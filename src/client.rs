//! Query side transport: [`IrohObjectStore`] implements the
//! `object_store::ObjectStore` trait over one iroh connection, with a wire
//! byte counter backing `--stats`.

use std::{
    fmt,
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{self, BoxStream, StreamExt, TryStreamExt};
use iroh::{
    Endpoint, EndpointAddr,
    endpoint::{ConnectError, Connection},
};
use object_store::{
    Attributes, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, path::Path as ObjPath,
};

use crate::proto::{self, Entry, ErrCode, MAX_DATA_LEN, Request, Response};

const ATTEMPTS: u32 = 3;
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);

/// Client-side failures.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// Stream/connection failure; retried up to 3 attempts per request.
    #[error("transport error: {0}")]
    Transport(String),
    #[error(transparent)]
    Proto(#[from] proto::ProtoError),
    #[error("remote error ({code}): {msg}")]
    Remote { code: ErrCode, msg: String },
    #[error("unexpected response variant for {0}")]
    UnexpectedResponse(&'static str),
}

impl ClientError {
    fn transport<E: fmt::Display>(err: E) -> Self {
        Self::Transport(err.to_string())
    }

    fn is_transport(&self) -> bool {
        matches!(self, Self::Transport(_))
    }

    /// Remote error code, if this is a remote error (drives CLI exit codes).
    pub fn remote_code(&self) -> Option<ErrCode> {
        match self {
            Self::Remote { code, .. } => Some(*code),
            _ => None,
        }
    }
}

/// `object_store::ObjectStore` over an iroh connection.
///
/// The connection is established once per session; each request opens one
/// bidirectional stream (streams are multiplexed by QUIC). Mutating methods
/// return `NotSupported` — the protocol has no mutating verbs by design.
#[derive(Debug, Clone)]
pub struct IrohObjectStore {
    conn: Connection,
    transferred: Arc<AtomicU64>,
}

impl IrohObjectStore {
    /// Dial `addr` with the popfusion ALPN.
    pub async fn connect(
        endpoint: &Endpoint,
        addr: impl Into<EndpointAddr>,
    ) -> Result<Self, ConnectError> {
        let conn = endpoint.connect(addr, proto::ALPN).await?;
        Ok(Self {
            conn,
            transferred: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Total wire bytes (request + response frames) transferred so far.
    pub fn bytes_transferred(&self) -> u64 {
        self.transferred.load(Ordering::Relaxed)
    }

    async fn request_once(&self, request: &Request) -> Result<Response, ClientError> {
        let frame = proto::encode_frame(request)?;
        let (mut send, mut recv) = self.conn.open_bi().await.map_err(ClientError::transport)?;
        send.write_all(&frame)
            .await
            .map_err(ClientError::transport)?;
        send.finish().map_err(ClientError::transport)?;
        let mut header = [0u8; 4];
        recv.read_exact(&mut header)
            .await
            .map_err(ClientError::transport)?;
        let len = proto::body_len(header)?;
        let mut body = vec![0u8; len];
        recv.read_exact(&mut body)
            .await
            .map_err(ClientError::transport)?;
        self.transferred
            .fetch_add((frame.len() + 4 + len) as u64, Ordering::Relaxed);
        match proto::decode_body::<Response>(&body)? {
            Response::Err { code, msg } => Err(ClientError::Remote { code, msg }),
            response => Ok(response),
        }
    }

    // AIDEV-NOTE: per the README error table only transport errors (stream reset
    // mid-read) are retried — 3 attempts with exponential backoff, per request.
    async fn request(&self, request: &Request) -> Result<Response, ClientError> {
        let mut backoff = INITIAL_BACKOFF;
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.request_once(request).await {
                Err(err) if err.is_transport() && attempt < ATTEMPTS => {
                    tracing::debug!(%err, attempt, "retrying after transport error");
                    tokio::time::sleep(backoff).await;
                    backoff *= 3;
                }
                other => return other,
            }
        }
    }

    /// Recursive remote listing (used by `ls` and table registration).
    pub async fn list_all(&self, prefix: &str) -> Result<Vec<Entry>, ClientError> {
        match self
            .request(&Request::List {
                prefix: prefix.to_string(),
            })
            .await?
        {
            Response::List { entries } => Ok(entries),
            _ => Err(ClientError::UnexpectedResponse("List")),
        }
    }

    /// Size and mtime (unix seconds) of one remote object.
    pub async fn stat(&self, path: &str) -> Result<(u64, i64), ClientError> {
        match self
            .request(&Request::Stat {
                path: path.to_string(),
            })
            .await?
        {
            Response::Stat {
                size,
                last_modified,
            } => Ok((size, last_modified)),
            _ => Err(ClientError::UnexpectedResponse("Stat")),
        }
    }

    /// One raw ranged read; `len` must be ≤ [`MAX_DATA_LEN`].
    pub async fn get_range_raw(
        &self,
        path: &str,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, ClientError> {
        debug_assert!(len <= MAX_DATA_LEN);
        match self
            .request(&Request::GetRange {
                path: path.to_string(),
                offset,
                len,
            })
            .await?
        {
            Response::Data { bytes } => Ok(bytes),
            _ => Err(ClientError::UnexpectedResponse("GetRange")),
        }
    }

    /// Lazy stream of ≤ 16 MiB chunks covering `range` (client-side chunking,
    /// since the server caps `GetRange` at [`MAX_DATA_LEN`] per request).
    fn chunk_stream(
        &self,
        path: String,
        range: Range<u64>,
    ) -> BoxStream<'static, object_store::Result<Bytes>> {
        let client = self.clone();
        let end = range.end;
        stream::try_unfold(range.start, move |offset| {
            let client = client.clone();
            let path = path.clone();
            async move {
                if offset >= end {
                    return Ok(None);
                }
                let len = (end - offset).min(MAX_DATA_LEN);
                let bytes = client
                    .get_range_raw(&path, offset, len)
                    .await
                    .map_err(|err| to_store_error(&path, err))?;
                Ok(Some((Bytes::from(bytes), offset + len)))
            }
        })
        .boxed()
    }

    async fn meta_of(&self, location: &ObjPath) -> object_store::Result<ObjectMeta> {
        let (size, last_modified) = self
            .stat(location.as_ref())
            .await
            .map_err(|err| to_store_error(location.as_ref(), err))?;
        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: timestamp(last_modified),
            size,
            e_tag: None,
            version: None,
        })
    }
}

impl fmt::Display for IrohObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "iroh://{}", self.conn.remote_id())
    }
}

fn timestamp(secs: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(secs, 0).unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}

// AIDEV-NOTE: the wire Entry deliberately carries no mtime (List stays tiny);
// epoch is fine for read-only listing consumers.
fn entry_meta(entry: Entry) -> ObjectMeta {
    ObjectMeta {
        location: ObjPath::from(entry.path),
        last_modified: DateTime::<Utc>::UNIX_EPOCH,
        size: entry.size,
        e_tag: None,
        version: None,
    }
}

fn to_store_error(path: &str, err: ClientError) -> object_store::Error {
    match &err {
        ClientError::Remote {
            code: ErrCode::NotFound,
            ..
        } => object_store::Error::NotFound {
            path: path.to_string(),
            source: Box::new(err),
        },
        _ => object_store::Error::Generic {
            store: "popfusion",
            source: Box::new(err),
        },
    }
}

fn read_only(operation: &str) -> object_store::Error {
    object_store::Error::NotSupported {
        source: format!("popfusion protocol is read-only: {operation} is not supported").into(),
    }
}

#[async_trait]
impl ObjectStore for IrohObjectStore {
    async fn put_opts(
        &self,
        _location: &ObjPath,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(read_only("put"))
    }

    async fn put_multipart_opts(
        &self,
        _location: &ObjPath,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(read_only("put_multipart"))
    }

    async fn get_opts(
        &self,
        location: &ObjPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        // AIDEV-NOTE: one Stat round-trip per get keeps ObjectMeta truthful for
        // every consumer; server-side footer/metadata caching is a v2 item.
        let meta = self.meta_of(location).await?;
        if options.head {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream::empty().boxed()),
                meta,
                range: 0..0,
                attributes: Attributes::default(),
            });
        }
        let size = meta.size;
        let range = match options.range {
            None => 0..size,
            Some(GetRange::Bounded(r)) => r.start..r.end.min(size),
            Some(GetRange::Offset(offset)) => offset..size,
            Some(GetRange::Suffix(n)) => size.saturating_sub(n)..size,
        };
        if range.start >= range.end {
            return Err(object_store::Error::Generic {
                store: "popfusion",
                source: format!("invalid range {range:?} for object of {size} bytes").into(),
            });
        }
        let payload = GetResultPayload::Stream(
            self.chunk_stream(location.as_ref().to_string(), range.clone()),
        );
        Ok(GetResult {
            payload,
            meta,
            range,
            attributes: Attributes::default(),
        })
    }

    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let client = self.clone();
        let prefix = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();
        stream::once(async move {
            let entries = client
                .list_all(&prefix)
                .await
                .map_err(|err| to_store_error(&prefix, err))?;
            Ok::<_, object_store::Error>(stream::iter(entries.into_iter().map(entry_meta).map(Ok)))
        })
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjPath>,
    ) -> object_store::Result<ListResult> {
        let base = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();
        let entries = self
            .list_all(&base)
            .await
            .map_err(|err| to_store_error(&base, err))?;
        let mut common_prefixes = std::collections::BTreeSet::new();
        let mut objects = Vec::new();
        let strip = if base.is_empty() {
            String::new()
        } else {
            format!("{base}/")
        };
        for entry in entries {
            let Some(rest) = entry.path.strip_prefix(&strip) else {
                continue;
            };
            match rest.split_once('/') {
                Some((first, _)) => {
                    let dir = format!("{strip}{first}");
                    common_prefixes.insert(ObjPath::from(dir));
                }
                None => objects.push(entry_meta(entry)),
            }
        }
        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjPath>> {
        locations
            .map(|result| result.and_then(|_| Err(read_only("delete"))))
            .boxed()
    }

    async fn copy_opts(
        &self,
        _from: &ObjPath,
        _to: &ObjPath,
        _options: CopyOptions,
    ) -> object_store::Result<()> {
        Err(read_only("copy"))
    }
}
