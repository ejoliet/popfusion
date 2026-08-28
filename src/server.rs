//! Serve side: iroh `ProtocolHandler` that maps wire requests to a backing
//! [`ObjectStore`], enforcing the peer allowlist and path safety.

use std::{collections::HashSet, path::PathBuf, sync::Arc};

use futures::TryStreamExt;
use iroh::{
    EndpointId,
    endpoint::{Connection, RecvStream, SendStream},
    protocol::{AcceptError, ProtocolHandler},
};
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjPath};
use tracing::{debug, info, warn};

use crate::proto::{self, Entry, ErrCode, MAX_DATA_LEN, Request, Response};

/// Access policy for inbound peers.
#[derive(Debug, Clone)]
pub enum Access {
    /// Only these peer public keys may connect (`--allow`).
    Allow(HashSet<EndpointId>),
    /// Anyone may connect (`--open`, a loud opt-out).
    Open,
}

impl Access {
    /// Whether `peer` is permitted to connect.
    pub fn permits(&self, peer: &EndpointId) -> bool {
        match self {
            Access::Allow(keys) => keys.contains(peer),
            Access::Open => true,
        }
    }
}

/// Unauthorized peer marker error (surfaces in the router's warning log).
#[derive(Debug, thiserror::Error)]
#[error("unauthorized peer {0}: not in --allow list")]
pub struct Unauthorized(pub EndpointId);

/// The popfusion protocol handler: a dumb, auditable, read-only byte proxy.
#[derive(Debug, Clone)]
pub struct PopfusionServer {
    store: Arc<dyn ObjectStore>,
    access: Access,
    /// Canonicalized filesystem root when serving a local directory.
    /// Used to reject symlink escapes; `None` for S3/in-memory backends.
    local_root: Option<PathBuf>,
}

impl PopfusionServer {
    /// `local_root`, when given, MUST already be canonicalized.
    pub fn new(store: Arc<dyn ObjectStore>, access: Access, local_root: Option<PathBuf>) -> Self {
        Self {
            store,
            access,
            local_root,
        }
    }

    /// Handle a single request/response exchange on one bidirectional stream.
    ///
    /// Never panics on malformed frames: every failure path either responds
    /// with `Response::Err` or silently drops the stream (peer went away).
    async fn handle_stream(&self, send: &mut SendStream, recv: &mut RecvStream) {
        let mut header = [0u8; 4];
        if recv.read_exact(&mut header).await.is_err() {
            // Peer closed the stream without a full header; nothing to answer.
            return;
        }
        let response = match proto::body_len(header) {
            Err(err) => Response::Err {
                code: ErrCode::Internal,
                msg: err.to_string(),
            },
            Ok(len) => {
                // AIDEV-NOTE: body_len already capped len at MAX_FRAME_LEN, so this
                // allocation is bounded no matter what the peer claims.
                let mut body = vec![0u8; len];
                if recv.read_exact(&mut body).await.is_err() {
                    return;
                }
                match proto::decode_body::<Request>(&body) {
                    Err(err) => Response::Err {
                        code: ErrCode::Internal,
                        msg: format!("malformed frame: {err}"),
                    },
                    Ok(request) => self.handle_request(request).await,
                }
            }
        };
        let frame = match proto::encode_frame(&response) {
            Ok(frame) => frame,
            Err(err) => {
                // e.g. a listing too large for one frame; degrade to an error reply.
                warn!(%err, "response did not fit in a frame");
                match proto::encode_frame(&Response::Err {
                    code: ErrCode::Internal,
                    msg: err.to_string(),
                }) {
                    Ok(frame) => frame,
                    Err(_) => return,
                }
            }
        };
        if let Err(err) = send.write_all(&frame).await {
            debug!(%err, "failed writing response");
            return;
        }
        if let Err(err) = send.finish() {
            debug!(%err, "failed finishing stream");
        }
    }

    /// Map one request to one response. Infallible by design: errors become
    /// `Response::Err`.
    pub async fn handle_request(&self, request: Request) -> Response {
        match request {
            Request::List { prefix } => self.handle_list(&prefix).await,
            Request::Stat { path } => self.handle_stat(&path).await,
            Request::GetRange { path, offset, len } => {
                self.handle_get_range(&path, offset, len).await
            }
        }
    }

    async fn handle_list(&self, prefix: &str) -> Response {
        let prefix = if prefix.is_empty() {
            None
        } else {
            match validate_path(prefix) {
                Ok(p) => Some(p),
                Err(resp) => return resp,
            }
        };
        let listing: Result<Vec<_>, _> = self.store.list(prefix.as_ref()).try_collect().await;
        match listing {
            Ok(metas) => Response::List {
                entries: metas
                    .into_iter()
                    .map(|m| Entry {
                        path: m.location.to_string(),
                        size: m.size,
                    })
                    .collect(),
            },
            Err(err) => store_error(&err),
        }
    }

    async fn handle_stat(&self, path: &str) -> Response {
        let obj_path = match validate_path(path) {
            Ok(p) => p,
            Err(resp) => return resp,
        };
        if let Err(resp) = self.check_local_containment(path).await {
            return resp;
        }
        match self.store.head(&obj_path).await {
            Ok(meta) => Response::Stat {
                size: meta.size,
                last_modified: meta.last_modified.timestamp(),
            },
            Err(err) => store_error(&err),
        }
    }

    async fn handle_get_range(&self, path: &str, offset: u64, len: u64) -> Response {
        let obj_path = match validate_path(path) {
            Ok(p) => p,
            Err(resp) => return resp,
        };
        if len == 0 || len > MAX_DATA_LEN {
            return Response::Err {
                code: ErrCode::RangeInvalid,
                msg: format!("len {len} outside 1..={MAX_DATA_LEN}"),
            };
        }
        let Some(end) = offset.checked_add(len) else {
            return Response::Err {
                code: ErrCode::RangeInvalid,
                msg: "offset+len overflows u64".into(),
            };
        };
        if let Err(resp) = self.check_local_containment(path).await {
            return resp;
        }
        match self.store.get_range(&obj_path, offset..end).await {
            // AIDEV-NOTE: object_store truncates ranges that run past EOF; the wire
            // contract promises "exactly len, or error", so a short read is RangeInvalid
            // (it indicates a client bug — the client knows object sizes from Stat/List).
            Ok(bytes) if bytes.len() as u64 == len => Response::Data {
                bytes: bytes.to_vec(),
            },
            Ok(bytes) => Response::Err {
                code: ErrCode::RangeInvalid,
                msg: format!(
                    "range {offset}..{end} beyond object (got {} bytes)",
                    bytes.len()
                ),
            },
            Err(err) => store_error(&err),
        }
    }

    /// Defense against symlink escapes when serving a local directory: the
    /// fully resolved path must stay inside the canonicalized served root.
    async fn check_local_containment(&self, rel: &str) -> Result<(), Response> {
        let Some(root) = &self.local_root else {
            return Ok(());
        };
        let full = root.join(rel);
        match tokio::fs::canonicalize(&full).await {
            Ok(canonical) if canonical.starts_with(root) => Ok(()),
            Ok(_) => Err(Response::Err {
                code: ErrCode::Forbidden,
                msg: "path escapes served root".into(),
            }),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Err(Response::Err {
                code: ErrCode::NotFound,
                msg: format!("{rel}: not found"),
            }),
            Err(err) => Err(Response::Err {
                code: ErrCode::Internal,
                msg: err.to_string(),
            }),
        }
    }
}

/// Validate and normalize a wire path.
///
/// AIDEV-NOTE: every inbound path is adversarial. We reject absolute paths,
/// backslashes (Windows separator smuggling), NULs, and any `.`/`..`/empty
/// segment *before* handing the path to the backing store, instead of trying
/// to normalize traversal away — reject-don't-repair.
pub fn validate_path(raw: &str) -> Result<ObjPath, Response> {
    let forbidden = |msg: &str| {
        Err(Response::Err {
            code: ErrCode::Forbidden,
            msg: format!("{raw:?}: {msg}"),
        })
    };
    if raw.is_empty() {
        return forbidden("empty path");
    }
    if raw.starts_with('/') {
        return forbidden("absolute paths are not allowed");
    }
    if raw.contains('\\') || raw.contains('\0') {
        return forbidden("path contains forbidden characters");
    }
    if raw
        .split('/')
        .any(|seg| seg.is_empty() || seg == "." || seg == "..")
    {
        return forbidden("path traversal segments are not allowed");
    }
    match ObjPath::parse(raw) {
        Ok(path) => Ok(path),
        Err(err) => forbidden(&err.to_string()),
    }
}

fn store_error(err: &object_store::Error) -> Response {
    match err {
        object_store::Error::NotFound { .. } => Response::Err {
            code: ErrCode::NotFound,
            msg: err.to_string(),
        },
        object_store::Error::InvalidPath { .. } => Response::Err {
            code: ErrCode::Forbidden,
            msg: err.to_string(),
        },
        _ => Response::Err {
            code: ErrCode::Internal,
            msg: err.to_string(),
        },
    }
}

impl ProtocolHandler for PopfusionServer {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let peer = connection.remote_id();
        if !self.access.permits(&peer) {
            // Rejected before any request frame is read.
            warn!(%peer, "rejecting unauthorized peer");
            connection.close(1u32.into(), b"forbidden");
            return Err(AcceptError::from_err(Unauthorized(peer)));
        }
        info!(%peer, "peer connected");
        loop {
            match connection.accept_bi().await {
                Ok((mut send, mut recv)) => {
                    let server = self.clone();
                    // One stream per request; streams are handled concurrently.
                    tokio::spawn(async move {
                        server.handle_stream(&mut send, &mut recv).await;
                    });
                }
                Err(_) => {
                    // Connection closed by the peer (or shutdown); routine.
                    debug!(%peer, "connection closed");
                    return Ok(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn erred(resp: &Response) -> Option<ErrCode> {
        match resp {
            Response::Err { code, .. } => Some(*code),
            _ => None,
        }
    }

    fn mem_server() -> PopfusionServer {
        PopfusionServer::new(Arc::new(InMemory::new()), Access::Open, None)
    }

    async fn seeded_server(path: &str, bytes: &[u8]) -> PopfusionServer {
        let store = InMemory::new();
        store
            .put(&ObjPath::from(path), bytes.to_vec().into())
            .await
            .expect("seed object");
        PopfusionServer::new(Arc::new(store), Access::Open, None)
    }

    #[test]
    fn allowlist_enforcement() {
        let allowed = iroh::SecretKey::generate().public();
        let stranger = iroh::SecretKey::generate().public();
        let access = Access::Allow([allowed].into_iter().collect());
        assert!(access.permits(&allowed));
        assert!(!access.permits(&stranger));
        assert!(Access::Open.permits(&stranger));
    }

    #[test]
    fn path_traversal_rejected() {
        for evil in [
            "../secret",
            "a/../../secret",
            "/etc/passwd",
            "a/./b",
            "a//b",
            "a\\b",
            "..",
            "",
            "a/..",
            "x\0y",
        ] {
            assert!(
                validate_path(evil).is_err(),
                "expected rejection for {evil:?}"
            );
        }
    }

    #[test]
    fn honest_paths_accepted() {
        for good in ["a.parquet", "dir/file.parquet", "deep/er/still.parquet"] {
            assert!(validate_path(good).is_ok(), "expected accept for {good:?}");
        }
    }

    #[tokio::test]
    async fn get_range_traversal_is_forbidden() {
        let server = mem_server();
        let resp = server
            .handle_request(Request::GetRange {
                path: "../x".into(),
                offset: 0,
                len: 1,
            })
            .await;
        assert_eq!(erred(&resp), Some(ErrCode::Forbidden));
    }

    #[tokio::test]
    async fn get_range_cap_enforced() {
        let server = seeded_server("t.bin", &[0u8; 64]).await;
        let resp = server
            .handle_request(Request::GetRange {
                path: "t.bin".into(),
                offset: 0,
                len: MAX_DATA_LEN + 1,
            })
            .await;
        assert_eq!(erred(&resp), Some(ErrCode::RangeInvalid));
        let resp = server
            .handle_request(Request::GetRange {
                path: "t.bin".into(),
                offset: u64::MAX,
                len: 2,
            })
            .await;
        assert_eq!(erred(&resp), Some(ErrCode::RangeInvalid));
    }

    #[tokio::test]
    async fn get_range_beyond_eof_is_range_invalid() {
        let server = seeded_server("t.bin", &[7u8; 10]).await;
        let resp = server
            .handle_request(Request::GetRange {
                path: "t.bin".into(),
                offset: 5,
                len: 100,
            })
            .await;
        assert_eq!(erred(&resp), Some(ErrCode::RangeInvalid));
    }

    #[tokio::test]
    async fn get_range_happy_path() {
        let data: Vec<u8> = (0u8..100).collect();
        let server = seeded_server("t.bin", &data).await;
        let resp = server
            .handle_request(Request::GetRange {
                path: "t.bin".into(),
                offset: 10,
                len: 5,
            })
            .await;
        assert_eq!(
            resp,
            Response::Data {
                bytes: vec![10, 11, 12, 13, 14]
            }
        );
    }

    #[tokio::test]
    async fn stat_and_list_and_not_found() {
        let server = seeded_server("dir/t.bin", &[1u8; 42]).await;
        match server
            .handle_request(Request::Stat {
                path: "dir/t.bin".into(),
            })
            .await
        {
            Response::Stat { size, .. } => assert_eq!(size, 42),
            other => panic!("unexpected: {other:?}"),
        }
        match server
            .handle_request(Request::List { prefix: "".into() })
            .await
        {
            Response::List { entries } => {
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].path, "dir/t.bin");
                assert_eq!(entries[0].size, 42);
            }
            other => panic!("unexpected: {other:?}"),
        }
        let resp = server
            .handle_request(Request::Stat {
                path: "missing.bin".into(),
            })
            .await;
        assert_eq!(erred(&resp), Some(ErrCode::NotFound));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlink_escape_rejected() {
        use object_store::local::LocalFileSystem;

        let outside = tempfile::tempdir().expect("outside dir");
        let secret_path = outside.path().join("secret.txt");
        std::fs::write(&secret_path, b"top secret").expect("write secret");

        let root = tempfile::tempdir().expect("root dir");
        std::os::unix::fs::symlink(&secret_path, root.path().join("sneaky.txt"))
            .expect("create symlink");

        let canonical_root = root.path().canonicalize().expect("canonicalize root");
        let store = LocalFileSystem::new_with_prefix(root.path()).expect("local store");
        let server = PopfusionServer::new(Arc::new(store), Access::Open, Some(canonical_root));

        let resp = server
            .handle_request(Request::Stat {
                path: "sneaky.txt".into(),
            })
            .await;
        assert_eq!(erred(&resp), Some(ErrCode::Forbidden));
        let resp = server
            .handle_request(Request::GetRange {
                path: "sneaky.txt".into(),
                offset: 0,
                len: 4,
            })
            .await;
        assert_eq!(erred(&resp), Some(ErrCode::Forbidden));
    }
}
