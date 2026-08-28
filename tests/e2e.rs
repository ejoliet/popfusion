//! In-process serve+query over loopback iroh.
//!
//! The bytes-transferred assertion is the point of the tool: a selective
//! predicate must move < 10% of the remote file. Acceptance test, not a
//! nice-to-have.

#![deny(unsafe_code)]

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::Path,
    sync::Arc,
};

use datafusion::arrow::{
    array::{Array, Float64Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use iroh::{Endpoint, EndpointAddr, SecretKey, endpoint::presets, protocol::Router};
use object_store::{
    ObjectStore, ObjectStoreExt, local::LocalFileSystem, memory::InMemory, path::Path as ObjPath,
};
use popfusion::{
    client::IrohObjectStore,
    proto, query,
    server::{Access, PopfusionServer},
};

const ROWS: usize = 200_000;
const ROW_GROUP: usize = 10_000;

/// Deterministic catalog sorted by mag so row-group stats prune tightly:
/// mag = 10.0 + 10.0 * i / ROWS, ra/dec pseudo-random (incompressible).
fn catalog_batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ra", DataType::Float64, false),
        Field::new("dec", DataType::Float64, false),
        Field::new("mag", DataType::Float64, false),
    ]));
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = move || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (state >> 11) as f64 / (1u64 << 53) as f64
    };
    let ids: Int64Array = (0..rows as i64).collect::<Vec<_>>().into();
    let ras: Float64Array = (0..rows).map(|_| 360.0 * next()).collect::<Vec<_>>().into();
    let decs: Float64Array = (0..rows)
        .map(|_| -90.0 + 180.0 * next())
        .collect::<Vec<_>>()
        .into();
    let mags: Float64Array = (0..rows)
        .map(|i| 10.0 + 10.0 * i as f64 / rows as f64)
        .collect::<Vec<_>>()
        .into();
    RecordBatch::try_new(
        schema,
        vec![Arc::new(ids), Arc::new(ras), Arc::new(decs), Arc::new(mags)],
    )
    .expect("build record batch")
}

fn parquet_bytes(rows: usize) -> Vec<u8> {
    let batch = catalog_batch(rows);
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROW_GROUP))
        .build();
    let mut buf = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).expect("parquet writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    buf
}

fn write_catalog(dir: &Path) -> u64 {
    let bytes = parquet_bytes(ROWS);
    let path = dir.join("wfi_catalog.parquet");
    std::fs::write(&path, &bytes).expect("write parquet file");
    bytes.len() as u64
}

/// Loopback-reachable address for an endpoint bound with `presets::Minimal`
/// (no relays, no discovery — direct sockets only).
fn loopback_addr(endpoint: &Endpoint) -> EndpointAddr {
    let mut addr = EndpointAddr::new(endpoint.id());
    for sock in endpoint.bound_sockets() {
        let sock = if sock.ip().is_unspecified() {
            let ip: IpAddr = if sock.is_ipv4() {
                Ipv4Addr::LOCALHOST.into()
            } else {
                Ipv6Addr::LOCALHOST.into()
            };
            SocketAddr::new(ip, sock.port())
        } else {
            sock
        };
        addr = addr.with_ip_addr(sock);
    }
    addr
}

async fn spawn_server(
    store: Arc<dyn ObjectStore>,
    access: Access,
    local_root: Option<&Path>,
) -> (Router, EndpointAddr) {
    let endpoint = Endpoint::builder(presets::Minimal)
        .bind()
        .await
        .expect("bind server endpoint");
    let addr = loopback_addr(&endpoint);
    let canonical = local_root.map(|p| p.canonicalize().expect("canonicalize root"));
    let handler = PopfusionServer::new(store, access, canonical);
    let router = Router::builder(endpoint)
        .accept(proto::ALPN, handler)
        .spawn();
    (router, addr)
}

async fn client_for(key: SecretKey, addr: EndpointAddr) -> (Endpoint, Arc<IrohObjectStore>) {
    let endpoint = Endpoint::builder(presets::Minimal)
        .secret_key(key)
        .bind()
        .await
        .expect("bind client endpoint");
    let store = IrohObjectStore::connect(&endpoint, addr)
        .await
        .expect("connect to server");
    (endpoint, Arc::new(store))
}

fn scalar_i64(batch: &RecordBatch, column: &str) -> i64 {
    batch
        .column_by_name(column)
        .expect("column present")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column")
        .value(0)
}

fn scalar_f64(batch: &RecordBatch, column: &str) -> f64 {
    batch
        .column_by_name(column)
        .expect("column present")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("float64 column")
        .value(0)
}

#[tokio::test]
async fn e2e_selective_query_transfers_under_10_percent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let file_size = write_catalog(dir.path());

    let client_key = SecretKey::generate();
    let access = Access::Allow([client_key.public()].into_iter().collect());
    let local = LocalFileSystem::new_with_prefix(dir.path()).expect("local store");
    let (router, addr) = spawn_server(Arc::new(local), access, Some(dir.path())).await;
    let server_id = addr.id;

    let (endpoint, store) = client_for(client_key, addr).await;
    let (ctx, total_remote) = query::create_session(store.clone(), server_id)
        .await
        .expect("create session");
    assert_eq!(total_remote, file_size);

    // mag < 10.4 selects exactly the first 8000 of 200k rows (sorted by mag),
    // i.e. one 10k row group out of twenty.
    let df = ctx
        .sql("SELECT count(*) AS n, min(mag) AS lo, max(mag) AS hi FROM \"wfi_catalog.parquet\" WHERE mag < 10.4")
        .await
        .expect("plan query");
    let batches = df.collect().await.expect("run query");
    let batch = &batches[0];
    assert_eq!(scalar_i64(batch, "n"), 8000);
    assert_eq!(scalar_f64(batch, "lo"), 10.0);
    assert!(scalar_f64(batch, "hi") < 10.4);

    let transferred = store.bytes_transferred();
    assert!(
        transferred < file_size / 10,
        "selective query transferred {transferred} bytes, expected < 10% of {file_size}"
    );
    println!(
        "transferred {} of {} ({:.1}%)",
        transferred,
        file_size,
        100.0 * transferred as f64 / file_size as f64
    );

    endpoint.close().await;
    router.shutdown().await.ok();
}

#[tokio::test]
async fn e2e_unauthorized_peer_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_catalog(dir.path());

    // Allowlist contains some *other* key, not the client's.
    let allowed = SecretKey::generate().public();
    let access = Access::Allow([allowed].into_iter().collect());
    let local = LocalFileSystem::new_with_prefix(dir.path()).expect("local store");
    let (router, addr) = spawn_server(Arc::new(local), access, Some(dir.path())).await;

    let endpoint = Endpoint::builder(presets::Minimal)
        .secret_key(SecretKey::generate())
        .bind()
        .await
        .expect("bind client endpoint");
    // The QUIC handshake may complete before the server rejects the peer, so
    // accept failure either at connect time or on the first request.
    match IrohObjectStore::connect(&endpoint, addr).await {
        Err(_) => {}
        Ok(store) => {
            let result = store.list_all("").await;
            assert!(result.is_err(), "unauthorized peer must not read a listing");
        }
    }

    endpoint.close().await;
    router.shutdown().await.ok();
}

#[tokio::test]
async fn e2e_inmemory_backend_double_serves_queries() {
    // Backend-agnostic serve side (S3 stand-in per README testing table):
    // the same handler over `object_store`'s in-memory double.
    let mem = InMemory::new();
    let rows = 10_000usize;
    mem.put(&ObjPath::from("cat.parquet"), parquet_bytes(rows).into())
        .await
        .expect("seed in-memory object");

    let client_key = SecretKey::generate();
    let access = Access::Allow([client_key.public()].into_iter().collect());
    let (router, addr) = spawn_server(Arc::new(mem), access, None).await;
    let server_id = addr.id;

    let (endpoint, store) = client_for(client_key, addr).await;
    let (ctx, _) = query::create_session(store, server_id)
        .await
        .expect("create session");
    let df = ctx
        .sql("SELECT count(*) AS n FROM \"cat.parquet\"")
        .await
        .expect("plan query");
    let batches = df.collect().await.expect("run query");
    assert_eq!(scalar_i64(&batches[0], "n"), rows as i64);

    // README Quick Start quotes table names with single quotes; that form
    // must keep working verbatim.
    let df = ctx
        .sql("SELECT count(*) AS n FROM 'cat.parquet'")
        .await
        .expect("plan single-quoted query");
    let batches = df.collect().await.expect("run single-quoted query");
    assert_eq!(scalar_i64(&batches[0], "n"), rows as i64);

    endpoint.close().await;
    router.shutdown().await.ok();
}
