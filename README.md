# popfusion

> SQL over remote Parquet, peer-to-peer. Dial a public key, run a query, fetch only the byte ranges that survive predicate pruning. No server, no presigned URLs, no cloud account required on the client.

![Rust](https://img.shields.io/badge/rust-1.85+-orange)
![License](https://img.shields.io/badge/license-MIT-lightgrey)
![Status](https://img.shields.io/badge/status-v1_spec-yellow)

---

## Purpose

**Problem**: Parquet data that exists (scratch volumes, S3 buckets, laptops) but is not queryable by collaborators without ingestion, IAM grants, or bulk file copies. The gap between "data lands on a machine" and "someone else can query it" is measured in days.

**Solution**: A single Rust binary with two modes. `serve` exposes a directory or S3 prefix as a read-only, range-readable object store over an iroh P2P connection. `query` runs Apache DataFusion locally against that remote store. DataFusion's Parquet reader fetches only footers and pruned row groups, so selective queries against multi-GB datasets transfer kilobytes-to-megabytes.

**Scope**: Pre-archive lateral collaboration (Roman SSC commissioning artifacts, pipeline dev handoffs, ROMAN-4652 light-curve evaluation harness) and OSS release for the general "torrent meets DuckDB" audience. Not a TAP/IRSA replacement.

---

## Architecture

```
 QUERY SIDE (client)                       SERVE SIDE (data holder)
┌──────────────────────────┐              ┌──────────────────────────┐
│ CLI: popfusion query      │              │ CLI: popfusion serve      │
│  ┌──────────────────┐    │   iroh QUIC  │  ┌────────────────────┐  │
│  │ DataFusion       │    │  ALPN:       │  │ ProtocolHandler    │  │
│  │ (SQL, pruning,   │    │ popfusion/1  │  │  LIST / STAT / GET │  │
│  │  joins, Arrow)   │    │◄────────────►│  └─────────┬──────────┘  │
│  └───────┬──────────┘    │  direct P2P  │            │             │
│  ┌───────▼──────────┐    │  or relay    │  ┌─────────▼──────────┐  │
│  │ IrohObjectStore  │    │  fallback    │  │ object_store       │  │
│  │ (object_store    │    │              │  │  LocalFileSystem   │  │
│  │  trait impl)     │    │              │  │  or AmazonS3       │  │
│  └──────────────────┘    │              │  └────────────────────┘  │
└──────────────────────────┘              └──────────────────────────┘
```

**Data flow**:
- `SQL string` → DataFusion plan → `ObjectStore::get_range()` → wire request → server → local disk or S3 `GetObject` (Range) → bytes back → DataFusion execution → Arrow batches printed locally.
- All query compute is client-side. The server is a dumb, auditable byte proxy.

**Key components**:

| Component | Responsibility |
|-----------|---------------|
| `proto` | Wire message types + postcard (de)serialization, ALPN constant |
| `server` | iroh `Router` + `ProtocolHandler`; maps requests to a backing `ObjectStore`; enforces allowlist + path safety |
| `client` | `IrohObjectStore` implementing the `object_store::ObjectStore` trait over an iroh connection |
| `query` | DataFusion `SessionContext` setup, table registration, result printing, bytes-transferred counter |
| `cli` | clap entrypoint: `serve`, `query`, `ls`, `id` |

---

## Recommended Stack

| Layer | Chosen | Why | Rejected |
|-------|--------|-----|----------|
| P2P transport | `iroh` 1.x | Stable 1.0 (wire-protocol stability), dial-by-public-key, hole-punching + relay fallback, custom protocols via ALPN `Router` | `libp2p` (heavier concept surface), raw `quinn` (reimplements NAT traversal/discovery) |
| Query engine | `datafusion` (Apache) | Embedded Rust SQL engine, native `ObjectStore` integration, Parquet footer/stats pruning for free | `duckdb` (C++ FFI, custom filesystem extension harder), `polars` (weaker SQL, no object-store pushdown hook) |
| Storage abstraction | `object_store` (Apache Arrow) | The exact trait DataFusion consumes; ships `LocalFileSystem` and `AmazonS3` backends — server reuses it too | Custom trait (would re-wrap everything) |
| Wire encoding | `postcard` + length-prefixed frames | Tiny, no-schema-registry, serde-native | protobuf/prost (codegen overhead for 4 message types), JSON (binary payloads) |
| CLI | `clap` v4 derive | Standard | — |

> 💡 One round of overrides welcome, but iroh + DataFusion + object_store is the load-bearing triangle; changing any corner changes the project.

---

## Repository Layout

```
popfusion/
├── src/
│   ├── main.rs            # clap CLI: serve | query | ls | id
│   ├── proto.rs           # Request/Response enums, ALPN, framing
│   ├── server.rs          # ProtocolHandler, allowlist, backend wiring
│   ├── client.rs          # IrohObjectStore (ObjectStore impl), byte counter
│   └── query.rs           # DataFusion session, register tables, run SQL
├── tests/
│   └── e2e.rs             # In-process serve+query over loopback iroh
├── Cargo.toml
├── LICENSE                # MIT
└── README.md              # This file
```

---

## Prerequisites

| Requirement | Version / Value | Notes |
|-------------|-----------------|-------|
| Rust | 1.85+ | edition 2024 |
| Network egress | outbound UDP + HTTPS | No inbound ports needed on either side |
| AWS credentials | optional | Serve side only, only for `--s3` mode (env/instance profile/IRSA) |

**Required IAM permissions (S3 mode, serve side only)**:
- `s3:GetObject`, `s3:ListBucket` on the served prefix. Read-only. Nothing else.

---

## Distribution

Single static binary; two channels. Users do **not** need Rust unless they choose the crates.io path.

### Prebuilt binaries (recommended for users)

Releases are automated with [`cargo-dist`](https://opensource.axo.dev/cargo-dist/): pushing a version tag triggers CI that builds macOS (arm64/x86_64), Linux, and Windows binaries, attaches them to a GitHub Release, and generates an installer script.

```bash
# Install (no Rust required)
curl -LsSf https://github.com/ejoliet/popfusion/releases/latest/download/popfusion-installer.sh | sh
```

Maintainer release flow (one-time setup, then per release):

```bash
# One-time
cargo install cargo-dist
dist init                      # writes dist-workspace.toml + .github/workflows/release.yml

# Per release
cargo set-version 0.2.0        # or edit Cargo.toml
git commit -am "release: v0.2.0"
git tag v0.2.0 && git push && git push --tags
# CI builds all targets and publishes the GitHub Release
```

### crates.io (users who have Rust)

```bash
cargo publish                  # maintainer, once per version
cargo install popfusion        # user; compiles locally (DataFusion is heavy, expect minutes)
```

---

## Quick Start

```bash
# Machine A — serve a directory of Parquet files
popfusion serve ./data --allow <peer-pubkey-B>
# prints: node id: <NODE_ID_A>

# Machine A — or serve an S3 prefix (credentials from env / instance profile)
popfusion serve s3://my-bucket/catalogs/ --allow <peer-pubkey-B>

# Machine B — discover what's there
popfusion ls <NODE_ID_A>

# Machine B — query it
popfusion query <NODE_ID_A> \
  "SELECT ra, dec, mag FROM 'wfi_catalog.parquet' WHERE mag < 14 LIMIT 100"
# ...results...
# transferred: 3.2 MB of 41.7 GB remote data

# Machine B — print own pubkey (to be allowlisted)
popfusion id
```

---

## Configuration Reference

CLI flags primary; env vars for the serve daemon case.

| Flag / Variable | Type | Default | Required | Description |
|----------------|------|---------|----------|-------------|
| `serve <SOURCE>` | path or `s3://` URL | — | ✅ | Directory or S3 prefix to expose |
| `--allow <PUBKEY>` | repeatable | — | ✅ (see Q1) | Peer public keys permitted to connect |
| `--secret-key-file` | path | `~/.popfusion/key` | | iroh secret key; created if absent, chmod 600. Persist for stable node ID |
| `--relay-url` | URL | n0 public relays | | Self-hosted relay (IPAC-internal deployments) |
| `PF_LOG` | str | `info` | | tracing filter |
| `AWS_REGION` etc. | str | SDK defaults | | Standard AWS SDK chain, serve side only |

---

## API / Interface Contract

### CLI

```
popfusion serve <SOURCE> [--allow <PUBKEY>]... [--secret-key-file <PATH>] [--relay-url <URL>]
popfusion query <NODE_ID> <SQL> [--format table|csv|json] [--stats]
popfusion ls    <NODE_ID> [PREFIX]
popfusion id    [--secret-key-file <PATH>]
```

`query` registers every `*.parquet` under the remote root as a table named by its
relative path (quoted identifier), and registers a directory of homogeneous Parquet
as one listing table named after the directory.

### Wire protocol — ALPN `popfusion/1`

One bidirectional QUIC stream per request. Frames: `u32` little-endian length prefix + postcard body.

```rust
enum Request {
    List { prefix: String },                    // recursive listing
    Stat { path: String },                      // size, etag/mtime
    GetRange { path: String, offset: u64, len: u64 },
}

enum Response {
    List { entries: Vec<Entry> },               // Entry { path, size }
    Stat { size: u64, last_modified: i64 },
    Data { bytes: Vec<u8> },                    // exactly len, or error
    Err  { code: ErrCode, msg: String },        // NotFound | Forbidden | RangeInvalid | Internal
}
```

Server-side invariants:
- Paths are relative, normalized, and must resolve inside the served root (reject `..`, absolute paths, symlink escapes).
- `GetRange` capped at 16 MiB per request (client chunks larger reads).
- Read-only: no mutating verbs exist in the protocol.
- Unauthorized peer key → connection rejected before any request is read.

### Client contract

`IrohObjectStore` implements `object_store::ObjectStore`: `get`, `get_range`, `get_opts` (range support mandatory), `head`, `list`. Mutating methods return `NotSupported`. Connection is established once per session and streams are multiplexed per request. An `AtomicU64` byte counter backs `--stats`.

---

## Error Handling

| Error | When | Exit code | Retry? |
|-------|------|-----------|--------|
| `DialError` | Peer unreachable / not allowlisted | 2 | No (print hint: check `--allow` on server) |
| `NotFound` | Table/file missing remotely | 3 | No |
| `RangeInvalid` | offset+len beyond object | 4 | No (indicates client bug) |
| `TransportError` | Stream reset mid-read | 2 | Yes, 3x with backoff, per-request |
| SQL errors | DataFusion planning/execution | 5 | No; surface DataFusion message verbatim |

Server logs structured lines to stderr (tracing, JSON when `PF_LOG_FORMAT=json`); never panics on malformed frames — respond `Err` and close the stream.

---

## Testing

```bash
cargo test              # unit + e2e (loopback iroh, tempdir Parquet)
cargo clippy -- -D warnings
```

| Suite | Covers |
|-------|--------|
| `proto` unit | Frame round-trip, oversize frame rejection |
| `server` unit | Path traversal rejection, allowlist enforcement, range caps |
| `e2e.rs` | Full serve+query in one process: generate Parquet with arrow-rs, assert query results AND that bytes transferred < 10% of file size for a selective predicate |

> ⚠️ The bytes-transferred assertion in e2e is the point of the tool. It is an acceptance test, not a nice-to-have.

---

## Non-Goals (v1)

- **EKS packaging** (Dockerfile, manifests, IRSA docs, k8s Secret for the iroh key) — v2, gated on: v1 was dialed by at least one user who is not Emmanuel.
- **Footer/metadata caching on the serve side** — v2 (S3 latency optimization).
- **Prometheus metrics / health endpoint** — v2.
- Writes of any kind. The protocol has no mutating verbs by design.
- Federated multi-node joins in the CLI UX (DataFusion can already do it if you register two stores; no first-class support in v1).
- Dataset discovery/gossip (`hatstream` territory), tickets, web UI.
- TAP facade — separate tool if ever (see tapwrap).

---

## Open Questions

- [ ] **Q1**: Is `--allow` mandatory (refuse to start without it) or does an explicit `--open` flag permit unauthenticated serving for demos? Recommendation: mandatory, with `--open` as a loud opt-out. — owner: Emmanuel, resolve before Phase 2.
- [ ] **Q2**: Table naming for `ls`/`query` ergonomics — quoted relative paths vs. sanitized identifiers (`wfi_catalog` from `wfi_catalog.parquet`)? — owner: Emmanuel, resolve before Phase 4.
- [ ] **Q3**: Pin iroh minor version or track latest 1.x? (Wire stability claimed at 1.0; Cargo caret default is fine unless FFI consumers appear.) — owner: Emmanuel, default: caret `1`.

---

## Agent Build Instructions

> Authoritative build spec. Implement end-to-end from this README only.
> Resolve Open Questions first; Q3 has a stated default.

### Build Order

| Phase | Deliverable | Done when |
|-------|-------------|-----------|
| 0 | Cargo scaffold, CI (fmt + clippy + test), MIT license | `cargo clippy -D warnings` passes |
| 1 | `proto.rs` framing + types | Round-trip unit tests pass |
| 2 | `server.rs` over `LocalFileSystem`, allowlist, path safety | Unit tests incl. traversal attacks pass |
| 3 | `client.rs` `IrohObjectStore` + byte counter | `get_range` e2e over loopback iroh passes |
| 4 | `query.rs` + CLI wiring | `query` returns correct results in e2e |
| 5 | S3 backend branch on serve side | e2e against LocalStack or `object_store` in-memory double |
| 6 | Polish: `--stats` output, `ls`, `id`, README Quick Start verified | Bytes-transferred acceptance test passes |

### Usage

```bash
# 0. build
cargo build --release
alias pf=./target/release/popfusion

# 1. data (any dir with .parquet works; quick sample via python/duckdb if you have one)
mkdir -p /tmp/pfdata   # drop a .parquet file in here

# 2. terminal A — serve (quick demo, no auth)
pf serve /tmp/pfdata --open
# → prints: node id: <NODE_ID>

# 3. terminal B — play
pf ls <NODE_ID>
pf query <NODE_ID> "SELECT * FROM 'yourfile.parquet' LIMIT 10" --stats
Proper auth flow (2 machines, or 2 key files on one machine):

# machine B: print pubkey
pf id                              # → <PUBKEY_B>

# machine A: serve allowlisted
pf serve /tmp/pfdata --allow <PUBKEY_B>

# machine B: query
pf query <NODE_ID_A> "SELECT count(*) FROM 'yourfile.parquet'" --stats
Same-machine with allowlist: give client own key → --secret-key-file /tmp/ck on id/query/ls.
```

Notes:

- --stats prints transferred: X of Y remote data — the whole point, watch it stay tiny on selective WHERE.
- S3: pf serve s3://bucket/prefix --allow … (creds from env).
- Logs: PF_LOG=debug pf serve ….
- Needs outbound UDP+HTTPS (n0 relays/discovery); no inbound ports.
- No sample parquet handy? Fastest: python3 -c "import pyarrow as pa, pyarrow.parquet as pq; pq.write_table(pa.table({'x': range(100000)}), '/tmp/pfdata/t.parquet')" — then FROM 't.parquet'.


### Constraints

- Rust 2024 edition; `#![deny(unsafe_code)]`.
- `AIDEV-` prefixed comments for non-obvious logic only (framing caps, path normalization rationale, ObjectStore trait quirks).
- Secrets: iroh secret key file chmod 600; never logged. No credentials in code or tests.
- No blocking I/O inside async tasks (tokio throughout; `spawn_blocking` for any sync FS edge).
- Server must be defensive: treat every inbound frame as adversarial.
- Dependencies: iroh, datafusion, object_store, postcard, clap, tokio, tracing, anyhow/thiserror. Nothing else without justification in a code comment.

### Acceptance Criteria

- [ ] `cargo test` passes; e2e proves selective query transfers < 10% of total remote bytes
- [ ] `cargo clippy -- -D warnings` clean
- [ ] Quick Start works verbatim on two real machines on different networks (the GO/NO-GO gate)
- [ ] Path traversal and unauthorized-peer tests pass
- [ ] `--stats` prints total bytes transferred and remote dataset size
- [ ] ship-check run before repo goes public (key files gitignored — cullroom rule)

### GO/NO-GO Gates

| Gate | Criterion | On NO-GO |
|------|-----------|----------|
| Spike (day 1) | Cross-network query on ≥ 5 GB real Parquet with verified pruning | Archive repo, write burn note |
| v2 | ≥ 1 non-Emmanuel user dialed a served dataset within 30 days | Stay at v1, no EKS work |

---

## Next Steps

1. [ ] Resolve Q1, Q2 (Q3 defaulted)
2. [ ] Phases 0–4 (local backend, full query path) — this is the day-1 spike
3. [ ] Run spike gate demo; capture the bytes-transferred screenshot
4. [ ] Phase 5–6 (S3 backend, polish)
5. [ ] ship-check, then publish `ejoliet/popfusion` + HN/Reddit post
6. [ ] Evaluate v2 gate after 30 days

---

## References

- iroh docs — custom protocols & Router: https://docs.iroh.computer/concepts/protocols
- iroh crate (1.x, ProtocolHandler example): https://crates.io/crates/iroh
- DataFusion `ObjectStore` integration and `object_store` crate (Apache Arrow)
- Related internal specs: hatstream, tapwrap, ROMAN-4652 light-curve evaluation
