# popfusion implementation notes

One line per entry, timestamped, grep-friendly. DEVIATION: prefix marks departures from README spec.

- 2026-08-27 Q1 resolved per README recommendation: `--allow` mandatory; `--open` flag is a loud opt-out for unauthenticated demo serving.
- 2026-08-27 Q2 resolved per README API contract section (already normative there): tables named by quoted relative path; homogeneous directories also registered as one listing table named after the directory.
- 2026-08-27 Q3 resolved per stated default: iroh caret `1`.
- 2026-08-27 object_store pinned 0.13 (not latest 0.14) because DataFusion 55 depends on ^0.13.2; trait identity must match. Alternative rejected: 0.14 (would not compile against DataFusion's register_object_store).
- 2026-08-27 tracing-subscriber, serde, async-trait, bytes, futures added beyond README dep list; each justified in Cargo.toml comments (required companions of listed deps, not new capabilities).
- 2026-08-27 arrow/parquet for test data generation used via `datafusion::arrow` / `datafusion::parquet` re-exports to avoid extra direct deps.
- 2026-08-27 DEVIATION: added src/lib.rs beyond README layout — integration tests (tests/e2e.rs) cannot import from a binary-only crate; main.rs stays thin over the lib.
- 2026-08-27 client get_opts issues one Stat round-trip per get so ObjectMeta is always truthful; alternative rejected: fabricating meta for Bounded ranges (risks lying to DataFusion consumers). Latency optimization deferred to v2 footer caching.
- 2026-08-27 --stats counter counts wire frame bytes in BOTH directions (requests + responses) via AtomicU64 in request path; simplest honest definition of "transferred".
- 2026-08-27 e2e uses iroh presets::Minimal (no relay, no discovery) + loopback EndpointAddr from bound_sockets, so tests touch no external network services.
- 2026-08-27 query/ls load the same persistent secret key as `popfusion id` — otherwise the pubkey the user allowlisted would not match the dialing endpoint.
- 2026-08-27 symlink-escape defense: canonicalize(root.join(path)) must stay under canonicalized root, checked for Stat/GetRange on local backend; List relies on the rooted LocalFileSystem (names only, no byte access).
- 2026-08-27 iroh 1.1 API notes: Endpoint::builder(presets::N0), Router::builder(...).accept(ALPN, handler).spawn(), ProtocolHandler::accept(&self, Connection) -> Result<(), AcceptError>, EndpointId (formerly NodeId), SecretKey::generate() takes no rng.
- 2026-08-27 verified DataFusion 55 accepts single-quoted table names ('wfi_catalog.parquet') exactly as the README Quick Start shows; asserted in e2e.
- 2026-08-27 acceptance evidence: selective predicate (mag < 10.4, 1 of 20 row groups) transferred 622,123 of 7,819,122 bytes = 8.0% < 10% gate.
