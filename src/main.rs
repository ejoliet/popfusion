//! popfusion CLI: `serve | query | ls | id`.

#![deny(unsafe_code)]

use std::{collections::HashSet, path::PathBuf, process::ExitCode, str::FromStr, sync::Arc};

use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use iroh::{
    Endpoint, EndpointAddr, EndpointId, RelayMap, RelayMode, RelayUrl, SecretKey,
    endpoint::presets, protocol::Router,
};
use object_store::{
    ObjectStore, aws::AmazonS3Builder, local::LocalFileSystem, path::Path as ObjPath,
    prefix::PrefixStore,
};
use popfusion::{
    client::{ClientError, IrohObjectStore},
    proto::{self, ErrCode},
    query::{self, OutputFormat, human_bytes},
    server::{Access, PopfusionServer},
};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "popfusion",
    version,
    about = "SQL over remote Parquet, peer-to-peer"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Expose a directory or S3 prefix as a read-only object store
    Serve {
        /// Directory or s3:// prefix to expose
        source: String,
        /// Peer public keys permitted to connect (repeatable)
        #[arg(long = "allow", value_name = "PUBKEY", conflicts_with = "open")]
        allow: Vec<String>,
        /// Serve unauthenticated to ANY peer (loud opt-out of --allow)
        #[arg(long)]
        open: bool,
        /// iroh secret key file; created if absent (default ~/.popfusion/key)
        #[arg(long, value_name = "PATH")]
        secret_key_file: Option<PathBuf>,
        /// Self-hosted relay URL (defaults to the n0 public relays)
        #[arg(long, value_name = "URL")]
        relay_url: Option<String>,
    },
    /// Run SQL against a remote node
    Query {
        /// Remote node id (server public key)
        node_id: String,
        /// SQL statement; tables are named by remote relative path
        sql: String,
        #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
        format: OutputFormat,
        /// Print bytes transferred and remote dataset size
        #[arg(long)]
        stats: bool,
        #[arg(long, value_name = "PATH")]
        secret_key_file: Option<PathBuf>,
    },
    /// List remote objects
    Ls {
        node_id: String,
        prefix: Option<String>,
        #[arg(long, value_name = "PATH")]
        secret_key_file: Option<PathBuf>,
    },
    /// Print this machine's public key (to be allowlisted)
    Id {
        #[arg(long, value_name = "PATH")]
        secret_key_file: Option<PathBuf>,
    },
}

fn main() -> ExitCode {
    init_tracing();
    let cli = Cli::parse();
    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err:#}");
            let (code, hint) = classify_exit(&err);
            if let Some(hint) = hint {
                eprintln!("{hint}");
            }
            ExitCode::from(code)
        }
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_env("PF_LOG").unwrap_or_else(|_| EnvFilter::new("info"));
    let json = std::env::var("PF_LOG_FORMAT").is_ok_and(|v| v == "json");
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    if json {
        builder.json().init();
    } else {
        builder.init();
    }
}

/// README error table: dial/transport → 2, NotFound → 3, RangeInvalid → 4, SQL → 5.
fn classify_exit(err: &anyhow::Error) -> (u8, Option<&'static str>) {
    for cause in err.chain() {
        if cause
            .downcast_ref::<iroh::endpoint::ConnectError>()
            .is_some()
        {
            return (
                2,
                Some(
                    "hint: is the server running, and is your public key (see `popfusion id`) in its --allow list?",
                ),
            );
        }
        if let Some(client) = cause.downcast_ref::<ClientError>() {
            return match client.remote_code() {
                Some(ErrCode::NotFound) => (3, None),
                Some(ErrCode::RangeInvalid) => (4, None),
                Some(ErrCode::Forbidden) => (2, Some("hint: check --allow on the server")),
                Some(ErrCode::Internal) => (1, None),
                None => (2, None), // transport / protocol errors
            };
        }
        if cause
            .downcast_ref::<datafusion::error::DataFusionError>()
            .is_some()
        {
            return (5, None);
        }
    }
    (1, None)
}

#[tokio::main]
async fn run(cli: Cli) -> anyhow::Result<()> {
    match cli.cmd {
        Cmd::Serve {
            source,
            allow,
            open,
            secret_key_file,
            relay_url,
        } => serve(source, allow, open, secret_key_file, relay_url).await,
        Cmd::Query {
            node_id,
            sql,
            format,
            stats,
            secret_key_file,
        } => {
            let (store, node, endpoint) = open_store(&node_id, secret_key_file).await?;
            query::run(store, node, &sql, format, stats).await?;
            endpoint.close().await;
            Ok(())
        }
        Cmd::Ls {
            node_id,
            prefix,
            secret_key_file,
        } => {
            let (store, _, endpoint) = open_store(&node_id, secret_key_file).await?;
            let entries = store.list_all(&prefix.unwrap_or_default()).await?;
            let mut total = 0u64;
            for entry in &entries {
                println!("{:>14}  {}", entry.size, entry.path);
                total += entry.size;
            }
            println!("{} objects, {}", entries.len(), human_bytes(total));
            endpoint.close().await;
            Ok(())
        }
        Cmd::Id { secret_key_file } => {
            let key = load_or_create_key(secret_key_file)?;
            println!("{}", key.public());
            Ok(())
        }
    }
}

async fn serve(
    source: String,
    allow: Vec<String>,
    open: bool,
    secret_key_file: Option<PathBuf>,
    relay_url: Option<String>,
) -> anyhow::Result<()> {
    let access = if open {
        warn!(
            "--open: serving UNAUTHENTICATED — any peer that learns this node id can read the data"
        );
        Access::Open
    } else if allow.is_empty() {
        // Q1 resolution: --allow is mandatory; --open is the loud opt-out.
        bail!(
            "refusing to start without --allow <PUBKEY> (or pass --open to serve unauthenticated)"
        );
    } else {
        let keys = allow
            .iter()
            .map(|s| EndpointId::from_str(s))
            .collect::<Result<HashSet<_>, _>>()
            .context("invalid --allow public key")?;
        Access::Allow(keys)
    };

    let (store, local_root): (Arc<dyn ObjectStore>, Option<PathBuf>) =
        if source.starts_with("s3://") {
            let url = url::Url::parse(&source).context("invalid s3:// URL")?;
            // Credentials come from the standard AWS SDK chain (env, profile, IRSA).
            let s3 = AmazonS3Builder::from_env()
                .with_url(source.clone())
                .build()
                .context("building S3 backend")?;
            let prefix = url.path().trim_matches('/').to_string();
            let store: Arc<dyn ObjectStore> = if prefix.is_empty() {
                Arc::new(s3)
            } else {
                Arc::new(PrefixStore::new(s3, ObjPath::from(prefix)))
            };
            (store, None)
        } else {
            let root = std::fs::canonicalize(&source)
                .with_context(|| format!("cannot open served directory {source:?}"))?;
            (
                Arc::new(LocalFileSystem::new_with_prefix(&root).context("local backend")?),
                Some(root),
            )
        };

    let secret = load_or_create_key(secret_key_file)?;
    let mut builder = Endpoint::builder(presets::N0).secret_key(secret);
    if let Some(raw) = relay_url {
        let relay = RelayUrl::from_str(&raw).context("invalid --relay-url")?;
        builder = builder.relay_mode(RelayMode::Custom(RelayMap::from(relay)));
    }
    let endpoint = builder.bind().await.context("binding iroh endpoint")?;
    println!("node id: {}", endpoint.id());

    let handler = PopfusionServer::new(store, access, local_root);
    let router = Router::builder(endpoint)
        .accept(proto::ALPN, handler)
        .spawn();
    info!(source = %source, "serving; press Ctrl-C to stop");
    tokio::signal::ctrl_c()
        .await
        .context("waiting for Ctrl-C")?;
    router.shutdown().await.ok();
    Ok(())
}

async fn open_store(
    node_id: &str,
    secret_key_file: Option<PathBuf>,
) -> anyhow::Result<(Arc<IrohObjectStore>, EndpointId, Endpoint)> {
    let node = EndpointId::from_str(node_id).context("invalid node id")?;
    // The query side uses the same persistent key that `popfusion id` prints,
    // so the server's --allow entry matches.
    let secret = load_or_create_key(secret_key_file)?;
    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(secret)
        .bind()
        .await
        .context("binding iroh endpoint")?;
    let store = IrohObjectStore::connect(&endpoint, EndpointAddr::new(node)).await?;
    Ok((Arc::new(store), node, endpoint))
}

fn default_key_path() -> anyhow::Result<PathBuf> {
    // AIDEV-NOTE: std::env::home_dir was un-deprecated in Rust 1.85+; avoids a `dirs` dep.
    let home = std::env::home_dir().context("cannot determine home directory")?;
    Ok(home.join(".popfusion").join("key"))
}

/// Load the iroh secret key, creating it (chmod 600) if absent.
/// The key is never logged; only the derived public key is ever printed.
fn load_or_create_key(path: Option<PathBuf>) -> anyhow::Result<SecretKey> {
    let path = match path {
        Some(p) => p,
        None => default_key_path()?,
    };
    if path.exists() {
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("reading secret key file {}", path.display()))?;
        let key = SecretKey::from_str(raw.trim())
            .with_context(|| format!("parsing secret key file {}", path.display()))?;
        Ok(key)
    } else {
        let key = SecretKey::generate();
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        }
        std::fs::write(&path, to_hex(&key.to_bytes()) + "\n")
            .with_context(|| format!("writing secret key file {}", path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                .context("chmod 600 on secret key file")?;
        }
        info!(path = %path.display(), "created new secret key");
        Ok(key)
    }
}

fn to_hex(bytes: &[u8; 32]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::with_capacity(64), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_key_round_trips_through_from_str() {
        let key = SecretKey::generate();
        let hex = to_hex(&key.to_bytes());
        assert_eq!(hex.len(), 64);
        let parsed = SecretKey::from_str(&hex).expect("hex key parses");
        assert_eq!(parsed.public(), key.public());
    }
}
