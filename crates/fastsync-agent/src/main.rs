mod api;
mod benchmark;
mod events;
mod jobs;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use benchmark::{GenerateDatasetArgs, LocalBenchmarkArgs};
use clap::{Args, Parser, Subcommand};
use fastsync_discovery::{DiscoveryConfig, DiscoveryService};
use fastsync_protocol::DiscoveryAnnouncement;
use fastsync_storage::Database;
use fastsync_transfer::{DeviceIdentity, TransferEngine};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use crate::api::{AGENT_FEATURES, AppState};
use crate::events::EventBus;
use crate::jobs::JobManager;

pub(crate) const DEFAULT_QUIC_PORT: u16 = 39_463;

#[derive(Debug, Parser)]
#[command(
    name = "fastsync",
    version,
    about = "Authenticated LAN file transfer agent"
)]
struct Cli {
    #[command(flatten)]
    agent: AgentArgs,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Args)]
struct AgentArgs {
    /// Persistent state directory. Defaults to the platform's local data directory.
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Name advertised to other FastSync devices. Defaults to the hostname.
    #[arg(long)]
    device_name: Option<String>,

    /// HTTP dashboard and API listen address.
    #[arg(long, default_value = "127.0.0.1:8765")]
    http_bind: SocketAddr,

    /// QUIC transfer listen address.
    #[arg(long, default_value = "0.0.0.0:39463")]
    quic_bind: SocketAddr,

    /// UDP discovery listen address.
    #[arg(long, default_value = "0.0.0.0:39462")]
    discovery_bind: SocketAddr,

    /// UDP discovery broadcast destination.
    #[arg(long, default_value = "255.255.255.255:39462")]
    discovery_broadcast: SocketAddr,

    /// Seconds between discovery announcements.
    #[arg(long, default_value_t = 3)]
    discovery_interval: u64,

    /// Disable LAN discovery while retaining manual connection support.
    #[arg(long)]
    no_discovery: bool,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Generate a deterministic local benchmark dataset. Refuses to run in CI.
    #[command(name = "generate-dataset", visible_alias = "dataset")]
    GenerateDataset(GenerateDatasetArgs),

    /// Compare local filesystem copy strategies; this does not benchmark the network.
    #[command(name = "benchmark", visible_alias = "bench")]
    Benchmark(LocalBenchmarkArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    initialize_tracing();
    let cli = Cli::parse();
    match cli.command {
        Some(Command::GenerateDataset(args)) => benchmark::run_generate_command(args),
        Some(Command::Benchmark(args)) => benchmark::run_benchmark_command(args).await,
        None => run_agent(cli.agent).await,
    }
}

fn initialize_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    if let Err(error) = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init()
    {
        eprintln!("failed to initialize tracing subscriber: {error}");
    }
}

async fn run_agent(args: AgentArgs) -> Result<()> {
    if !(1..=3_600).contains(&args.discovery_interval) {
        bail!("--discovery-interval must be between 1 and 3600 seconds");
    }
    let data_directory = match args.data_dir {
        Some(path) => path,
        None => dirs::data_local_dir()
            .ok_or_else(|| anyhow!("the platform has no local data directory"))?
            .join("FastSync"),
    };
    let device_name = match args.device_name {
        Some(name) if !name.trim().is_empty() => name,
        Some(_) => bail!("--device-name must not be empty"),
        None => default_device_name()?,
    };
    let database_path = data_directory.join("fastsync.sqlite3");
    let database = Database::open(&database_path)
        .with_context(|| format!("failed to open `{}`", database_path.display()))?;
    let identity = DeviceIdentity::load_or_create(database.clone(), device_name)
        .context("failed to load persistent device identity")?;
    let engine = TransferEngine::bind(args.quic_bind, database.clone(), identity.clone())
        .with_context(|| format!("failed to bind QUIC endpoint to {}", args.quic_bind))?;
    let quic_address = engine
        .local_addr()
        .context("failed to read bound QUIC address")?;
    let http_listener = TcpListener::bind(args.http_bind)
        .await
        .with_context(|| format!("failed to bind HTTP endpoint to {}", args.http_bind))?;
    let http_address = http_listener
        .local_addr()
        .context("failed to read bound HTTP address")?;

    let online_ttl = Duration::from_secs(args.discovery_interval.saturating_mul(3).max(10));
    let events = EventBus::new();
    let jobs = JobManager::new(database.clone(), engine.clone(), events.clone());
    let cancellation = CancellationToken::new();
    let state = AppState::new(
        database,
        engine.clone(),
        jobs.clone(),
        events,
        online_ttl,
        http_address,
        quic_address,
        data_directory.clone(),
        cancellation.clone(),
    );
    let (fatal_sender, mut fatal_receiver) = mpsc::channel::<String>(2);
    let mut tasks = Vec::new();

    let quic_task_engine = engine.clone();
    let quic_cancellation = cancellation.clone();
    let quic_failure = fatal_sender.clone();
    tasks.push(tokio::spawn(async move {
        let result = quic_task_engine.serve(quic_cancellation.clone()).await;
        match result {
            Err(error) => {
                let _ = quic_failure
                    .send(format!("QUIC service failed: {error}"))
                    .await;
            }
            Ok(()) if !quic_cancellation.is_cancelled() => {
                let _ = quic_failure
                    .send("QUIC service stopped unexpectedly".to_owned())
                    .await;
            }
            Ok(()) => {}
        }
    }));

    let http_cancellation = cancellation.clone();
    let http_shutdown = http_cancellation.clone();
    let http_failure = fatal_sender.clone();
    let application = api::router(state.clone());
    tasks.push(tokio::spawn(async move {
        let shutdown = async move {
            http_shutdown.cancelled().await;
        };
        let result = axum::serve(http_listener, application)
            .with_graceful_shutdown(shutdown)
            .await;
        match result {
            Err(error) => {
                let _ = http_failure
                    .send(format!("HTTP service failed: {error}"))
                    .await;
            }
            Ok(()) if !http_cancellation.is_cancelled() => {
                let _ = http_failure
                    .send("HTTP service stopped unexpectedly".to_owned())
                    .await;
            }
            Ok(()) => {}
        }
    }));

    if !args.no_discovery {
        let discovery_config = DiscoveryConfig {
            bind_addr: args.discovery_bind,
            broadcast_addr: args.discovery_broadcast,
            announce_interval: Duration::from_secs(args.discovery_interval),
            peer_ttl: online_ttl,
        };
        let announcement = DiscoveryAnnouncement::new(
            identity.id(),
            identity.name(),
            quic_address.port(),
            http_address.port(),
            AGENT_FEATURES
                .iter()
                .map(|feature| (*feature).to_owned())
                .collect(),
        );
        match DiscoveryService::bind_with_channel(discovery_config, announcement, 128).await {
            Ok((service, mut peer_updates)) => {
                tracing::info!(
                    bind = %service.local_addr(),
                    broadcast = %args.discovery_broadcast,
                    "LAN discovery enabled"
                );
                let discovery_cancellation = cancellation.clone();
                tasks.push(tokio::spawn(async move {
                    if let Err(error) = service.run(discovery_cancellation).await {
                        tracing::warn!(%error, "discovery service stopped; manual connections remain available");
                    }
                }));

                let consumer_state = state.clone();
                let consumer_cancellation = cancellation.clone();
                tasks.push(tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = consumer_cancellation.cancelled() => return,
                            peer = peer_updates.recv() => {
                                let Some(peer) = peer else {
                                    return;
                                };
                                if let Err(error) = consumer_state.record_discovery(peer) {
                                    tracing::warn!(%error, "failed to persist discovered device");
                                }
                            }
                        }
                    }
                }));
            }
            Err(error) => {
                tracing::warn!(%error, "discovery could not start; manual connections remain available");
            }
        }
    } else {
        tracing::info!("LAN discovery disabled; manual connections remain available");
    }
    drop(fatal_sender);

    tracing::info!(
        device_id = %identity.id(),
        device_name = identity.name(),
        http = %http_address,
        quic = %quic_address,
        data_dir = %data_directory.display(),
        discovery = %args.discovery_bind,
        "FastSync agent started"
    );

    let failure = tokio::select! {
        signal = tokio::signal::ctrl_c() => {
            signal
                .err()
                .map(|error| format!("failed to listen for Ctrl-C: {error}"))
        }
        failure = fatal_receiver.recv() => failure,
    };
    cancellation.cancel();
    jobs.shutdown().await;
    engine.close();
    for task in tasks {
        if let Err(error) = task.await {
            tracing::warn!(%error, "agent task did not shut down cleanly");
        }
    }
    if let Some(failure) = failure {
        return Err(anyhow!(failure));
    }
    tracing::info!("FastSync agent stopped");
    Ok(())
}

fn default_device_name() -> Result<String> {
    let hostname = hostname::get().context("failed to read hostname")?;
    let name = hostname.to_string_lossy().trim().to_owned();
    if name.is_empty() {
        Ok("FastSync device".to_owned())
    } else {
        Ok(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_without_subcommand_selects_agent_mode() {
        let parsed = Cli::try_parse_from(["fastsync"]);
        assert!(parsed.is_ok());
        if let Ok(cli) = parsed {
            assert!(cli.command.is_none());
            assert_eq!(cli.agent.http_bind.port(), 8765);
            assert_eq!(cli.agent.quic_bind.port(), DEFAULT_QUIC_PORT);
        }
    }
}
