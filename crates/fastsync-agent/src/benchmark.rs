use std::fs::{self, File};
use std::io::BufWriter;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, ValueEnum};
use fastsync_core::{FileType, ManifestEntry, safe_join, scan_directory};
use futures::{StreamExt, stream};
use serde::Serialize;

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;
const GIB: u64 = 1024 * MIB;
const DEFAULT_MAX_TOTAL_BYTES: u64 = 4 * GIB;
const MAX_FILE_COUNT: u64 = 1_000_000;
const GENERATOR_BUFFER_SIZE: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum DatasetProfile {
    Small,
    Medium,
    Large,
    Mixed,
}

#[derive(Clone, Debug, Args)]
pub struct GenerateDatasetArgs {
    /// Directory to create.
    pub output: PathBuf,

    /// Distribution of deterministic files.
    #[arg(long, value_enum, default_value = "mixed")]
    pub profile: DatasetProfile,

    /// Scale profile file counts from 1 to 1000 percent.
    #[arg(long, default_value_t = 100)]
    pub scale_percent: u32,

    /// Replace the profile's total file count.
    #[arg(long)]
    pub files: Option<u64>,

    /// Replace every generated file size in bytes.
    #[arg(long, conflicts_with = "bytes_per_file_mib")]
    pub bytes_per_file: Option<u64>,

    /// Replace every generated file size in MiB.
    #[arg(long, conflicts_with = "bytes_per_file")]
    pub bytes_per_file_mib: Option<u64>,

    /// Refuse plans larger than this many bytes unless explicitly raised.
    #[arg(long, default_value_t = DEFAULT_MAX_TOTAL_BYTES)]
    pub max_total_bytes: u64,

    /// Seed for deterministic file contents.
    #[arg(long, default_value_t = 0x4653_594e_4301_u64)]
    pub seed: u64,

    /// Replace a non-empty output directory.
    #[arg(long)]
    pub force: bool,
}

#[derive(Clone, Debug, Args)]
pub struct LocalBenchmarkArgs {
    /// Existing local dataset directory.
    pub source: PathBuf,

    /// Empty scratch directory used for benchmark copies.
    pub destination: PathBuf,

    /// Configured scheduler concurrency for the third comparison.
    #[arg(long, default_value_t = 8)]
    pub concurrency: usize,

    /// Preserve all three copied trees after the benchmark.
    #[arg(long)]
    pub keep_output: bool,

    /// Replace a non-empty benchmark destination.
    #[arg(long)]
    pub force: bool,

    /// Emit the report as JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct DatasetSummary {
    pub output: PathBuf,
    pub profile: String,
    pub files: u64,
    pub bytes: u64,
    pub seed: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct BenchmarkReport {
    pub benchmark: &'static str,
    pub source: PathBuf,
    pub files: u64,
    pub bytes: u64,
    pub results: Vec<BenchmarkResult>,
}

#[derive(Clone, Debug, Serialize)]
pub struct BenchmarkResult {
    pub strategy: String,
    pub concurrency: usize,
    pub elapsed_seconds: f64,
    pub mebibytes_per_second: f64,
    pub files_per_second: f64,
}

#[derive(Clone, Debug)]
struct DatasetClass {
    name: &'static str,
    count: u64,
    bytes_per_file: u64,
}

pub fn run_generate_command(args: GenerateDatasetArgs) -> Result<()> {
    if std::env::var_os("CI").is_some() {
        bail!("refusing to generate benchmark data while CI is set");
    }
    let summary = generate_dataset(&args)?;
    println!("FastSync deterministic dataset generated");
    println!("  path: {}", summary.output.display());
    println!("  profile: {}", summary.profile);
    println!("  files: {}", summary.files);
    println!(
        "  size: {:.2} MiB ({} bytes)",
        summary.bytes as f64 / MIB as f64,
        summary.bytes
    );
    println!("  seed: {}", summary.seed);
    Ok(())
}

pub async fn run_benchmark_command(args: LocalBenchmarkArgs) -> Result<()> {
    let report = local_copy_benchmark(
        &args.source,
        &args.destination,
        args.concurrency,
        args.keep_output,
        args.force,
    )
    .await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_report(&report);
    }
    Ok(())
}

fn generate_dataset(args: &GenerateDatasetArgs) -> Result<DatasetSummary> {
    if !(1..=1000).contains(&args.scale_percent) {
        bail!("scale-percent must be between 1 and 1000");
    }
    if args.max_total_bytes == 0 {
        bail!("max-total-bytes must be greater than zero");
    }
    if args.files == Some(0) {
        bail!("files must be greater than zero");
    }

    let override_size = match (args.bytes_per_file, args.bytes_per_file_mib) {
        (Some(bytes), None) => Some(bytes),
        (None, Some(mib)) => Some(
            mib.checked_mul(MIB)
                .ok_or_else(|| anyhow!("bytes-per-file-mib is too large"))?,
        ),
        (None, None) => None,
        (Some(_), Some(_)) => bail!("only one file-size override may be supplied"),
    };
    let mut classes = profile_classes(args.profile);
    if let Some(files) = args.files {
        let representative_size =
            override_size.unwrap_or_else(|| representative_size(args.profile));
        classes = vec![DatasetClass {
            name: "custom",
            count: files,
            bytes_per_file: representative_size,
        }];
    } else {
        for class in &mut classes {
            class.count = scaled_count(class.count, args.scale_percent)?;
            if let Some(size) = override_size {
                class.bytes_per_file = size;
            }
        }
    }
    let file_count = classes.iter().try_fold(0_u64, |total, class| {
        total
            .checked_add(class.count)
            .ok_or_else(|| anyhow!("dataset file count overflow"))
    })?;
    if file_count == 0 {
        bail!("dataset must contain at least one file");
    }
    if file_count > MAX_FILE_COUNT {
        bail!("dataset requests {file_count} files; maximum is {MAX_FILE_COUNT}");
    }
    let total_bytes = classes.iter().try_fold(0_u64, |total, class| {
        let class_bytes = class
            .count
            .checked_mul(class.bytes_per_file)
            .ok_or_else(|| anyhow!("dataset byte count overflow"))?;
        total
            .checked_add(class_bytes)
            .ok_or_else(|| anyhow!("dataset byte count overflow"))
    })?;
    if total_bytes > args.max_total_bytes {
        bail!(
            "dataset would create {total_bytes} bytes, above max-total-bytes {}; raise the limit explicitly to continue",
            args.max_total_bytes
        );
    }

    prepare_output_directory(&args.output, args.force, "dataset")?;
    write_dataset(&args.output, &classes, args.seed)?;

    Ok(DatasetSummary {
        output: args.output.clone(),
        profile: format!("{:?}", args.profile).to_lowercase(),
        files: file_count,
        bytes: total_bytes,
        seed: args.seed,
    })
}

async fn local_copy_benchmark(
    source: &Path,
    destination: &Path,
    concurrency: usize,
    keep_output: bool,
    force: bool,
) -> Result<BenchmarkReport> {
    if !(1..=1024).contains(&concurrency) {
        bail!("concurrency must be between 1 and 1024");
    }
    let source = fs::canonicalize(source)
        .with_context(|| format!("failed to resolve source `{}`", source.display()))?;
    if !source.is_dir() {
        bail!("benchmark source `{}` is not a directory", source.display());
    }

    let scan_source = source.clone();
    let scan = tokio::task::spawn_blocking(move || scan_directory(scan_source))
        .await
        .context("dataset scan task failed")??;
    if !scan.errors.is_empty() {
        bail!(
            "dataset scan reported {} errors; refusing an incomplete benchmark",
            scan.errors.len()
        );
    }
    let files = scan
        .entries
        .iter()
        .filter(|entry| entry.file_type == FileType::File)
        .count() as u64;
    let bytes = scan
        .entries
        .iter()
        .filter(|entry| entry.file_type == FileType::File)
        .map(|entry| entry.size)
        .try_fold(0_u64, |total, size| total.checked_add(size))
        .ok_or_else(|| anyhow!("dataset byte count overflow"))?;

    let safe_destination = canonical_target(destination)?;
    if safe_destination.starts_with(&source) || source.starts_with(&safe_destination) {
        bail!(
            "benchmark source `{}` and destination `{}` must not overlap",
            source.display(),
            safe_destination.display()
        );
    }
    prepare_output_directory(destination, force, "benchmark")?;
    let destination = fs::canonicalize(destination).with_context(|| {
        format!(
            "failed to resolve benchmark destination `{}`",
            destination.display()
        )
    })?;
    if destination.starts_with(&source) || source.starts_with(&destination) {
        bail!(
            "benchmark source `{}` and destination `{}` must not overlap",
            source.display(),
            destination.display()
        );
    }

    let mut results = Vec::with_capacity(3);
    let sequential_destination = destination.join("sequential");
    let source_for_copy = source.clone();
    let entries_for_copy = scan.entries.clone();
    let sequential_destination_for_copy = sequential_destination.clone();
    let (elapsed, copied_bytes) = tokio::task::spawn_blocking(move || {
        copy_sequential(
            &source_for_copy,
            &sequential_destination_for_copy,
            &entries_for_copy,
        )
    })
    .await
    .context("sequential copy task failed")??;
    validate_copied_bytes(bytes, copied_bytes, "sequential")?;
    results.push(benchmark_result("sequential", 1, elapsed, files, bytes));
    if !keep_output {
        remove_tree(&sequential_destination)?;
    }

    for (name, scheduler_concurrency) in [
        ("scheduler-1", 1_usize),
        ("scheduler-configured", concurrency),
    ] {
        let run_destination = destination.join(format!("{name}-{scheduler_concurrency}"));
        let (elapsed, copied_bytes) = copy_scheduled(
            &source,
            &run_destination,
            &scan.entries,
            scheduler_concurrency,
        )
        .await?;
        validate_copied_bytes(bytes, copied_bytes, name)?;
        results.push(benchmark_result(
            name,
            scheduler_concurrency,
            elapsed,
            files,
            bytes,
        ));
        if !keep_output {
            remove_tree(&run_destination)?;
        }
    }

    if !keep_output {
        match fs::remove_dir(&destination) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::DirectoryNotEmpty => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to remove empty benchmark directory `{}`",
                        destination.display()
                    )
                });
            }
        }
    }

    Ok(BenchmarkReport {
        benchmark: "local_filesystem_copy",
        source,
        files,
        bytes,
        results,
    })
}

fn profile_classes(profile: DatasetProfile) -> Vec<DatasetClass> {
    match profile {
        DatasetProfile::Small => vec![DatasetClass {
            name: "small",
            count: 1_024,
            bytes_per_file: 4 * KIB,
        }],
        DatasetProfile::Medium => vec![DatasetClass {
            name: "medium",
            count: 128,
            bytes_per_file: MIB,
        }],
        DatasetProfile::Large => vec![DatasetClass {
            name: "large",
            count: 4,
            bytes_per_file: 256 * MIB,
        }],
        DatasetProfile::Mixed => vec![
            DatasetClass {
                name: "small",
                count: 512,
                bytes_per_file: 4 * KIB,
            },
            DatasetClass {
                name: "medium",
                count: 64,
                bytes_per_file: MIB,
            },
            DatasetClass {
                name: "large",
                count: 2,
                bytes_per_file: 128 * MIB,
            },
        ],
    }
}

const fn representative_size(profile: DatasetProfile) -> u64 {
    match profile {
        DatasetProfile::Small => 4 * KIB,
        DatasetProfile::Medium | DatasetProfile::Mixed => MIB,
        DatasetProfile::Large => 256 * MIB,
    }
}

fn scaled_count(count: u64, percent: u32) -> Result<u64> {
    let numerator = count
        .checked_mul(u64::from(percent))
        .ok_or_else(|| anyhow!("scaled file count overflow"))?;
    Ok(numerator.div_ceil(100).max(1))
}

fn prepare_output_directory(path: &Path, force: bool, kind: &str) -> Result<()> {
    if path.exists() {
        let metadata = fs::symlink_metadata(path)
            .with_context(|| format!("failed to inspect {kind} output `{}`", path.display()))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            bail!(
                "{kind} output `{}` is not a regular directory",
                path.display()
            );
        }
        let mut entries = fs::read_dir(path)
            .with_context(|| format!("failed to read {kind} output `{}`", path.display()))?;
        let non_empty = entries
            .next()
            .transpose()
            .with_context(|| format!("failed to read {kind} output `{}`", path.display()))?
            .is_some();
        if non_empty && !force {
            bail!(
                "{kind} output `{}` is not empty; pass --force to replace it",
                path.display()
            );
        }
        if non_empty {
            fs::remove_dir_all(path)
                .with_context(|| format!("failed to replace {kind} output `{}`", path.display()))?;
        }
    }
    fs::create_dir_all(path)
        .with_context(|| format!("failed to create {kind} output `{}`", path.display()))
}

fn canonical_target(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .context("failed to read the current directory")?
            .join(path)
    };
    let mut existing = absolute.as_path();
    let mut missing = Vec::new();
    while !existing.exists() {
        let name = existing.file_name().ok_or_else(|| {
            anyhow!(
                "cannot resolve benchmark destination `{}`",
                absolute.display()
            )
        })?;
        missing.push(name.to_os_string());
        existing = existing.parent().ok_or_else(|| {
            anyhow!(
                "cannot resolve benchmark destination `{}`",
                absolute.display()
            )
        })?;
    }
    let mut resolved = fs::canonicalize(existing).with_context(|| {
        format!(
            "failed to resolve benchmark destination parent `{}`",
            existing.display()
        )
    })?;
    for component in missing.into_iter().rev() {
        resolved.push(component);
    }
    Ok(resolved)
}

fn write_dataset(root: &Path, classes: &[DatasetClass], seed: u64) -> Result<()> {
    let mut global_index = 0_u64;
    let mut buffer = vec![0_u8; GENERATOR_BUFFER_SIZE];
    for class in classes {
        for class_index in 0..class.count {
            let bucket = class_index / 128;
            let directory = root.join(class.name).join(format!("set-{bucket:04}"));
            fs::create_dir_all(&directory).with_context(|| {
                format!(
                    "failed to create dataset directory `{}`",
                    directory.display()
                )
            })?;
            let path = directory.join(format!("file-{class_index:08}.bin"));
            write_deterministic_file(
                &path,
                class.bytes_per_file,
                seed ^ global_index.wrapping_mul(0x9e37_79b9_7f4a_7c15),
                &mut buffer,
            )?;
            global_index = global_index.saturating_add(1);
        }
    }
    Ok(())
}

fn write_deterministic_file(path: &Path, size: u64, seed: u64, buffer: &mut [u8]) -> Result<()> {
    let file = File::create(path)
        .with_context(|| format!("failed to create dataset file `{}`", path.display()))?;
    let mut writer = BufWriter::new(file);
    let mut remaining = size;
    let mut state = seed ^ 0xa076_1d64_78bd_642f;
    while remaining > 0 {
        let length = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| anyhow!("dataset file block size is out of range"))?;
        for chunk in buffer[..length].chunks_mut(8) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let random = state.to_le_bytes();
            chunk.copy_from_slice(&random[..chunk.len()]);
        }
        writer
            .write_all(&buffer[..length])
            .with_context(|| format!("failed to write dataset file `{}`", path.display()))?;
        remaining -= length as u64;
    }
    writer
        .flush()
        .with_context(|| format!("failed to flush dataset file `{}`", path.display()))
}

fn copy_sequential(
    source: &Path,
    destination: &Path,
    entries: &[ManifestEntry],
) -> Result<(std::time::Duration, u64)> {
    let started = Instant::now();
    create_directories_sync(destination, entries)?;
    let mut copied = 0_u64;
    for entry in entries
        .iter()
        .filter(|entry| entry.file_type == FileType::File)
    {
        let from = safe_join(source, &entry.relative_path)?;
        let to = safe_join(destination, &entry.relative_path)?;
        let bytes = fs::copy(&from, &to).with_context(|| {
            format!("failed to copy `{}` to `{}`", from.display(), to.display())
        })?;
        copied = copied
            .checked_add(bytes)
            .ok_or_else(|| anyhow!("copied byte count overflow"))?;
    }
    Ok((started.elapsed(), copied))
}

async fn copy_scheduled(
    source: &Path,
    destination: &Path,
    entries: &[ManifestEntry],
    concurrency: usize,
) -> Result<(std::time::Duration, u64)> {
    let started = Instant::now();
    create_directories_async(destination, entries).await?;
    let copies = entries
        .iter()
        .filter(|entry| entry.file_type == FileType::File)
        .map(|entry| {
            let from = safe_join(source, &entry.relative_path);
            let to = safe_join(destination, &entry.relative_path);
            async move {
                let from = from?;
                let to = to?;
                tokio::fs::copy(&from, &to).await.with_context(|| {
                    format!("failed to copy `{}` to `{}`", from.display(), to.display())
                })
            }
        });
    let mut bounded = stream::iter(copies).buffer_unordered(concurrency);
    let mut copied = 0_u64;
    while let Some(result) = bounded.next().await {
        copied = copied
            .checked_add(result?)
            .ok_or_else(|| anyhow!("copied byte count overflow"))?;
    }
    Ok((started.elapsed(), copied))
}

fn create_directories_sync(destination: &Path, entries: &[ManifestEntry]) -> Result<()> {
    fs::create_dir_all(destination).with_context(|| {
        format!(
            "failed to create benchmark destination `{}`",
            destination.display()
        )
    })?;
    for entry in entries
        .iter()
        .filter(|entry| entry.file_type == FileType::Directory)
    {
        let path = safe_join(destination, &entry.relative_path)?;
        fs::create_dir_all(&path).with_context(|| {
            format!("failed to create benchmark directory `{}`", path.display())
        })?;
    }
    Ok(())
}

async fn create_directories_async(destination: &Path, entries: &[ManifestEntry]) -> Result<()> {
    tokio::fs::create_dir_all(destination)
        .await
        .with_context(|| {
            format!(
                "failed to create benchmark destination `{}`",
                destination.display()
            )
        })?;
    for entry in entries
        .iter()
        .filter(|entry| entry.file_type == FileType::Directory)
    {
        let path = safe_join(destination, &entry.relative_path)?;
        tokio::fs::create_dir_all(&path).await.with_context(|| {
            format!("failed to create benchmark directory `{}`", path.display())
        })?;
    }
    Ok(())
}

fn validate_copied_bytes(expected: u64, actual: u64, strategy: &str) -> Result<()> {
    if expected == actual {
        Ok(())
    } else {
        bail!("{strategy} copied {actual} bytes, but the source manifest contains {expected} bytes")
    }
}

fn benchmark_result(
    strategy: &str,
    concurrency: usize,
    elapsed: std::time::Duration,
    files: u64,
    bytes: u64,
) -> BenchmarkResult {
    let seconds = elapsed.as_secs_f64();
    let mebibytes = bytes as f64 / MIB as f64;
    BenchmarkResult {
        strategy: strategy.to_owned(),
        concurrency,
        elapsed_seconds: seconds,
        mebibytes_per_second: if seconds > 0.0 {
            mebibytes / seconds
        } else {
            0.0
        },
        files_per_second: if seconds > 0.0 {
            files as f64 / seconds
        } else {
            0.0
        },
    }
}

fn remove_tree(path: &Path) -> Result<()> {
    fs::remove_dir_all(path)
        .with_context(|| format!("failed to remove benchmark copy `{}`", path.display()))
}

fn print_report(report: &BenchmarkReport) {
    println!("FastSync local filesystem copy benchmark (no network or QUIC)");
    println!("source: {}", report.source.display());
    println!(
        "dataset: {} files, {:.2} MiB ({} bytes)",
        report.files,
        report.bytes as f64 / MIB as f64,
        report.bytes
    );
    println!();
    println!(
        "{:<24} {:>11} {:>13} {:>13}",
        "strategy", "elapsed", "MiB/s", "files/s"
    );
    for result in &report.results {
        println!(
            "{:<24} {:>10.3}s {:>13.2} {:>13.2}",
            format!("{} (c={})", result.strategy, result.concurrency),
            result.elapsed_seconds,
            result.mebibytes_per_second,
            result.files_per_second
        );
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[tokio::test]
    async fn benchmarks_a_small_deterministic_dataset() -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::tempdir()?;
        let source = temporary.path().join("source");
        let destination = temporary.path().join("copies");
        let args = GenerateDatasetArgs {
            output: source.clone(),
            profile: DatasetProfile::Small,
            scale_percent: 100,
            files: Some(4),
            bytes_per_file: Some(1_024),
            bytes_per_file_mib: None,
            max_total_bytes: MIB,
            seed: 7,
            force: false,
        };
        let first = generate_dataset(&args)?;
        assert_eq!(first.files, 4);
        assert_eq!(first.bytes, 4_096);

        let report = local_copy_benchmark(&source, &destination, 2, false, false).await?;
        assert_eq!(report.benchmark, "local_filesystem_copy");
        assert_eq!(report.files, 4);
        assert_eq!(report.bytes, 4_096);
        assert_eq!(report.results.len(), 3);
        assert!(
            report
                .results
                .iter()
                .all(|result| result.elapsed_seconds.is_finite())
        );
        Ok(())
    }

    #[test]
    fn generation_is_deterministic() -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::tempdir()?;
        let first_root = temporary.path().join("first");
        let second_root = temporary.path().join("second");
        let mut args = GenerateDatasetArgs {
            output: first_root.clone(),
            profile: DatasetProfile::Small,
            scale_percent: 100,
            files: Some(1),
            bytes_per_file: Some(256),
            bytes_per_file_mib: None,
            max_total_bytes: MIB,
            seed: 99,
            force: false,
        };
        generate_dataset(&args)?;
        args.output = second_root.clone();
        generate_dataset(&args)?;
        let relative = Path::new("custom/set-0000/file-00000000.bin");
        assert_eq!(
            fs::read(first_root.join(relative))?,
            fs::read(second_root.join(relative))?
        );
        Ok(())
    }
}
