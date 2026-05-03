use std::{path::PathBuf, sync::Arc};

use abcd_downloader::{
    DownloadEvent, DownloadOptions, DownloadReporter, VerifyMode, default_jobs, download,
    format_descriptions, parse_bytes, parse_formats, purge_cache,
};
use anyhow::Result;
use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(name = "abcd-downloader")]
#[command(about = "Download selected models from ABC: A Big CAD Dataset")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Download(DownloadArgs),
    ListFormats,
    Purge(PurgeArgs),
}

#[derive(Args, Clone)]
struct DownloadArgs {
    #[arg(value_name = "MODEL_SPEC")]
    specs: Vec<String>,

    #[arg(short, long, value_name = "LIST", default_value = "meta,step,stl")]
    formats: String,

    #[arg(short, long, value_name = "DIR", default_value = ".")]
    out: PathBuf,

    #[arg(long, value_name = "DIR")]
    cache_dir: Option<PathBuf>,

    #[arg(long, default_value_t = default_jobs())]
    jobs: usize,

    #[arg(long, default_value_t = 2)]
    download_jobs: usize,

    #[arg(long)]
    extract_jobs: Option<usize>,

    #[arg(long, default_value = "v00")]
    version: String,

    #[arg(long, value_enum, default_value_t = CliVerifyMode::Size)]
    verify: CliVerifyMode,

    #[arg(long, value_name = "BYTES_PER_SEC", value_parser = parse_bytes)]
    rate_limit: Option<u64>,

    #[arg(long)]
    overwrite: bool,

    #[arg(long)]
    dry_run: bool,
}

#[derive(Args)]
struct PurgeArgs {
    #[arg(long, value_name = "DIR")]
    cache_dir: Option<PathBuf>,
}

#[derive(Copy, Clone, ValueEnum, Eq, PartialEq)]
enum CliVerifyMode {
    None,
    Size,
    Md5,
}

impl From<CliVerifyMode> for VerifyMode {
    fn from(value: CliVerifyMode) -> Self {
        match value {
            CliVerifyMode::None => Self::None,
            CliVerifyMode::Size => Self::Size,
            CliVerifyMode::Md5 => Self::Md5,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Download(args) => {
            let options = DownloadOptions {
                model_specs: args.specs,
                formats: parse_formats(&args.formats)?,
                out_dir: args.out,
                cache_dir: args.cache_dir,
                jobs: args.jobs,
                download_jobs: args.download_jobs,
                extract_jobs: args.extract_jobs,
                version: args.version,
                verify: args.verify.into(),
                rate_limit: args.rate_limit,
                overwrite: args.overwrite,
                dry_run: args.dry_run,
                reporter: Some(Arc::new(StdoutReporter)),
            };
            download(options).await
        }
        Command::ListFormats => {
            for (name, description) in format_descriptions() {
                println!("{name:<15} {description}");
            }
            Ok(())
        }
        Command::Purge(args) => {
            let dir = purge_cache(args.cache_dir)?;
            println!("removed {}", dir.display());
            Ok(())
        }
    }
}

struct StdoutReporter;

impl DownloadReporter for StdoutReporter {
    fn report(&self, event: DownloadEvent) {
        match event {
            DownloadEvent::AllExisting => println!("all requested files already exist"),
            DownloadEvent::Planned {
                chunk,
                archive_format,
                model_count,
                archive_name,
            } => println!(
                "chunk {chunk:04} {archive_format:<5} {model_count} models -> {archive_name}"
            ),
            DownloadEvent::UsingCached { archive_name } => println!("using cached {archive_name}"),
            DownloadEvent::Downloading { archive_name } => println!("downloading {archive_name}"),
            DownloadEvent::Extracting { archive_name } => println!("extracting {archive_name}"),
        }
    }
}
