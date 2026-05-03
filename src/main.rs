use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use directories::ProjectDirs;
use futures_util::{StreamExt, TryStreamExt, stream};
use reqwest::Client;
use sevenz_rust2::{ArchiveEntry, decompress_file_with_extract_fn};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fs::{self, File},
    io::{self, Read},
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};
use tokio::sync::Semaphore;

const BASE_URL: &str = "https://deep-geometry.github.io/abc-dataset/data";
const CHUNK_SIZE: usize = 10_000;

#[derive(Parser)]
#[command(name = "abcd")]
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

    #[arg(long, value_enum, default_value_t = VerifyMode::Size)]
    verify: VerifyMode,

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
enum VerifyMode {
    None,
    Size,
    Md5,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
enum Format {
    Meta,
    Step,
    Para,
    Xt,
    Xb,
    Stl2,
    Obj,
    Feat,
    Stat,
    Ofs,
}

#[derive(Debug, Clone)]
struct ManifestEntry {
    url: String,
    archive_name: String,
}

#[derive(Debug, Clone)]
struct ArchiveTask {
    archive_format: &'static str,
    chunk: usize,
    archive_name: String,
    url: String,
    formats: Vec<Format>,
    model_ids: Vec<usize>,
}

#[derive(Debug, Clone)]
struct ArchiveMeta {
    sizes: HashMap<String, u64>,
    md5s: HashMap<String, String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Download(args) => download(args).await,
        Command::ListFormats => {
            list_formats();
            Ok(())
        }
        Command::Purge(args) => {
            let dir = archive_cache_dir(args.cache_dir)?;
            if dir.exists() {
                fs::remove_dir_all(&dir).with_context(|| format!("remove {}", dir.display()))?;
            }
            println!("removed {}", dir.display());
            Ok(())
        }
    }
}

async fn download(args: DownloadArgs) -> Result<()> {
    if args.specs.is_empty() {
        bail!("provide model ids/ranges, for example: abcd download 1 2 3-10 ^5");
    }

    let formats = parse_formats(&args.formats)?;
    let ids = parse_model_specs(&args.specs)?;
    let cache_dir = archive_cache_dir(args.cache_dir.clone())?;
    let extract_jobs = args.extract_jobs.unwrap_or(args.jobs).max(1);
    let out = args.out;

    fs::create_dir_all(&out).with_context(|| format!("create {}", out.display()))?;
    fs::create_dir_all(&cache_dir).with_context(|| format!("create {}", cache_dir.display()))?;

    let client = Client::builder().build()?;
    let archive_formats = archive_formats(&formats);

    let manifests = fetch_manifests(&client, &args.version, &archive_formats).await?;
    let meta = match args.verify {
        VerifyMode::None => ArchiveMeta {
            sizes: HashMap::new(),
            md5s: HashMap::new(),
        },
        VerifyMode::Size => ArchiveMeta {
            sizes: fetch_yaml_map_u64(&client, "size.yml").await?,
            md5s: HashMap::new(),
        },
        VerifyMode::Md5 => ArchiveMeta {
            sizes: HashMap::new(),
            md5s: fetch_yaml_map_string(&client, "md5.yml").await?,
        },
    };

    let tasks = build_tasks(&ids, &formats, &manifests, &out, args.overwrite)?;
    if tasks.is_empty() {
        println!("all requested files already exist");
        return Ok(());
    }

    if args.dry_run {
        for task in &tasks {
            println!(
                "chunk {:04} {:<5} {} models -> {}",
                task.chunk,
                task.archive_format,
                task.model_ids.len(),
                task.archive_name
            );
        }
        return Ok(());
    }

    let download_sem = Arc::new(Semaphore::new(args.download_jobs.max(1)));
    let extract_sem = Arc::new(Semaphore::new(extract_jobs));
    let meta = Arc::new(meta);
    let cache_dir = Arc::new(cache_dir);
    let out = Arc::new(out);
    let client = Arc::new(client);

    stream::iter(tasks)
        .map(Ok::<_, anyhow::Error>)
        .try_for_each_concurrent(args.jobs.max(1), |task| {
            let client = Arc::clone(&client);
            let cache_dir = Arc::clone(&cache_dir);
            let out = Arc::clone(&out);
            let meta = Arc::clone(&meta);
            let download_sem = Arc::clone(&download_sem);
            let extract_sem = Arc::clone(&extract_sem);
            let verify = args.verify;
            let overwrite = args.overwrite;
            let rate_limit = args.rate_limit;
            async move {
                let archive_path = cache_dir.join(&task.archive_name);
                {
                    let _permit = download_sem.acquire().await?;
                    download_archive(
                        &client,
                        &task,
                        &archive_path,
                        verify,
                        &meta,
                        rate_limit,
                    )
                    .await?;
                }

                {
                    let _permit = extract_sem.acquire().await?;
                    let task_clone = task.clone();
                    let archive_clone = archive_path.clone();
                    let out_clone = Arc::clone(&out);
                    tokio::task::spawn_blocking(move || {
                        extract_archive(&archive_clone, &out_clone, &task_clone, overwrite)
                    })
                    .await??;
                }

                Ok(())
            }
        })
        .await?;

    Ok(())
}

async fn fetch_manifests(
    client: &Client,
    version: &str,
    archive_formats: &BTreeSet<&'static str>,
) -> Result<HashMap<&'static str, Vec<ManifestEntry>>> {
    let mut out = HashMap::new();
    for format in archive_formats {
        let url = format!("{BASE_URL}/{format}_{version}.txt");
        let text = client
            .get(&url)
            .send()
            .await?
            .error_for_status()
            .with_context(|| format!("fetch manifest {url}"))?
            .text()
            .await?;
        let tokens: Vec<_> = text.split_whitespace().collect();
        if tokens.len() % 2 != 0 {
            bail!("manifest {url} has an odd number of fields");
        }
        let entries = tokens
            .chunks_exact(2)
            .map(|pair| ManifestEntry {
                url: pair[0].to_owned(),
                archive_name: pair[1].to_owned(),
            })
            .collect();
        out.insert(*format, entries);
    }
    Ok(out)
}

async fn fetch_yaml_map_u64(client: &Client, file: &str) -> Result<HashMap<String, u64>> {
    let url = format!("{BASE_URL}/{file}");
    let text = client.get(&url).send().await?.error_for_status()?.text().await?;
    serde_yaml::from_str(&text).with_context(|| format!("parse {file}"))
}

async fn fetch_yaml_map_string(client: &Client, file: &str) -> Result<HashMap<String, String>> {
    let url = format!("{BASE_URL}/{file}");
    let text = client.get(&url).send().await?.error_for_status()?.text().await?;
    serde_yaml::from_str(&text).with_context(|| format!("parse {file}"))
}

fn build_tasks(
    ids: &BTreeSet<usize>,
    formats: &BTreeSet<Format>,
    manifests: &HashMap<&'static str, Vec<ManifestEntry>>,
    out: &Path,
    overwrite: bool,
) -> Result<Vec<ArchiveTask>> {
    let mut grouped: BTreeMap<(&'static str, usize), (BTreeSet<Format>, BTreeSet<usize>)> =
        BTreeMap::new();

    for id in ids {
        for format in formats {
            if !overwrite && model_has_format(out, *id, *format) {
                continue;
            }
            grouped
                .entry((format.archive_format(), id / CHUNK_SIZE))
                .or_default()
                .0
                .insert(*format);
            grouped
                .entry((format.archive_format(), id / CHUNK_SIZE))
                .or_default()
                .1
                .insert(*id);
        }
    }

    grouped
        .into_iter()
        .map(|((archive_format, chunk), (formats, ids))| {
            let manifest = manifests
                .get(archive_format)
                .ok_or_else(|| anyhow!("missing manifest for {archive_format}"))?;
            let entry = manifest
                .get(chunk)
                .ok_or_else(|| anyhow!("no {archive_format} chunk {chunk:04} in manifest"))?;
            Ok(ArchiveTask {
                archive_format,
                chunk,
                archive_name: entry.archive_name.clone(),
                url: entry.url.clone(),
                formats: formats.into_iter().collect(),
                model_ids: ids.into_iter().collect(),
            })
        })
        .collect()
}

async fn download_archive(
    client: &Client,
    task: &ArchiveTask,
    archive_path: &Path,
    verify: VerifyMode,
    meta: &ArchiveMeta,
    rate_limit: Option<u64>,
) -> Result<()> {
    if archive_path.exists() && archive_ok(archive_path, &task.archive_name, verify, meta)? {
        println!("using cached {}", task.archive_name);
        return Ok(());
    }

    println!("downloading {}", task.archive_name);
    let part_path = archive_path.with_extension("7z.part");
    let response = client
        .get(&task.url)
        .send()
        .await?
        .error_for_status()
        .with_context(|| format!("download {}", task.url))?;
    let mut file = tokio::fs::File::create(&part_path).await?;
    let started = Instant::now();
    let mut written = 0_u64;

    let mut body = response.bytes_stream();
    while let Some(chunk) = body.next().await {
        let chunk = chunk?;
        tokio::io::AsyncWriteExt::write_all(&mut file, &chunk).await?;
        written += chunk.len() as u64;
        if let Some(limit) = rate_limit.filter(|limit| *limit > 0) {
            let expected = written as f64 / limit as f64;
            let elapsed = started.elapsed().as_secs_f64();
            if expected > elapsed {
                tokio::time::sleep(std::time::Duration::from_secs_f64(expected - elapsed)).await;
            }
        }
    }
    tokio::io::AsyncWriteExt::flush(&mut file).await?;
    drop(file);

    tokio::fs::rename(&part_path, archive_path).await?;
    if !archive_ok(archive_path, &task.archive_name, verify, meta)? {
        let _ = tokio::fs::remove_file(archive_path).await;
        bail!("verification failed for {}", task.archive_name);
    }

    Ok(())
}

fn archive_ok(
    path: &Path,
    archive_name: &str,
    verify: VerifyMode,
    meta: &ArchiveMeta,
) -> Result<bool> {
    match verify {
        VerifyMode::None => Ok(path.exists()),
        VerifyMode::Size => {
            let Some(expected) = meta.sizes.get(archive_name) else {
                return Ok(path.exists());
            };
            Ok(path.metadata()?.len() == *expected)
        }
        VerifyMode::Md5 => {
            let Some(expected) = meta.md5s.get(archive_name) else {
                return Ok(path.exists());
            };
            let bytes = fs::read(path)?;
            let digest = format!("{:x}", md5::compute(bytes));
            Ok(digest == *expected)
        }
    }
}

fn extract_archive(
    archive_path: &Path,
    out: &Path,
    task: &ArchiveTask,
    overwrite: bool,
) -> Result<()> {
    println!("extracting {}", task.archive_name);
    let wanted_ids: HashSet<usize> = task.model_ids.iter().copied().collect();
    let wanted_formats: HashSet<Format> = task.formats.iter().copied().collect();

    decompress_file_with_extract_fn(archive_path, out, |entry, reader, _dest| {
        extract_entry(
            entry,
            reader,
            out,
            &wanted_ids,
            &wanted_formats,
            overwrite,
        )
        .map_err(|err| sevenz_rust2::Error::from(io::Error::other(err.to_string())))
    })
    .with_context(|| format!("extract {}", archive_path.display()))?;

    Ok(())
}

fn extract_entry(
    entry: &ArchiveEntry,
    reader: &mut dyn Read,
    out: &Path,
    wanted_ids: &HashSet<usize>,
    wanted_formats: &HashSet<Format>,
    overwrite: bool,
) -> Result<bool> {
    if entry.is_directory() {
        return Ok(true);
    }

    let Some(model_id) = entry_model_id(entry.name()) else {
        io::copy(reader, &mut io::sink())?;
        return Ok(true);
    };

    let matching_formats: Vec<_> = wanted_formats
        .iter()
        .copied()
        .filter(|format| format.matches_entry(entry.name()))
        .collect();

    if !wanted_ids.contains(&model_id) || matching_formats.is_empty() {
        io::copy(reader, &mut io::sink())?;
        return Ok(true);
    }

    let model_dir = out.join(format!("{model_id:08}"));
    fs::create_dir_all(&model_dir)?;
    let file_name = Path::new(entry.name())
        .file_name()
        .ok_or_else(|| anyhow!("entry has no filename: {}", entry.name()))?;
    let target = model_dir.join(file_name);

    if target.exists() && !overwrite {
        io::copy(reader, &mut io::sink())?;
    } else {
        let mut file = File::create(&target)?;
        io::copy(reader, &mut file)?;
    }

    if matching_formats.iter().any(|format| format.unzip_para()) {
        unzip_para(&target, &model_dir, &matching_formats, overwrite)?;
        if !wanted_formats.contains(&Format::Para) {
            let _ = fs::remove_file(&target);
        }
    }

    Ok(true)
}

fn unzip_para(zip_path: &Path, model_dir: &Path, formats: &[Format], overwrite: bool) -> Result<()> {
    let want_xt = formats.contains(&Format::Xt);
    let want_xb = formats.contains(&Format::Xb);
    if !want_xt && !want_xb {
        return Ok(());
    }

    let file = File::open(zip_path)?;
    let mut archive = zip::ZipArchive::new(file)?;
    for index in 0..archive.len() {
        let mut zipped = archive.by_index(index)?;
        let Some(name) = zipped.enclosed_name().map(PathBuf::from) else {
            continue;
        };
        let is_xt = name.extension().is_some_and(|ext| ext == "x_t");
        let is_xb = name.extension().is_some_and(|ext| ext == "x_b");
        if (!want_xt || !is_xt) && (!want_xb || !is_xb) {
            continue;
        }
        let target = model_dir.join(name.file_name().unwrap_or_default());
        if target.exists() && !overwrite {
            continue;
        }
        let mut out = File::create(target)?;
        io::copy(&mut zipped, &mut out)?;
    }
    Ok(())
}

fn model_has_format(out: &Path, id: usize, format: Format) -> bool {
    let dir = out.join(format!("{id:08}"));
    if !dir.is_dir() {
        return false;
    }

    let Ok(entries) = fs::read_dir(dir) else {
        return false;
    };
    entries.flatten().any(|entry| {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        format.matches_entry(&name) || format.matches_output_name(&name)
    })
}

fn parse_model_specs(specs: &[String]) -> Result<BTreeSet<usize>> {
    let mut include = BTreeSet::new();
    let mut exclude = BTreeSet::new();

    for raw in specs {
        let (is_exclude, spec) = raw
            .strip_prefix('^')
            .map_or((false, raw.as_str()), |stripped| (true, stripped));
        let parsed = parse_id_or_range(spec)?;
        if is_exclude {
            exclude.extend(parsed);
        } else {
            include.extend(parsed);
        }
    }

    for id in exclude {
        include.remove(&id);
    }
    Ok(include)
}

fn parse_id_or_range(spec: &str) -> Result<Vec<usize>> {
    if spec.is_empty() {
        bail!("empty model spec");
    }

    if let Some(batch_spec) = spec.strip_prefix("batch:") {
        return parse_batch_spec(batch_spec);
    }

    if let Some((start, end)) = spec.split_once('-') {
        let start = parse_id(start)?;
        let end = parse_id(end)?;
        if start > end {
            bail!("range start is greater than end: {spec}");
        }
        Ok((start..=end).collect())
    } else {
        Ok(vec![parse_id(spec)?])
    }
}

fn parse_batch_spec(spec: &str) -> Result<Vec<usize>> {
    let batches = if let Some((start, end)) = spec.split_once('-') {
        let start = parse_id(start)?;
        let end = parse_id(end)?;
        if start > end {
            bail!("batch range start is greater than end: batch:{spec}");
        }
        (start..=end).collect()
    } else {
        vec![parse_id(spec)?]
    };

    let mut ids = Vec::with_capacity(batches.len() * CHUNK_SIZE);
    for batch in batches {
        let start = batch
            .checked_mul(CHUNK_SIZE)
            .ok_or_else(|| anyhow!("batch is too large: {batch}"))?;
        let end = start
            .checked_add(CHUNK_SIZE - 1)
            .ok_or_else(|| anyhow!("batch is too large: {batch}"))?;
        ids.extend(start..=end);
    }
    Ok(ids)
}

fn parse_id(spec: &str) -> Result<usize> {
    spec.parse()
        .with_context(|| format!("invalid model id: {spec}"))
}

fn parse_formats(input: &str) -> Result<BTreeSet<Format>> {
    input
        .split(',')
        .map(|part| {
            let normalized = part.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "meta" | "metadata" | "yml" => Ok(Format::Meta),
                "step" | "stp" => Ok(Format::Step),
                "para" | "parasolid" => Ok(Format::Para),
                "xt" | "x_t" => Ok(Format::Xt),
                "xb" | "x_b" => Ok(Format::Xb),
                "stl" | "stl2" => Ok(Format::Stl2),
                "obj" => Ok(Format::Obj),
                "feat" | "feature" | "features" => Ok(Format::Feat),
                "stat" | "stats" | "statistics" => Ok(Format::Stat),
                "ofs" | "script" | "featurescript" | "feature-script" => Ok(Format::Ofs),
                "" => bail!("empty format in list"),
                _ => bail!("unknown format: {part}"),
            }
        })
        .collect()
}

fn archive_formats(formats: &BTreeSet<Format>) -> BTreeSet<&'static str> {
    formats.iter().map(|format| format.archive_format()).collect()
}

impl Format {
    fn archive_format(self) -> &'static str {
        match self {
            Format::Meta => "meta",
            Format::Step => "step",
            Format::Para | Format::Xt | Format::Xb => "para",
            Format::Stl2 => "stl2",
            Format::Obj => "obj",
            Format::Feat => "feat",
            Format::Stat => "stat",
            Format::Ofs => "ofs",
        }
    }

    fn matches_entry(self, name: &str) -> bool {
        match self {
            Format::Meta => name.contains("_metadata_") && name.ends_with(".yml"),
            Format::Step => name.contains("_step_") && name.ends_with(".step"),
            Format::Para | Format::Xt | Format::Xb => {
                name.contains("_parasolid_") && name.ends_with(".zip")
            }
            Format::Stl2 => name.contains("_trimesh_") && name.ends_with(".stl"),
            Format::Obj => name.contains("_trimesh_") && name.ends_with(".obj"),
            Format::Feat => name.contains("_features_") && name.ends_with(".yml"),
            Format::Stat => name.contains("_stats_") && name.ends_with(".yml"),
            Format::Ofs => name.contains("_featurescript_") && name.ends_with(".yml"),
        }
    }

    fn matches_output_name(self, name: &str) -> bool {
        match self {
            Format::Xt => name.ends_with(".x_t"),
            Format::Xb => name.ends_with(".x_b"),
            _ => self.matches_entry(name),
        }
    }

    fn unzip_para(self) -> bool {
        matches!(self, Format::Xt | Format::Xb)
    }
}

fn entry_model_id(name: &str) -> Option<usize> {
    let file_name = Path::new(name).file_name()?.to_string_lossy();
    let prefix = file_name.get(0..8)?;
    prefix.parse().ok()
}

fn parse_bytes(input: &str) -> Result<u64, String> {
    let input = input.trim();
    let number_end = input
        .find(|ch: char| !ch.is_ascii_digit() && ch != '.')
        .unwrap_or(input.len());
    let (number, suffix) = input.split_at(number_end);
    let number: f64 = number
        .parse()
        .map_err(|_| format!("invalid byte rate: {input}"))?;
    let multiplier = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1.0,
        "k" | "kb" | "kib" => 1024.0,
        "m" | "mb" | "mib" => 1024.0 * 1024.0,
        "g" | "gb" | "gib" => 1024.0 * 1024.0 * 1024.0,
        _ => return Err(format!("invalid byte suffix in {input}")),
    };
    Ok((number * multiplier) as u64)
}

fn archive_cache_dir(cache_dir: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(cache_dir) = cache_dir {
        return Ok(cache_dir);
    }

    let dirs = ProjectDirs::from("", "", "abcd")
        .ok_or_else(|| anyhow!("could not determine XDG cache directory"))?;
    Ok(dirs.cache_dir().join("archives"))
}

fn default_jobs() -> usize {
    std::thread::available_parallelism().map_or(4, usize::from)
}

fn list_formats() {
    println!("meta            metadata yml");
    println!("step            STEP boundary representation");
    println!("stl, stl2       merged STL mesh from official stl2 chunks");
    println!("para            Parasolid zip from ABC para chunks");
    println!("xt, x_t         extracted Parasolid text parts from para zip");
    println!("xb, x_b         extracted Parasolid binary parts from para zip when present");
    println!("obj             mesh OBJ");
    println!("feat            feature yml");
    println!("stat            statistics yml");
    println!("ofs             FeatureScript yml");
}
