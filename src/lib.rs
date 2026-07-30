use anyhow::{Context, Result, anyhow, bail};
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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum VerifyMode {
    None,
    Size,
    Md5,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Format {
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

impl ArchiveTask {
    fn planned_event(&self) -> DownloadEvent {
        DownloadEvent::Planned {
            chunk: self.chunk,
            archive_format: self.archive_format,
            model_count: self.model_ids.len(),
            archive_name: self.archive_name.clone(),
        }
    }
}

#[derive(Debug, Clone)]
struct ArchiveMeta {
    sizes: HashMap<String, u64>,
    md5s: HashMap<String, String>,
}

#[derive(Clone)]
pub struct DownloadOptions {
    pub model_specs: Vec<String>,
    pub formats: BTreeSet<Format>,
    pub out_dir: PathBuf,
    pub cache_dir: Option<PathBuf>,
    pub jobs: usize,
    pub download_jobs: usize,
    pub extract_jobs: Option<usize>,
    pub version: String,
    pub verify: VerifyMode,
    pub rate_limit: Option<u64>,
    pub overwrite: bool,
    pub dry_run: bool,
    pub reporter: Option<Arc<dyn DownloadReporter>>,
}

impl Default for DownloadOptions {
    fn default() -> Self {
        Self {
            model_specs: Vec::new(),
            formats: parse_formats("meta,step,stl").expect("default formats are valid"),
            out_dir: PathBuf::from("."),
            cache_dir: None,
            jobs: default_jobs(),
            download_jobs: 2,
            extract_jobs: None,
            version: "v00".to_owned(),
            verify: VerifyMode::Size,
            rate_limit: None,
            overwrite: false,
            dry_run: false,
            reporter: None,
        }
    }
}

pub trait DownloadReporter: Send + Sync {
    fn report(&self, event: DownloadEvent);
}

#[derive(Clone, Debug)]
pub enum DownloadEvent {
    AllExisting,
    Planned {
        chunk: usize,
        archive_format: &'static str,
        model_count: usize,
        archive_name: String,
    },
    UsingCached {
        archive_name: String,
    },
    Downloading {
        archive_name: String,
    },
    Extracting {
        archive_name: String,
    },
}

pub async fn download(options: DownloadOptions) -> Result<()> {
    if options.model_specs.is_empty() {
        bail!("provide model ids/ranges, for example: abcd-downloader download 1 2 3-10 ^5");
    }

    let reporter = options.reporter.clone();
    let ids = parse_model_specs(&options.model_specs)?;
    let cache_dir = archive_cache_dir(options.cache_dir.clone())?;
    let extract_jobs = options.extract_jobs.unwrap_or(options.jobs).max(1);
    let out = options.out_dir;

    let formats = options.formats;

    fs::create_dir_all(&out).with_context(|| format!("create {}", out.display()))?;
    fs::create_dir_all(&cache_dir).with_context(|| format!("create {}", cache_dir.display()))?;

    let client = Client::builder().build()?;
    let archive_formats = archive_formats(&formats);

    let manifests = fetch_manifests(&client, &options.version, &archive_formats).await?;
    let meta = match options.verify {
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

    let tasks = build_tasks(&ids, &formats, &manifests, &out, options.overwrite)?;
    if tasks.is_empty() {
        report(reporter.as_deref(), DownloadEvent::AllExisting);
        return Ok(());
    }

    if options.dry_run {
        for task in &tasks {
            report(reporter.as_deref(), task.planned_event());
        }
        return Ok(());
    }

    let download_sem = Arc::new(Semaphore::new(options.download_jobs.max(1)));
    let extract_sem = Arc::new(Semaphore::new(extract_jobs));
    let meta = Arc::new(meta);
    let cache_dir = Arc::new(cache_dir);
    let out = Arc::new(out);
    let client = Arc::new(client);

    stream::iter(tasks)
        .map(Ok::<_, anyhow::Error>)
        .try_for_each_concurrent(options.jobs.max(1), |task| {
            let client = Arc::clone(&client);
            let cache_dir = Arc::clone(&cache_dir);
            let out = Arc::clone(&out);
            let meta = Arc::clone(&meta);
            let download_sem = Arc::clone(&download_sem);
            let extract_sem = Arc::clone(&extract_sem);
            let verify = options.verify;
            let overwrite = options.overwrite;
            let rate_limit = options.rate_limit;
            let reporter = reporter.clone();
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
                        reporter.as_deref(),
                    )
                    .await?;
                }

                {
                    let _permit = extract_sem.acquire().await?;
                    let task_clone = task.clone();
                    let archive_clone = archive_path.clone();
                    let out_clone = Arc::clone(&out);
                    let extract_reporter = reporter.clone();
                    tokio::task::spawn_blocking(move || {
                        extract_archive(
                            &archive_clone,
                            &out_clone,
                            &task_clone,
                            overwrite,
                            extract_reporter.as_deref(),
                        )
                    })
                    .await??;
                }

                Ok(())
            }
        })
        .await?;

    Ok(())
}

pub fn purge_cache(cache_dir: Option<PathBuf>) -> Result<PathBuf> {
    let dir = archive_cache_dir(cache_dir)?;
    if dir.exists() {
        fs::remove_dir_all(&dir).with_context(|| format!("remove {}", dir.display()))?;
    }
    Ok(dir)
}

fn report(reporter: Option<&dyn DownloadReporter>, event: DownloadEvent) {
    if let Some(reporter) = reporter {
        reporter.report(event);
    }
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
    let text = client
        .get(&url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    serde_yaml::from_str(&text).with_context(|| format!("parse {file}"))
}

async fn fetch_yaml_map_string(client: &Client, file: &str) -> Result<HashMap<String, String>> {
    let url = format!("{BASE_URL}/{file}");
    let text = client
        .get(&url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
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
    reporter: Option<&dyn DownloadReporter>,
) -> Result<()> {
    if archive_path.exists() && archive_ok(archive_path, &task.archive_name, verify, meta)? {
        report(
            reporter,
            DownloadEvent::UsingCached {
                archive_name: task.archive_name.clone(),
            },
        );
        return Ok(());
    }

    report(
        reporter,
        DownloadEvent::Downloading {
            archive_name: task.archive_name.clone(),
        },
    );
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
    reporter: Option<&dyn DownloadReporter>,
) -> Result<()> {
    report(
        reporter,
        DownloadEvent::Extracting {
            archive_name: task.archive_name.clone(),
        },
    );
    let wanted_ids: HashSet<usize> = task.model_ids.iter().copied().collect();
    let wanted_formats: HashSet<Format> = task.formats.iter().copied().collect();

    decompress_file_with_extract_fn(archive_path, out, |entry, reader, _dest| {
        extract_entry(entry, reader, out, &wanted_ids, &wanted_formats, overwrite)
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

fn unzip_para(
    zip_path: &Path,
    model_dir: &Path,
    formats: &[Format],
    overwrite: bool,
) -> Result<()> {
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
        let is_xt = extension_eq(&name, "x_t");
        let is_xb = extension_eq(&name, "x_b");
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

pub fn parse_model_specs(specs: &[String]) -> Result<BTreeSet<usize>> {
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

    if let Some(chunk_spec) = spec.strip_prefix("chunk:") {
        return parse_chunk_spec(chunk_spec);
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

fn parse_chunk_spec(spec: &str) -> Result<Vec<usize>> {
    let chunks = if let Some((start, end)) = spec.split_once('-') {
        let start = parse_id(start)?;
        let end = parse_id(end)?;
        if start > end {
            bail!("chunk range start is greater than end: chunk:{spec}");
        }
        (start..=end).collect()
    } else {
        vec![parse_id(spec)?]
    };

    let mut ids = Vec::with_capacity(chunks.len() * CHUNK_SIZE);
    for chunk in chunks {
        let start = chunk
            .checked_mul(CHUNK_SIZE)
            .ok_or_else(|| anyhow!("chunk is too large: {chunk}"))?;
        let end = start
            .checked_add(CHUNK_SIZE - 1)
            .ok_or_else(|| anyhow!("chunk is too large: {chunk}"))?;
        ids.extend(start..=end);
    }
    Ok(ids)
}

fn parse_id(spec: &str) -> Result<usize> {
    spec.parse()
        .with_context(|| format!("invalid model id: {spec}"))
}

pub fn parse_formats(input: &str) -> Result<BTreeSet<Format>> {
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
    formats
        .iter()
        .map(|format| format.archive_format())
        .collect()
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
            Format::Xt => Path::new(name)
                .extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| extension.eq_ignore_ascii_case("x_t")),
            Format::Xb => Path::new(name)
                .extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| extension.eq_ignore_ascii_case("x_b")),
            _ => self.matches_entry(name),
        }
    }

    fn unzip_para(self) -> bool {
        matches!(self, Format::Xt | Format::Xb)
    }
}

fn extension_eq(path: &Path, expected: &str) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case(expected))
}

fn entry_model_id(name: &str) -> Option<usize> {
    let file_name = Path::new(name).file_name()?.to_string_lossy();
    let prefix = file_name.get(0..8)?;
    prefix.parse().ok()
}

pub fn parse_bytes(input: &str) -> Result<u64, String> {
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

    let dirs = ProjectDirs::from("", "", "abcd-downloader")
        .ok_or_else(|| anyhow!("could not determine XDG cache directory"))?;
    Ok(dirs.cache_dir().join("archives"))
}

pub fn default_jobs() -> usize {
    std::thread::available_parallelism().map_or(4, usize::from)
}

pub fn format_descriptions() -> &'static [(&'static str, &'static str)] {
    &[
        ("meta", "metadata yml"),
        ("step", "STEP boundary representation"),
        ("stl, stl2", "merged STL mesh from official stl2 chunks"),
        ("para", "Parasolid zip from ABC para chunks"),
        ("xt, x_t", "extracted Parasolid text parts from para zip"),
        (
            "xb, x_b",
            "extracted Parasolid binary parts from para zip when present",
        ),
        ("obj", "mesh OBJ"),
        ("feat", "feature yml"),
        ("stat", "statistics yml"),
        ("ofs", "FeatureScript yml"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parasolid_member_extensions_are_case_insensitive() {
        assert!(extension_eq(Path::new("part.x_t"), "x_t"));
        assert!(extension_eq(Path::new("part.X_T"), "x_t"));
        assert!(extension_eq(Path::new("part.x_b"), "x_b"));
        assert!(extension_eq(Path::new("part.X_B"), "x_b"));
        assert!(Format::Xt.matches_output_name("part.X_T"));
        assert!(Format::Xb.matches_output_name("part.X_B"));
    }
}
