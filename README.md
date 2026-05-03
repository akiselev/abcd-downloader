# abcd-downloader

Rust library and CLI for downloading selected models from
[ABC: A Big CAD Dataset](https://deep-geometry.github.io/abc-dataset/).

The CLI supports model ids, ranges, exclusions, and whole ABC batches:

```bash
abcd-downloader download 1 2 3-10 '^5' --formats meta,step,stl
abcd-downloader download batch:0 --formats featurescript,step,xt --out ~/git/abcd-data
abcd-downloader purge
```

Archives are cached under the platform XDG cache directory by default and reused
across runs. Extracted files are grouped by model id:

```text
00000050/
  00000050_..._metadata_000.yml
  00000050_..._step_000.step
  Part Studio 1 - Part 1.x_t
```

## Library

```rust
use abcd_downloader::{DownloadOptions, download, parse_formats};

# async fn example() -> anyhow::Result<()> {
let mut options = DownloadOptions::default();
options.model_specs = vec!["batch:0".to_owned()];
options.formats = parse_formats("featurescript,step,xt")?;
options.out_dir = "/tmp/abcd-data".into();

download(options).await?;
# Ok(())
# }
```

## Formats

Supported aliases include `meta`, `step`, `stl`/`stl2`, `para`, `xt`/`x_t`,
`xb`/`x_b`, `obj`, `feat`, `stat`, and `ofs`/`featurescript`.

ABC chunks are large; use `--download-jobs`, `--extract-jobs`, `--jobs`, and
`--rate-limit` to control resource use.
