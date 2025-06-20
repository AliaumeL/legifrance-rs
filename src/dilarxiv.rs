use clap::Parser;

use anyhow::{Context, Result};
use std::path::PathBuf;

use indicatif::{ProgressBar, ProgressStyle};

use log::{error, info, warn};

use legifrance::dumps::extractor::{count_tags_in_file, parse_file};
use legifrance::dumps::fonds::{FONDS, Fond};
use legifrance::dumps::tarballs;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    /// Whether to download the tarballs
    #[clap(short, long, default_value = "false")]
    tarballs: bool,

    /// The list of tarballs to download,
    /// the default (empty) list will download all tarballs
    ///
    #[clap(short, long, num_args(0..))]
    fond: Vec<Fond>,

    /// Whether to extract the tarballs
    #[clap(short, long, default_value = "false")]
    extract: bool,

    /// Whether to index the extracted content
    #[clap(short, long, default_value = "false")]
    index: bool,

    /// The path to the index directory
    #[clap(short, long)]
    query: Option<String>,

    /// Whether to save *all* the search results in a file
    #[clap(short, long)]
    save: Option<String>,

    /// Read a result list (one line per file) and create
    /// a CSV with the correct metadata
    #[clap(short, long)]
    csv: Option<String>,
}

async fn get_tarballs(fonds: &[Fond], dir: &PathBuf) -> Result<Vec<String>> {
    // Create a new HTTP client
    let client = reqwest::Client::new();

    let mut tarballs = Vec::new();

    for fond in fonds {
        info!("Downloading tarballs for {}", fond);
        let url = format!("{}/{}", tarballs::BASE_URL, fond);
        // Download the tarballs
        match tarballs::download_tarballs(&client, dir, &url).await {
            Ok(tarballs_list) => {
                tarballs.extend(tarballs_list);
            }
            Err(e) => {
                error!("Error fetching tarballs: {}", e);
                continue;
            }
        }
    }

    Ok(tarballs)
}

fn extract_tarballs(idir: &PathBuf, odir: &PathBuf) -> Result<()> {
    // Extract all tarballs
    let tbfiles = std::fs::read_dir(idir).expect("Could not read directory for tarballs");

    let to_extract: Vec<_> = tbfiles
        .into_iter()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            let name = path.file_name()?.to_str()?;
            let ext = path.extension()?.to_str()?;
            if path.is_file() && (ext == "tar.gz" || ext == "gz") {
                Some(name.to_string())
            } else {
                None
            }
        })
        .collect();

    let pb = ProgressBar::new(to_extract.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{msg} [{elapsed_precise}] {wide_bar} {pos}/{len} ({eta})")
            .context("Error creating progress bar")?
            .progress_chars("##-"),
    );

    for p in to_extract {
        pb.set_message(format!("Extracting {}", p));
        let path = idir.join(p);
        if path.exists() {
            match tarballs::extract_tarball(&path, &odir) {
                Ok(_) => info!("Successfully extracted {:?}", path),
                Err(e) => error!("Error extracting {:?}: {}", path, e),
            }
        } else {
            warn!("Tarball {:?} does not exist", path);
        }
        pb.inc(1);
    }
    Ok(())
}

fn result_file_to_csv(result_file: &str, output_file: &str) -> Result<()> {
    use std::io::BufRead;

    let file = std::fs::File::open(result_file)?;
    let reader = std::io::BufReader::new(file);
    let mut writer = csv::WriterBuilder::new()
        .has_headers(true)
        .from_path(output_file)?;

    let mut tcount = std::collections::HashMap::new();
    let mut buffer = String::new();

    for line in reader.lines() {
        let line = line?;
        count_tags_in_file(&line, &mut tcount);
        let content = parse_file(&line, &mut buffer);
        writer.serialize(content)?;
        buffer.clear();
    }
    writer.flush()?;

    println!("Found {} tags", tcount.len());
    for (tag, count) in tcount {
        println!("{}: {}", tag, count);
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    // Initialize the logger
    env_logger::init();

    let args = Cli::parse();

    let cwd = std::env::current_dir().expect("Failed to get current directory");

    let dir = cwd.join("tarballs");

    let edir = cwd.join("extracted");

    let index_path = cwd.join("index");

    if !index_path.exists() {
        std::fs::create_dir_all(&index_path).expect("Failed to create index directory");
    }

    if args.tarballs {
        let fonds = if args.fond.is_empty() {
            FONDS
        } else {
            &args.fond
        };
        let _ = get_tarballs(fonds, &dir)
            .await
            .expect("Failed to get tarballs");
    }

    if args.extract {
        extract_tarballs(&dir, &edir).expect("Could not extract all tarballs");
    }

    let (index, flds) = tarballs::init_tantivy(&index_path).expect("Failed to create index");

    if args.index {
        info!("Creating index at {}", index_path.display());

        let mut writer = index.writer(50_000_000).expect("Failed to create writer");
        tarballs::index_files_in_dir(&mut writer, &flds, &edir).expect("Failed to index files");
    }

    if let Some(query) = args.query {
        match tarballs::search_index(&index, &flds, &args.save, &query) {
            Ok((count, results)) => {
                println!("Found {} results for query '{}'", count, query);
                for (path, year) in results {
                    println!("Found: [{}] {}", year, path);
                }
            }
            Err(e) => error!("Error searching index: {}", e),
        }
    }

    if let Some(result_file) = args.csv {
        let output_file = format!("{}.csv", result_file);
        result_file_to_csv(&result_file, &output_file)
            .expect("Failed to convert result file to CSV");
    }
}
