mod tarballs;

use clap::Parser;

use std::path::PathBuf;
use anyhow::{Result, Context};

use indicatif::{ProgressBar, ProgressStyle};

use log::warn;

// implement ValueEnum for Fond
use clap::ValueEnum;
impl ValueEnum for tarballs::Fond {
    fn value_variants<'a>() -> &'a [Self] {
        &tarballs::FONDS
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        Some(clap::builder::PossibleValue::new(self.as_str()))
    }
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    /// Whether to download the tarballs
    #[clap(short, long, default_value = "false")]
    tarballs: bool,

    /// The list of tarballs to download,
    /// the default (empty) list will download all tarballs
    ///
    #[clap(short, long, default_value = "")]
    fond: Vec<tarballs::Fond>,

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

}

async fn get_tarballs(fonds: &[tarballs::Fond], dir: &PathBuf) -> Result<Vec<String>> {
    // Create a new HTTP client
    let client = reqwest::Client::new();

    let mut tarballs = Vec::new();

    for fond in fonds {
        println!("Downloading tarballs for {}", fond);
        let url = format!("{}/{}", tarballs::BASE_URL, fond);
        // Download the tarballs
        match tarballs::download_tarballs(&client, dir, &url).await {
            Ok(tarballs_list) => {
                tarballs.extend(tarballs_list);
            }
            Err(e) => {
                eprintln!("Error fetching tarballs: {}", e);
                continue;
            }
        }
    }

    Ok(tarballs)
}

fn extract_tarballs(idir: &PathBuf, odir: &PathBuf) -> Result<()> {
    // Extract all tarballs
    let tbfiles = std::fs::read_dir(idir)
        .expect("Could not read directory for tarballs");

    let to_extract : Vec<_> = tbfiles
        .into_iter()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path  = entry.path();
            let name  = path.file_name()?.to_str()?;
            let ext   = path.extension()?.to_str()?;
            if path.is_file() && (ext == "tar.gz" || ext == "gz") {
                Some(name.to_string())
            } else {
                None
            }
        }).collect();

    let pb = ProgressBar::new(to_extract.len() as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{msg} [{elapsed_precise}] {wide_bar} {pos}/{len} ({eta})")
        .context("Error creating progress bar")?
        .progress_chars("##-"));

    for p in to_extract {
        pb.set_message(format!("Extracting {}", p));
        let path = idir.join(p);
        if path.exists() {
            match tarballs::extract_tarball(&path, &odir) {
                Ok(_) => println!("Successfully extracted {:?}", path),
                Err(e) => eprintln!("Error extracting {:?}: {}", path, e),
            }
        } else {
            warn!("Tarball {:?} does not exist", path);
        }
        pb.inc(1);
    }
    Ok(())
}


#[tokio::main]
async fn main() {
    // Initialize the logger
    env_logger::init();

    // add to the help message the actual list of tarballs
    // this is a bit of a hack
    
    let args = Cli::parse();

    let dir    = std::env::current_dir()
        .expect("Failed to get current directory")
        .join("tarballs");

    let edir = std::env::current_dir()
        .expect("Failed to get current directory")
        .join("extracted");

    let index_path = std::env::current_dir()
        .expect("Failed to get current directory")
        .join("index");
    if !index_path.exists() {
        std::fs::create_dir_all(&index_path)
            .expect("Failed to create index directory");
    }

    if args.tarballs {
        let fonds = if args.fond.is_empty() {
            tarballs::FONDS
        } else {
            &args.fond
        };
        let _ = get_tarballs(fonds, &dir).await
            .expect("Failed to get tarballs");
    }

    if args.extract {
        extract_tarballs(&dir, &edir)
            .expect("Could not extract all tarballs");
    }


    let (index, flds) = tarballs::init_tantivy(&index_path)
        .expect("Failed to create index");

    if args.index {
        println!("Creating index at {}", index_path.display());

        let mut writer = index.writer(50_000_000)
            .expect("Failed to create writer");

        // index the files
        tarballs::index_files_in_dir(&mut writer, &flds, &edir)
            .expect("Failed to index files");

    }

    if let Some(query) = args.query {
        match tarballs::search_index(&index, &flds, &query) {
            Ok((count, results)) => {
                println!("Found {} results for query '{}'", count, query);
                for (path, year) in results {
                    println!("Found: [{}] {}", year, path);
                }
            }
            Err(e) => eprintln!("Error searching index: {}", e),
        }
    }
}
