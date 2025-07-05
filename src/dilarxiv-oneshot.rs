/// A “one shot” version of the dilarxiv program
/// that downloads, extracts, and runs a query
/// on documents from the DILA, without writing
/// too many files to the disk an once.
///
/// This is intended to be used to reproduce a
/// specific search result in “low disk space”
/// environments (e.g. github actions) and may
/// be marginally faster than the dilarxiv program
/// if the user is only interested in a single query.
use clap::Parser;

use anyhow::Result;
use std::path::{Path, PathBuf};

use indicatif::{ProgressBar, ProgressStyle};

use log::{error, info, debug};

use temp_dir::TempDir;

use std::io::BufWriter;

use legifrance::dumps::extractor::parse_file;
use legifrance::dumps::fonds::{FONDS, Fond};
use legifrance::dumps::tarballs;

use legifrance::dumps::extractor::PreDilaText;


#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    /// The list of tarballs to download,
    /// the default (empty) list will download all tarballs
    #[clap(short, long, num_args(0..))]
    fond: Vec<Fond>,

    /// The query used to search the index
    #[clap(short, long)]
    query: String,

    /// Export the search results to a CSV
    /// file with the correct metadata
    #[clap(short, long)]
    to_csv: String,
}

/// create workers that will read and 
/// process files in parallel.
/// This creates `num_threads` threads, and returns their handles 
/// together with channels to communicate with them
fn span_parser_threads(
    num_threads : usize,
    writer_channel: &std::sync::mpsc::Sender<PreDilaText>,
    ) -> Vec<(std::thread::JoinHandle<()>, std::sync::mpsc::Sender<PathBuf>)> {
    use std::thread;
    use std::sync::mpsc;

    info!("Using {} worker threads", num_threads);

    (0..num_threads)
        .map(|_| {
            let (thread_tx, thread_rx) = mpsc::channel();
            let writer_channel = writer_channel.clone();
            let handle = thread::spawn(move || {
                let mut file_buffer = String::new();
                // This is a placeholder for any work that needs to be done
                // in the worker threads. In this case, we do nothing.
                while let Ok(file_path) = thread_rx.recv() {
                    let content = parse_file(file_path, &mut file_buffer);
                    writer_channel
                        .send(content)
                        .expect("Failed to send content to writer channel");
                    file_buffer.clear();
                }
            });
            (handle, thread_tx)
        })
        .collect()
}

fn spawn_writer_thread<T>(file_path : T) -> (std::thread::JoinHandle<()>, std::sync::mpsc::Sender<PreDilaText>)
    where 
        T : AsRef<Path>
{
    use std::thread;
    use std::sync::mpsc;

    let mut writer = csv::WriterBuilder::new()
        .has_headers(true)
        .from_path(file_path)
        .expect("Failed to create CSV writer");

    let (writer_tx, writer_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        while let Ok(row) = writer_rx.recv(){
            info!("Writing row to CSV: {:?}", row);
            writer.serialize(row)
                .expect("Failed to write row to CSV");
        }
        info!("Flushing CSV writer");
        writer.flush().expect("Failed to flush CSV writer");
    });
    (handle, writer_tx)
}

fn result_file_to_csv<T>(edir: &PathBuf, result_file: T, output_file: T) -> Result<()>
where
    T: AsRef<Path>,
{
    use std::io::BufRead;

    info!("Converting result file to CSV: {}", output_file.as_ref().display());

    // open the resulting file, create it if it does not exist, erase
    // it if it did
    let file = std::fs::OpenOptions::new()
        .read(true)
        .append(false)
        .write(true)
        .create(true)
        .truncate(true)
        .open(result_file)?;

    let mut reader = std::io::BufReader::new(file);
    // buffer to allocate lines
    let mut line = String::new();

    let (writer_handle, writer_channel) = spawn_writer_thread(output_file);
    let parsers = span_parser_threads(5, &writer_channel);

    let mut i = 0;
    while reader.read_line(&mut line)? != 0 {
        let path = edir.join(&line);
        if let Some((_, tx)) = parsers.get(i % parsers.len()) {
            // send the path to the worker thread
            tx.send(path).expect("Failed to send path to worker thread");
        } else {
            error!("No worker thread available to process file: {}", path.display());
        }
        i += 1;
        // clear the line buffer for the next iteration
        line.clear();
    }

    // wait for everyone to finish
    for (handle, tx) in parsers {
        handle.join().expect("Failed to join parser thread");
        drop(tx);
    }
    writer_handle.join().expect("Failed to join writer thread");
    drop(writer_channel);
    info!("All worker threads finished processing");

    Ok(())
}

#[tokio::main]
async fn main() {
    // Initialize the logger
    env_logger::init();

    use futures::StreamExt;
    use std::sync::Arc;

    info!("Starting dilarxiv-oneshot...");

    let args = Cli::parse();

    let tmpdir_doc = TempDir::new().expect("Failed to create temporary directory");

    let tmpdir = tmpdir_doc.path().to_path_buf();

    let dl_dir = Arc::new(tmpdir.join("tarballs"));
    let extract_dir = Arc::new(tmpdir.join("extracted"));
    let results_dir = Arc::new(tmpdir.join("results"));

    // Create the directories if they don't exist
    std::fs::create_dir(dl_dir.as_path()).expect("Failed to create download directory");
    std::fs::create_dir(extract_dir.as_path()).expect("Failed to create extract directory");
    std::fs::create_dir(results_dir.as_path()).expect("Failed to create results directory");

    let result_file = tmpdir.join("results.txt");
    let result_tmp = tmpdir.join("results_tmp.txt");

    let result_file_out = Some(
        result_tmp
            .to_str()
            .expect("Failed to convert path to string")
            .to_string(),
    );
    let mut result_file_final = BufWriter::new(
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .append(false)
            .create(true)
            .open(&result_file)
            .expect("Failed to open result file"),
    );

    info!("Created all temporary directories");

    let client = Arc::new(reqwest::Client::new());

    let fonds = if args.fond.is_empty() {
        FONDS
    } else {
        &args.fond
    };

    // First list tarballs to download (relatively small)
    // do it in parallel by using the `futures` crate with streams
    let strm = futures::stream::iter(fonds.iter())
        .filter_map(|fond| {
            let client = client.clone();
            async move {
                match tarballs::list_tarballs(&client, fond).await {
                    Ok(tarballs) => {
                        info!("Found {} tarballs for {}", tarballs.len(), fond);
                        Some(async move { tarballs })
                    }
                    Err(e) => {
                        error!("Failed to download tarballs for {}: {}", fond, e);
                        None
                    }
                }
            }
        })
        .buffered(10)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .flatten()
        .rev()
        .collect::<Vec<_>>();

    info!("Found {} tarballs to download", strm.len());

    let (index, flds) = tarballs::init_tantivy_ram().expect("Failed to create index");

    let mut writer = index.writer(100_000_000).expect("Failed to create writer");

    info!("Prepared the index and writer");

    // create the progress bar
    let pb = ProgressBar::new(strm.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] {wide_bar:.cyan/blue} {percent}% ({eta})",
            )
            .expect("Error creating progress bar")
            .progress_chars("##-"),
    );

    // now, for every block of x tarballs,
    // download + extract them in parallel
    for chunk in strm.chunks(10) {
        pb.set_message(format!("Processing {} tarballs", chunk.len()));
        // this download happens in parallel
        let tblist = tarballs::download_tarball_list(&client, chunk, &dl_dir)
            .await
            .expect("Failed to download tarballs");

        info!("Downloaded tarballs");

        // extract them (also in parallel)
        let _ = futures::stream::iter(tblist)
            .map(|tarball_path| {
                let dl_dir = dl_dir.clone();
                let extract_dir = extract_dir.clone();
                async move {
                    let path = dl_dir.join(&tarball_path);
                    tarballs::extract_tarball(&path, &extract_dir)
                }
            })
            .buffer_unordered(10)
            .collect::<Vec<_>>()
            .await;

        info!("Extracted tarballs from {}", dl_dir.display());
        info!("Extracted to {}", extract_dir.display());

        // index the extracted files
        // (sequentially)
        tarballs::index_files_in_dir(&mut writer, &flds, &extract_dir)
            .expect("Failed to index files");
        
        info!("Indexed all the files");

        // commit the writer
        writer.commit().expect("Failed to commit writer");

        // now search the index
        match tarballs::search_index(&index, &flds, &result_file_out, &args.query) {
            Ok(_) => {
                info!("Search completed successfully");
            }
            Err(e) => {
                error!("Failed to search index: {}", e);
            }
        }
        info!("Search results written to {}", result_file.display());

        debug!("Copying results to final result file");
        // we should be `appending` results here...
        std::io::copy(
            &mut std::fs::File::open(&result_tmp).expect("Failed to open result file"),
            &mut result_file_final,
        )
            .expect("Failed to copy result file");


        // move the results of the search to the "results" directory
        // this is done by reading all the lines in the
        // `result_file` and performing std::fs operations

        let results = std::fs::read_to_string(&result_tmp)
            .expect("Failed to read results file");

        for line in results.lines() {
            let infile = extract_dir.join(line);
            let outfile = results_dir.join(line);

            debug!("Moving file from {} to {}", infile.display(), outfile.display());
            // create the parent directory if it doesn't exist
            if let Some(parent) = outfile.parent() {
                debug!("Creating parent directory: {}", parent.display());
                std::fs::create_dir_all(parent).expect("Failed to create parent directory");
            }

            // move the file
            std::fs::rename(&infile, &outfile).expect("Failed to move file");
        }

        // delete tarballs and extracted files
        std::fs::remove_dir_all(dl_dir.as_path()).expect("Failed to remove download directory");
        std::fs::remove_dir_all(extract_dir.as_path()).expect("Failed to remove directory");

        // recreate them immediately
        std::fs::create_dir_all(dl_dir.as_path()).expect("Failed to create download directory");
        std::fs::create_dir_all(extract_dir.as_path()).expect("Failed to create extract directory");

        // clear the index
        writer
            .delete_all_documents()
            .expect("Failed to delete all documents");
        writer.commit().expect("Failed to commit writer");

        // clear the result file
        std::fs::remove_file(&result_tmp)
            .expect("Failed to remove temporary result file");

        pb.inc(chunk.len() as u64);
    }
    pb.finish_with_message("All tarballs processed");

    info!("All tarballs processed, moving results to CSV");

    result_file_to_csv(&results_dir, result_file.as_path(), args.to_csv.as_ref())
        .expect("Failed to convert result file to CSV");

    info!("Results exported to {}", args.to_csv);
}
