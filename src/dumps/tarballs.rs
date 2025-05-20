///
/// 1. Fetch the tarballs from the dila server (without re-downloading them)
/// 2. Extract the tarballs in a directory, creating one big archive
/// 3. Index the content of the XML files remembering aclnofile name
/// 4. Answer queries on the database like: what are the files
///    matching some fulltext query
use anyhow::{Context, Result};
use reqwest::Client;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::path::PathBuf;

use futures::stream::StreamExt;

use log::{debug, warn};

/// Base URL for the dila server
pub const BASE_URL: &str = "https://echanges.dila.gouv.fr/OPENDATA";

/// List all tarballs in the dila server that are listed
/// in the page content given as a string
pub fn get_tarballs_from_page_content(page: &str) -> Vec<String> {
    // fetch all strings matching the regex
    // \w*-\w*.tar.gz
    // and return them
    let re = regex::Regex::new(r"\w*-\w*.tar.gz").unwrap();
    let mut names: Vec<String> = re
        .captures_iter(page)
        .filter_map(|cap| cap.get(0))
        .map(|m| m.as_str().to_string())
        .collect();
    names.sort();
    names.dedup();
    names
}

async fn get_tarballs(client: &Client, url: &str) -> Result<Vec<String>> {
    let response = client.get(url).send().await?;
    if response.status().is_success() {
        let body = response.text().await?;
        Ok(get_tarballs_from_page_content(&body))
    } else {
        warn!("Failed to fetch tarballs from {}", url);
        Ok(vec![])
    }
}

async fn download_tarball(
    client: &Client,
    url: &str,
    path: &PathBuf,
    mp: &MultiProgress,
) -> Result<()> {
    if path.exists() {
        debug!("{} already exists, skipping download", path.display());
        return Ok(());
    }

    // Create a progress bar for the download
    let pb = mp.add(ProgressBar::new(0));
    let pbstyle = ProgressStyle::default_bar()
        .template("{msg} [{wide_bar}] {bytes}/{total_bytes} ({eta})")
        .context("Failed to create progress bar template")?
        .progress_chars("##-");
    pb.set_style(pbstyle);
    pb.set_message(format!("Downloading {}", url));
    pb.set_length(0);

    let response = client.get(url).send().await?;
    if let Some(content_length) = response.content_length() {
        pb.set_length(content_length);
    }

    if response.status().is_success() {
        let mut file = tokio::fs::File::create(path)
            .await
            .context(format!("Failed to create file {}", path.display()))?;
        let mut buf_writer = tokio::io::BufWriter::new(&mut file);
        let mut bs = response.bytes_stream();

        while let Some(item) = bs.next().await {
            match item {
                Ok(bytes) => {
                    // Update the progress bar
                    pb.set_position(pb.position() + bytes.len() as u64);
                    tokio::io::copy(&mut bytes.as_ref(), &mut buf_writer)
                        .await
                        .context(format!("Failed to copy bytes to {}", path.display()))?;
                }
                Err(e) => {
                    warn!("Error downloading {}: {}", url, e);
                    return Err(e.into());
                }
            }
        }
    } else {
        warn!("Failed to download {}: {}", url, response.status());
    }
    Ok(())
}

/// Download the tarballs from the dila server
/// if they are not already present
async fn download_tarball_list(
    client: &Client,
    tarballs: &[String],
    dir: &PathBuf,
    base_url: &str,
) -> Result<()> {
    if !dir.exists() {
        std::fs::create_dir_all(dir)
            .context(format!("Failed to create directory {}", dir.display()))?;
    }

    // Create a multi-progress bar
    let m = MultiProgress::new();

    let tasks = tarballs.iter().map(async |tarball| {
        let url = format!("{}/{}", base_url, tarball);
        let path = dir.join(tarball);
        let pdir = path
            .parent()
            .unwrap_or_else(|| panic!("Failed to get parent directory for {}", path.display()));
        if !pdir.exists() {
            std::fs::create_dir_all(pdir)
                .context(format!("Failed to create directory {}", pdir.display()))?;
        }
        Ok(download_tarball(client, &url, &path, &m).await)
    });

    let _results: Vec<Result<Result<()>>> = futures::stream::iter(tasks)
        .buffer_unordered(10) // Limit the number of concurrent downloads
        .collect()
        .await;

    Ok(())
}

pub async fn download_tarballs(
    client: &Client,
    dir: &PathBuf,
    base_url: &str,
) -> Result<Vec<String>> {
    let tarballs = get_tarballs(client, base_url).await?;
    if tarballs.is_empty() {
        warn!("No tarballs found at {}", base_url);
        return Ok(vec![]);
    }
    debug!("Found {} tarballs", tarballs.len());
    download_tarball_list(client, &tarballs, dir, base_url).await?;
    Ok(tarballs)
}

/// SECOND PART
/// extract tarballs

pub fn extract_tarball(tarball: &PathBuf, dir: &PathBuf) -> Result<()> {
    let file = std::fs::File::open(tarball)
        .context(format!("Failed to open tarball {}", tarball.display()))?;

    let gzip = flate2::read::GzDecoder::new(file);

    let mut tar = tar::Archive::new(gzip);

    // Extract the tarball to the specified directory
    tar.unpack(dir)
        .context(format!("Failed to extract tarball {}", tarball.display()))?;
    Ok(())
}

/// List all files recursively in a directory
/// and return them as an iterator
pub fn list_files_in_dir(dir: &PathBuf) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in
        std::fs::read_dir(dir).context(format!("Failed to read directory {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            files.extend(list_files_in_dir(&path)?);
        } else {
            files.push(path);
        }
    }
    Ok(files)
}

/// Naïve search for a string in a file
fn search_in_file(file: &PathBuf, query: &str) -> Result<bool> {
    let ctn =
        std::fs::read_to_string(file).context(format!("Could not open file {}", file.display()))?;
    if ctn.contains(query) {
        Ok(true)
    } else {
        Ok(false)
    }
}

/// Search for a string in all files in a directory
/// and return the files that match
pub fn search_in_dir(dir: &PathBuf, query: &str) -> Result<Vec<PathBuf>> {
    let mut results = Vec::new();

    let mut candidates = vec![dir.clone()];
    let mut total = 1;

    let pb = ProgressBar::new(0);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{msg} [{wide_bar}] {pos}/{len} ({eta})")
            .context("Failed to create progress bar template")?
            .progress_chars("##-"),
    );
    pb.set_message(format!("Searching in {}", dir.display()));
    pb.set_length(total);

    while let Some(candidate) = candidates.pop() {
        pb.set_message(format!("Searching in {}", candidate.display()));
        pb.inc(1);
        for entry in std::fs::read_dir(candidate)
            .context(format!("Failed to read directory {}", dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                candidates.push(path);
                total += 1;
            } else {
                if search_in_file(&path, query)? {
                    results.push(path);
                }
            }
        }
        pb.set_length(total);
    }
    Ok(results)
}

/// STEP 3 create the index
/// using tantivy

pub mod file_collector {

    use std::fs::OpenOptions;
    use std::io::BufWriter;
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use tantivy::collector::Collector;
    use tantivy::collector::SegmentCollector;
    use tantivy::index::SegmentReader;
    use tantivy::schema::Field;
    use tantivy::schema::{OwnedValue, TantivyDocument};
    use tantivy::store::StoreReader;
    use tantivy::{DocId, Result, Score, SegmentOrdinal};

    /// In order to write the list of all matches into
    /// a file we will create our own Collector / SegmentCollector
    /// that will buffer-write the results to a file
    ///
    /// FileList collector is an empty struct used to implement the traits
    pub struct FileListCollector {
        path_field: Field,
        bufwriter: FileCollectorFruit,
    }

    impl FileListCollector {
        pub fn new(path_field: Field, file_path: &PathBuf) -> FileListCollector {
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(file_path)
                .expect("could not open file");
            let bufwriter = Arc::new(Mutex::new(BufWriter::new(file)));
            FileListCollector {
                path_field,
                bufwriter,
            }
        }
    }

    // this will be shared between all segments
    // so we need to make sure that it is thread-safe
    // and we need to use a Mutex to protect it
    type FileCollectorFruit = Arc<Mutex<std::io::BufWriter<std::fs::File>>>;

    pub struct FileListSegmentCollector {
        path_field: Field,
        bufwriter: FileCollectorFruit,
        store_reader: StoreReader,
    }

    impl SegmentCollector for FileListSegmentCollector {
        type Fruit = ();

        fn collect(&mut self, doc: DocId, _: Score) {
            let doc: TantivyDocument = self
                .store_reader
                .get(doc)
                .expect(format!("Could not get document {doc}").as_str());

            match doc.get_first(self.path_field) {
                Some(OwnedValue::Str(s)) => {
                    let mut lock = self.bufwriter.lock().expect("Unable to acquire lock");
                    write!(lock, "{}\n", s).expect("Unable to write to buffer");
                }
                _ => {}
            }
        }

        fn harvest(self) {
            let mut lock = self.bufwriter.lock().expect("Unable to acquire lock");
            lock.flush().expect("unable to flush buffer");
        }
    }

    impl Collector for FileListCollector {
        type Fruit = ();
        type Child = FileListSegmentCollector;

        fn requires_scoring(&self) -> bool {
            false
        }

        fn for_segment(
            &self,
            _: SegmentOrdinal,
            segment_reader: &SegmentReader,
        ) -> Result<Self::Child> {
            let store = segment_reader.get_store_reader(100)?;
            Ok(FileListSegmentCollector {
                path_field: self.path_field,
                bufwriter: self.bufwriter.clone(),
                store_reader: store,
            })
        }

        fn merge_fruits(&self, _: Vec<()>) -> Result<()> {
            Ok(())
        }
    }
}

pub struct IndexFields {
    path: tantivy::schema::Field,
    body: tantivy::schema::Field,
    year: tantivy::schema::Field,
}

pub fn init_tantivy(index_path: &PathBuf) -> Result<(tantivy::Index, IndexFields)> {
    use tantivy::Index;
    use tantivy::schema::*;
    use tantivy::tokenizer::*;

    let tok_fr = TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(RemoveLongFilter::limit(40))
        .filter(LowerCaser)
        .filter(AsciiFoldingFilter)
        .filter(StopWordFilter::new(Language::French).unwrap())
        .build();

    let idx_fr = TextFieldIndexing::default()
        .set_tokenizer("custom_fr")
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);

    let opts_fr = TextOptions::default()
        .set_indexing_options(idx_fr)
        .set_stored();

    let mut schema_builder = Schema::builder();
    let path = schema_builder.add_text_field("path", STRING | STORED);
    let body = schema_builder.add_text_field("body", opts_fr);
    let year = schema_builder.add_u64_field("year", FAST | INDEXED | STORED);
    let schema = schema_builder.build();

    // If the index does not exist, create it
    // otherwise open it
    let index = match Index::open_in_dir(index_path) {
        Ok(index) => index,
        Err(_) => {
            // Create the index
            Index::create_in_dir(index_path, schema.clone())?
        }
    };

    index.tokenizers().register("custom_fr", tok_fr);

    Ok((index, IndexFields { path, body, year }))
}

fn get_year_juri(doc: &str, re: &regex::Regex) -> Result<u64> {
    let names: Vec<u64> = re
        .captures_iter(doc)
        .map(|cap| cap["year"].to_owned())
        .filter_map(|s| s.parse().ok())
        .take(1)
        .collect();
    if names.len() > 0 {
        Ok(names[0])
    } else {
        Err(anyhow::anyhow!("Cannot find date in juri document"))
    }
}

#[derive(Debug, Clone)]
struct FondXMLFile {
    path: String,
    body: String,
    year: u64,
}

fn parse_file(file: &PathBuf, re: &regex::Regex) -> Result<FondXMLFile> {
    let body = std::fs::read_to_string(file).context("Could not open file")?;
    let year = get_year_juri(&body, re)
        .context(format!("Could not get year in {}", file.to_string_lossy()))?;
    let path = file.to_string_lossy().to_string();
    Ok(FondXMLFile { path, body, year })
}

/// Index a file in the tantivy index
fn index_file(
    index_writer: &mut tantivy::IndexWriter,
    fields: &IndexFields,
    file: FondXMLFile,
) -> Result<()> {
    let mut doc = tantivy::TantivyDocument::default();

    doc.add_text(fields.path, file.path);
    doc.add_text(fields.body, file.body);
    doc.add_u64(fields.year, file.year);
    index_writer.add_document(doc)?;
    Ok(())
}

/// Index all files in a directory using tantivy,
/// recursively
pub fn index_files_in_dir(
    index_writer: &mut tantivy::IndexWriter,
    fields: &IndexFields,
    dir: &PathBuf,
) -> Result<()> {
    // create a progress bar
    let pb = ProgressBar::new(0);
    let re = regex::Regex::new(r"(?<year>\d*)-\d*-\d*</DATE").unwrap();
    let files: Vec<PathBuf> = list_files_in_dir(dir)?
        .into_iter()
        .filter(|p| {
            if let Some(e) = p.extension() {
                e == "xml"
            } else {
                false
            }
        })
        .collect();

    pb.set_length(files.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{msg} [{wide_bar}] {pos}/{len} ({eta})")
            .context("Failed to create progress bar template")?
            .progress_chars("##-"),
    );
    pb.set_message(format!("Indexing {} files", files.len()));

    for file in files {
        if let Ok(doc) = parse_file(&file, &re) {
            match index_file(index_writer, fields, doc) {
                Ok(_) => {
                    pb.set_message(format!("Indexed {}", file.display()));
                }
                Err(e) => {
                    warn!("Failed to index {}: {}", file.display(), e);
                }
            }
        } else {
            warn!("Failed to parse {}", file.display());
        }
        pb.inc(1);
    }
    index_writer.commit()?;
    Ok(())
}

/// search all files in the index
pub fn search_index(
    index: &tantivy::Index,
    fields: &IndexFields,
    save: &Option<String>,
    query: &str,
) -> Result<(usize, Vec<(String, u64)>)> {
    use tantivy::schema::document::Value;
    let reader = index
        .reader_builder()
        .reload_policy(tantivy::ReloadPolicy::OnCommitWithDelay)
        .try_into()?;
    let searcher = reader.searcher();
    let query_parser = tantivy::query::QueryParser::for_index(index, vec![fields.body]);
    let query = query_parser.parse_query(query)?;

    let (doc_count, top_docs) = if let Some(savepath) = save {
        let fpath = PathBuf::from(savepath);
        let fcol = file_collector::FileListCollector::new(fields.path, &fpath);
        let (d, t, _) = searcher.search(
            &query,
            &(
                tantivy::collector::Count,
                tantivy::collector::TopDocs::with_limit(10),
                fcol,
            ),
        )?;
        (d, t)
    } else {
        searcher.search(
            &query,
            &(
                tantivy::collector::Count,
                tantivy::collector::TopDocs::with_limit(10),
            ),
        )?
    };

    let mut results = Vec::new();
    for (_, doc_address) in top_docs {
        let doc: tantivy::TantivyDocument = searcher.doc(doc_address)?;
        let title = doc
            .get_first(fields.path)
            .ok_or_else(|| anyhow::anyhow!("Failed to get path"))?
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Path is not a string"))?;
        let year = doc
            .get_first(fields.year)
            .ok_or_else(|| anyhow::anyhow!("Failed to get year"))?
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("Year is not u64"))?;

        results.push((title.to_owned(), year));
    }
    Ok((doc_count, results))
}

#[cfg(test)]
mod tests {
    use super::*;

    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const MOCK_CASS_CONTENT: &str = r#"
<img src="/icons/compressed.gif" alt="[   ]"> <a href="CASS_20231125-130812.tar.gz">CASS_20231125-130812.tar.gz</a>                 2023-11-25 15:04  261K  
<img src="/icons/compressed.gif" alt="[   ]"> <a href="CASS_20231127-204209.tar.gz">CASS_20231127-204209.tar.gz</a>                 2023-11-27 20:44  130K  
<img src="/icons/compressed.gif" alt="[   ]"> <a href="CASS_20231204-205306.tar.gz">CASS_20231204-205306.tar.gz</a>                 2023-12-04 20:55  145K  
<img src="/icons/compressed.gif" alt="[   ]"> <a href="CASS_20231211-211048.tar.gz">CASS_20231211-211048.tar.gz</a>                 2023-12-11 21:13  212K  
<img src="/icons/compressed.gif" alt="[   ]"> <a href="CASS_20231218-205651.tar.gz">CASS_20231218-205651.tar.gz</a>                 2023-12-18 20:59  311K  
<img src="/icons/compressed.gif" alt="[   ]"> <a href="CASS_20240101-200918.tar.gz">CASS_20240101-200918.tar.gz</a>                 2024-01-01 20:10  408K  
<img src="/icons/compressed.gif" alt="[   ]"> <a href="CASS_20240108-211850.tar.gz">CASS_20240108-211850.tar.gz</a>                 2024-01-08 21:22  165K  
<img src="/icons/compressed.gif" alt="[   ]"> <a href="CASS_20240115-204455.tar.gz">CASS_20240115-204455.tar.gz</a>                 2024-01-15 20:47  306K
"#;

    async fn setup_mock_server() -> MockServer {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/CASS"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(MOCK_CASS_CONTENT.to_string())
                    .append_header("Content-Type", "text/html"),
            )
            // Mounting the mock on the mock server - it's now effective!
            .mount(&mock_server)
            .await;

        mock_server
    }

    #[test]
    fn test_get_tarballs_from_page_content() {
        let tarballs = get_tarballs_from_page_content(MOCK_CASS_CONTENT);
        assert_eq!(tarballs.len(), 8);
        assert_eq!(tarballs[0], "CASS_20231125-130812.tar.gz");
    }

    #[test]
    fn test_get_year_juri() {
        let re = regex::Regex::new(r"(?<year>\d*)-\d*-\d*</DATE").unwrap();
        let doc = r#"<DATE_JURI>2023-01-01</DATE_JURI>"#;
        let year = get_year_juri(doc, &re).unwrap();
        assert_eq!(year, 2023);
    }

    #[tokio::test]
    async fn test_get_tarballs() {
        use crate::dumps::fonds::Fond;

        let mock_server = setup_mock_server().await;
        let url = format!("{}/{}", mock_server.uri(), Fond::CASS);
        let client = Client::new();
        let tarballs = get_tarballs(&client, &url).await.unwrap();
        assert_eq!(tarballs.len(), 8);
        assert_eq!(tarballs[0], "CASS_20231125-130812.tar.gz");
    }
}
