//
/// 1. Fetch the tarballs from the dila server (without re-downloading them)
/// 2. Extract the tarballs in a directory, creating one big archive
/// 3. Index the content of the XML files remembering aclnofile name
/// 4. Answer queries on the database like: what are the files
///    matching some fulltext query
use anyhow::{Context, Result};
use reqwest::{Url, Client};

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::path::{Path, PathBuf};

use futures::stream::StreamExt;

use log::{debug, warn};
use serde::{Serialize, Deserialize};

use chrono::NaiveDate;

use crate::dumps::fonds::Fond;

/// Base URL for the dila server
pub const BASE_URL: &str = "https://echanges.dila.gouv.fr/OPENDATA";


impl From<&Fond> for Url {
    fn from(fond: &Fond) -> Self {
        let url = format!("{}/{}/", BASE_URL, fond.as_str());
        Url::parse(&url).expect("Failed to parse URL")
    }
}

/// A tarball is a compressed archive that is stored in the dila servers.
/// They are attached to a specific `fond`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tarball {
    /// Tarball name, typically of the form FOND_YYYYMMDD-XXXXX.tar.gz
    pub name: String,
    /// Fond to which this tarball belongs
    pub fond: Fond,
    /// Date of the tarball, extracted from the name
    pub time: NaiveDate,
}

/// Display implementation for Tarball
impl std::fmt::Display for Tarball {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{}", self.fond.as_str(), self.name)
    }
}

/// Convert from a Tarball into a URL
/// pointing to that tarball on the dila server.
impl From<&Tarball> for Url {
    fn from(tarball: &Tarball) -> Self {
        let url = format!("{}/{}/{}", BASE_URL, tarball.fond.as_str(), tarball.name);
        Url::parse(&url).expect("Failed to parse URL")
    }
}

/// A tarball can naturally be seen as a path 
/// from its name, allowing us to download it
impl AsRef<Path> for Tarball {
    fn as_ref(&self) -> &Path {
        self.name.as_ref()
    }
}

/// Extract the date from a tarball name as a `NaiveDateTime`.
/// The tarball names are of the form FOND_YYYYMMDD-XXXXX.tar.gz
/// where the date is using the Gregorian calendar with paris timezone.
/// The output is a `NaiveDateTime` representing the date and time
/// without timezone information.
fn extract_date_from_tarball_name(name: &str) -> Result<NaiveDate> {
    debug!("Extracting date from tarball name: {}", name);
    let date_part = name.split('_').collect::<Vec<_>>()
        .last()
        .ok_or_else(|| anyhow::anyhow!("Failed to extract date part from tarball name"))?
        .split('-')
        .next()
        .ok_or_else(|| anyhow::anyhow!("Failed to extract date part from tarball name"))?;

    debug!("Date part extracted: {}", date_part);

    use chrono::NaiveDate;
    let dt = NaiveDate::parse_from_str(date_part, "%Y%m%d")
        .expect("Failed to parse date from tarball name");
    debug!("DateTime parsed: {}", dt);
    Ok(dt)
}

/// List all tarballs in the dila server that are listed
/// in the page content given as a string
pub fn get_tarballs_from_page_content(fond : &Fond, content: &str) -> Vec<Tarball> {
    // fetch all strings matching the regex
    // \w*-\w*.tar.gz
    // and return them
    debug!("Extracting tarballs from content for fond: {}", fond);
    let re = regex::Regex::new(r"\w*-\w*.tar.gz").unwrap();
    let mut names: Vec<String> = re
        .captures_iter(content)
        .filter_map(|cap| cap.get(0))
        .map(|m| m.as_str().to_string())
        .collect();
    names.sort();
    names.dedup();
    names.into_iter()
        .filter_map(|name| {
            let time = extract_date_from_tarball_name(&name).ok()?;
            Some(Tarball { name, fond: fond.clone(), time })
        })
        .collect()
}

pub async fn list_tarballs(client: &Client, fond: &Fond) -> Result<Vec<Tarball>> {
    let url : Url = fond.into();
    let response = client.get(url).send().await?;
    if response.status().is_success() {
        let body = response.text().await?;
        Ok(get_tarballs_from_page_content(fond, &body))
    } else {
        warn!("Failed to fetch tarballs from {}", fond);
        Err(anyhow::anyhow!("Failed to fetch tarballs from {}", fond))
    }
}

async fn download_tarball(
    client: &Client,
    outdir: &PathBuf,
    tarball: &Tarball,
    mp: &MultiProgress,
) -> Result<bool> {
    let path = outdir.join(tarball);
    if path.exists() {
        debug!("{} already exists, skipping download", path.display());
        return Ok(false);
    }
    let url : Url = tarball.into();

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
        let mut file = tokio::fs::File::create(path.as_path())
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
                    warn!("Error downloading {}: {}", tarball, e);
                    return Err(e.into());
                }
            }
        }
    } else {
        warn!("Failed to download {}: {}", tarball, response.status());
    }
    Ok(true)
}

/// Download the tarballs from the dila server
/// if they are not already present
pub async fn download_tarball_list(
    client: &Client,
    tarballs: &[Tarball],
    dir: &PathBuf,
) -> Result<Vec<Tarball>> {
    if !dir.exists() {
        std::fs::create_dir_all(dir)
            .context(format!("Failed to create directory {}", dir.display()))?;
    }

    // Create a multi-progress bar
    let m = MultiProgress::new();

    let tasks = tarballs.into_iter().map(async |tarball| {
        let result = download_tarball(client, dir, &tarball, &m).await;
        match result {
            Ok(true) => Some(tarball.clone()),
            _ => None,
        }
    });

    let results = futures::stream::iter(tasks)
        .buffer_unordered(10) // Limit the number of concurrent downloads
        .filter_map(async |r| r)
        .collect::<Vec<_>>()
        .await;

    Ok(results)
}

pub async fn download_tarballs(
    client: &Client,
    dir: &PathBuf,
    fond: &Fond,
) -> Result<Vec<Tarball>> {
    let tarballs = list_tarballs(client, fond).await?;
    if tarballs.is_empty() {
        warn!("No tarballs found at {}", fond);
        return Ok(vec![]);
    }
    debug!("Found {} tarballs", tarballs.len());
    let tarballs = download_tarball_list(client, &tarballs, dir).await?;
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
pub fn list_files_in_dir(dir: PathBuf) -> Result<Vec<PathBuf>> {
    let mut dir_stack = Vec::new();
    let mut files = Vec::new();
    dir_stack.push(dir);
    while let Some(current_dir) = dir_stack.pop() {
        let dir_list = std::fs::read_dir(&current_dir).context(format!(
            "Failed to read directory {}",
            current_dir.display()
        ))?;
        for entry in dir_list {
            let entry = entry.expect("Failed to read directory entry");
            let path = entry.path();
            if path.is_dir() {
                dir_stack.push(path);
            } else {
                files.push(path);
            }
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

#[inline(always)]
fn build_schema_and_tokenizer() -> (
    tantivy::schema::Schema,
    tantivy::tokenizer::TextAnalyzer,
    IndexFields,
) {
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

    (schema, tok_fr, IndexFields { path, body, year })
}

pub fn init_tantivy(index_path: &PathBuf) -> Result<(tantivy::Index, IndexFields)> {
    use tantivy::Index;

    let (schema, tokenizer, fields) = build_schema_and_tokenizer();
    // If the index does not exist, create it
    // otherwise open it
    let index = match Index::open_in_dir(index_path) {
        Ok(index) => index,
        Err(_) => {
            // Create the index
            Index::create_in_dir(index_path, schema)?
        }
    };

    index.tokenizers().register("custom_fr", tokenizer);

    Ok((index, fields))
}

pub fn init_tantivy_ram() -> Result<(tantivy::Index, IndexFields)> {
    use tantivy::Index;

    let (schema, tokenizer, fields) = build_schema_and_tokenizer();

    // Create the index in RAM
    let index = Index::create_in_ram(schema);
    index.tokenizers().register("custom_fr", tokenizer);

    Ok((index, fields))
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

fn parse_file(dir: &PathBuf, file: &PathBuf, re: &regex::Regex) -> Result<FondXMLFile> {
    let body = std::fs::read_to_string(file).context("Could not open file")?;
    let year = get_year_juri(&body, re)
        .context(format!("Could not get year in {}", file.to_string_lossy()))?;
    let path = file
        .strip_prefix(dir)
        .map_err(|_| anyhow::anyhow!("Failed to strip prefix from {}", file.display()))?
        .to_string_lossy()
        .to_string();
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
    let files: Vec<PathBuf> = list_files_in_dir(dir.clone())?
        .into_iter()
        .filter(|p| p.is_file() && p.extension().map_or(false, |ext| ext == "xml"))
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
        if let Ok(doc) = parse_file(dir, &file, &re) {
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

    #[test]
    fn test_date_parsing() {
        use chrono::Datelike;
        let name = "CASS_20231125-130812.tar.gz";
        let date = extract_date_from_tarball_name(name).unwrap();
        assert_eq!(date.day(), 25);
        assert_eq!(date.month(), 11);
        assert_eq!(date.year(), 2023);

        let name = "CASS_20240101-200918.tar.gz";
        let date = extract_date_from_tarball_name(name).unwrap();
        assert_eq!(date.day(), 1);
        assert_eq!(date.month(), 1);
        assert_eq!(date.year(), 2024);

        let name = "Freemium_jorf_global_20231119-100000.tar.gz"; 
        let date = extract_date_from_tarball_name(name).unwrap();
        assert_eq!(date.day(), 19);
        assert_eq!(date.month(), 11);
        assert_eq!(date.year(), 2023);
    }

    #[test]
    fn test_get_tarballs_from_page_content() {
        use chrono::Datelike;
        let tarballs = get_tarballs_from_page_content(&Fond::CASS, MOCK_CASS_CONTENT);
        assert_eq!(tarballs.len(), 8);
        assert_eq!(tarballs[0].name, "CASS_20231125-130812.tar.gz");
        assert_eq!(tarballs[0].fond, Fond::CASS);
        assert_eq!(tarballs[0].time.day(), 25);
        assert_eq!(tarballs[0].time.month(), 11);
        assert_eq!(tarballs[0].time.year(), 2023);
    }

    #[test]
    fn test_get_year_juri() {
        let re = regex::Regex::new(r"(?<year>\d*)-\d*-\d*</DATE").unwrap();
        let doc = r#"<DATE_JURI>2023-01-01</DATE_JURI>"#;
        let year = get_year_juri(doc, &re).unwrap();
        assert_eq!(year, 2023);
    }

}
