use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};

use log::{info, warn};

use std::path::PathBuf;
use indicatif::{ProgressBar, MultiProgress, ProgressStyle};
use futures::stream::StreamExt;

/// This is the module containing the datatypes
/// for the API
pub mod piste;
pub mod client;

use piste::{Fond, FONDS};
use client::{AuthenticatedClient, 
             PageQuery, 
             ping_api, 
             get_search_result,
             get_full_text};


async fn get_page_and_write(aclient: &AuthenticatedClient,
                            dir:     &PathBuf,
                            pq:      &PageQuery) -> Result<()> {

    // Compute the filename used to store the query
    let filename = match (&pq.fond, pq.start_year, pq.end_year) {
        (Some(f), Some(s), Some(e)) => format!("{}-DATE-{}-{}-PAGE-{}.json", f, s, e, pq.page),
        (None   , Some(s), Some(e)) => format!("ALL-DATE-{}-{}-PAGE-{}.json", s, e, pq.page),
        (Some(f), _, _)             => format!("{}-NODATE-PAGE-{}.json", f, pq.page),
        (None, _, _)                => format!("ALL-NODATE-PAGE-{}.json", pq.page),
    };

    let filepath = dir.join(&filename);

    // check if the file already exists, abort the download
    if std::path::Path::new(&filepath).exists() {
        info!("File {} already exists, skipping", filename);
        return Ok(());
    }

    let res = get_search_result(aclient, pq).await?;
    let total = res.total_result_number;
    info!("Total Results: {}", total);
    std::fs::write(filepath, serde_json::to_string(&res)?)
        .expect(format!("Unable to write file {}", filename).as_str());
    Ok(())
}

/// A PageQuery can only be used to get at most 100 results.
/// This function will compute the page queries
/// needed to get all the results.
///
/// It will return a vector of PageQuery
/// and the total number of results.
///
/// *Warning*: it is not possible to go beyond 100 pages
/// this is a limitation of the API. Hence, queries
/// should have at most 10 000 results. The function
/// will return an error if the number of pages
/// is greater than 100.
async fn compute_pagination(aclient: &AuthenticatedClient, 
                            pq:      &PageQuery)
    -> Result<Vec<PageQuery>> {
    // get the total number of pages
    let res = get_search_result(aclient, pq).await?;
    let total = res.total_result_number;

    // compute the number of pages
    let page_count = (total as f64 / 100.0).ceil() as u64;

    if page_count > 100 {
        warn!("Error: too many pages {} > 100 for query {:?}", page_count, pq.fond);
        return Err(anyhow::anyhow!("Error: too many pages {} > 100", page_count));
    }

    // create a stream of pages
    let pages = (1..=page_count).map(|page| {
        PageQuery {
            text: pq.text.clone(),
            page: page as u8, // this cast is safe because page <= 100
            start_year: pq.start_year,
            end_year: pq.end_year,
            fond: pq.fond.clone(),
        }
    }).collect::<Vec<_>>();

    Ok(pages)
}

/// Compute page queries for a list of fonds
/// and potentially a list of years
///
/// If the list of fonds is empty, it will use all the fonds
/// (i.e., put a None in the fond field of the PageQuery)
fn compute_prequeries(text:        &str,
                      fonds:       &[Fond],
                      year_ranges: &[(u64, u64)],
                      ) -> impl Iterator<Item = PageQuery> {
    year_ranges.iter().flat_map(|(start_year, end_year)| {
        match fonds.len() {
            0  => {
                    vec![PageQuery {
                        text: text.to_string(),
                        page: 1,
                        start_year: Some(*start_year as u64),
                        end_year:   Some(*end_year as u64),
                        fond: None,
                    }]
            }
            _ => {
                fonds.iter().map(|fond| {
                    PageQuery {
                        text: text.to_string(),
                        page: 1,
                        start_year: Some(*start_year as u64),
                        end_year:   Some(*end_year as u64),
                        fond:       Some(fond.clone()),
                    }
                }).collect::<Vec<_>>()
            }
        }
    })
}

async fn download_all_pages(aclient: &AuthenticatedClient,
                            dir:     &PathBuf,
                            mb:      &MultiProgress,
                            pq:      &PageQuery) -> Result<()> {

    let pqs = compute_pagination(&aclient, &pq).await?;

    let bar = mb.add(ProgressBar::new(0));
    bar.set_style(ProgressStyle::default_bar()
        .template("{msg} [{elapsed_precise}] {wide_bar} {pos:>7}/{len:7} ({eta})")
        .context("Unable to create progress bar")?
        .progress_chars("##-"));
    bar.set_message(format!("[{:?}] {:?}-{:?}-{:?}", pq.fond, pq.text, pq.start_year, pq.end_year));
    bar.set_length(pqs.len() as u64);

    let stream = futures::stream::iter(pqs)
        .map(|pq| {
            let aclient = &aclient;
            let dir     = &dir;
            let bar    = &bar;
            async move {
                match get_page_and_write(aclient, dir, &pq).await {
                    Ok(_) => {
                        bar.inc(1);
                        Ok(())
                    },
                    Err(e) => {
                        warn!("Error: {}", e);
                        Err(e)
                    }
                }
            }
        });

    // execute the stream using ordered concurrency
    // with a limit of 20 concurrent tasks
    let _ = stream.buffer_unordered(20).collect::<Vec<Result<()>>>().await;
    Ok(())
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct AbstractSearchResult {
    id: String,
    fond: Fond,
}

pub async fn example_text_extraction(
    aclient: &AuthenticatedClient,
    ) -> Result<()> {

    let doc = get_full_text(aclient, "JORFARTI000022337081", Fond::Jorf).await?;
   
    info!("Document: {:?}", doc);

    Ok(())

}

pub async fn call_api() -> Result<()> {
    // read the client_id and client_secret from a file
    // and strip the newline character
    let client_id     = std::fs::read_to_string("client-id.txt")?;
    let client_secret = std::fs::read_to_string("client-secret.txt")?;

    let aclient = AuthenticatedClient::from_secret(&client_id, &client_secret).await
        .context("Unable to create authenticated client")?;


    ping_api(&aclient, "/search/ping").await?;
    ping_api(&aclient, "/consult/ping").await?;

    example_text_extraction(&aclient).await?;

    let year_ranges = vec![
        (1950, 2000),
        (2000, 2025),
    ];
    let text = "CESEDA";



    let mb = MultiProgress::new();
    let dir    = PathBuf::from("data");
    let stream = futures::stream::iter(compute_prequeries(text, &FONDS, &year_ranges))
        .map(|pq| {
            let aclient = &aclient;
            let mb      = &mb;
            let dir     = &dir;
            async move {
                info!("Downloading pages for {:?}", &pq);
                download_all_pages(aclient, dir, mb, &pq).await
            }
        });

    // execute the stream using 10 concurrent tasks
    let _ = stream.buffer_unordered(4).collect::<Vec<Result<()>>>().await;

    Ok(())
}
