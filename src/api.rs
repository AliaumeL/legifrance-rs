use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};

use log::{info, warn, error};

use std::path::PathBuf;
use std::fs::File;
use std::io::{Write, BufWriter};
use std::sync::{Arc, Mutex};

use indicatif::{ProgressBar, MultiProgress, ProgressStyle};
use futures::stream::StreamExt;

/// This is the module containing the datatypes
/// for the API
pub mod piste;
pub mod client;

use client::{AuthenticatedClient, 
             PageQuery, 
             get_search_result,
             get_full_text};

type SharedBufferedWriter = Arc<Mutex<BufWriter<File>>>;

async fn get_page_and_write(aclient: &AuthenticatedClient,
                             out:     SharedBufferedWriter,
                             pq:      &PageQuery) -> Result<usize> {
    let res = get_search_result(aclient, pq).await
        .context("Unable to serialize search result")?;
    let total = res.total_result_number;
    info!("Total Results: {} for {:?}", total, pq);
    if let Ok(mut writer) = out.lock() {
        for doc in res.results.iter() {
            serde_json::to_writer(&mut *writer, doc)
                .context("Unable to serialize search result")?;
            writer.write_all(b"\n")
                .context("Unable to write newline")?;
        }
        Ok(res.results.len())
    } else {
        error!("Unable to lock the writer for {:?}", pq);
        return Err(anyhow::anyhow!("Unable to lock the writer"));
    }
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
        error!("Error: too many pages {} > 100 for query {:?}", page_count, pq.fond);
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


/// Split the year ranges into a list to fall below 
/// the 100 pages limitation.
fn split_year_ranges(pq : &PageQuery, step_size : usize) -> impl Iterator<Item = PageQuery> {

    info!("Splitting year ranges for {:?} using {}", pq, step_size);
    let start_year = pq.start_year.unwrap_or(1900);
    let end_year   = pq.end_year.unwrap_or(2025);

    (start_year..end_year)
        .step_by(step_size)
        .map(move |year| {
            let start = year;
            let end   = (year + (step_size as u64)).min(end_year);
            PageQuery {
                text: pq.text.clone(),
                page: pq.page,
                start_year: Some(start),
                end_year: Some(end),
                fond: pq.fond.clone(),
            }
        })
}

/// Find a good approximation for the step size in years
/// based on the total number of results claimed by the API.
///
/// We assume that results are uniformly distributed (which is not true).
fn compute_step_size(total: u64) -> usize {
    // we want to have at most 100 pages
    // and 100 results per page
    let max_results = 100 * 100;
    let step_size = (total as f64 / max_results as f64).ceil() as usize;
    (step_size / 3).min(1) // to be extra safe
}

/// Compute a probably correct list of queries
/// to execute in order to get all the results.
/// 
/// 1. It will first get the total number of results
/// 2. It uses it to compute a step size
/// 3. For every year range, it computes a pagination
/// 4. It returns a list of PageQuery
async fn compute_query_plan(aclient: &AuthenticatedClient,
                      pq:      &PageQuery) -> Result<(u64, Vec<PageQuery>)> {
    info!("Computing query plan for {:?}", pq);
    // get the total number of results
    let res = get_search_result(aclient, pq).await?;
    let total = res.total_result_number;
    info!("Total Results: {} for {:?}", total, pq);

    // heuristically compute the step size
    let step_size = compute_step_size(total);
    info!("Step size: {} for {:?}", step_size, pq);
    
    // The list of queries to run in the end
    let mut queries = vec![];

    // create a stream of futures responsible 
    // for computing the pagination for each year range
    // and execute them in parallel by using at most 10 
    // concurrent tasks
    let stream = futures::stream::iter(split_year_ranges(pq, step_size))
        .map(|q| {
            let aclient = &aclient;
            async move {
                // compute the pagination for each year range
                let pqs = compute_pagination(aclient, &q).await?;
                Ok(pqs)
            }
        });

    // execute the stream using ordered concurrency
    // with a limit of 10 concurrent tasks
    let results = stream.buffer_unordered(10).collect::<Vec<Result<Vec<PageQuery>>>>().await;
    info!("Finished computing pagination, {} queries will be run", results.len());
    for res in results {
        match res {
            Ok(pqs) => {
                queries.extend(pqs);
            }
            Err(e) => {
                error!("Could not correctly compute pagination! {}", e);
            }
        }
    }

    Ok((total, queries))
}

async fn store_all_to_file(aclient: &AuthenticatedClient,
                           out:     SharedBufferedWriter,
                           bar:     &ProgressBar,
                           pqs:     &[PageQuery]) -> Result<()> {

    let stream = futures::stream::iter(pqs)
        .map(|pq| {
            let aclient = &aclient;
            let out     = out.clone();
            let bar     = &bar;
            async move {
                match get_page_and_write(aclient, out, &pq).await {
                    Ok(res_count) => {
                        bar.inc(res_count as u64);
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

pub async fn call_search_endpoint(aclient: &AuthenticatedClient, 
                                  dir    : &PathBuf,
                                  pq     : &PageQuery) -> Result<()> {
    // create the output directory if it does not exist
    if !dir.exists() {
        std::fs::create_dir_all(dir)
            .context(format!("Unable to create directory {}", dir.display()))?;
    }
    // create the output file
    let filename = format!("{}-{}.json", pq.text, pq.fond.as_ref().map(|x| x.as_str()).unwrap_or("all"));
    let filepath = dir.join(filename);
    let file = File::create(&filepath)
        .context(format!("Unable to create file {}", filepath.display()))?;
    info!("Writing results to {}", filepath.display());
    let out = Arc::new(Mutex::new(BufWriter::new(file)));
    

    // get the total number of results
    // and compute the pagination
    let (total, pqs) = compute_query_plan(aclient, pq).await
        .context("Unable to compute query plan")?;

    info!("Total Results: {} for {} total queries to run", total, pqs.len());

    let bar = ProgressBar::new(0);
    bar.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} {msg} [{elapsed_precise}] {wide_bar} {pos}/{len} ({eta})")
        .context("Unable to set progress bar style")?
        .progress_chars("##-"));
    bar.set_message(format!("Getting results for {}", pq.text));
    bar.set_position(0);
    bar.set_length(total);

    bar.set_message(format!("Getting results for {} - {} pages", pq.text, pqs.len()));

    store_all_to_file(aclient, out.clone(), &bar, &pqs).await
        .context("Unable to store results to file")?;
    
    Ok(())

}

///
pub async fn get_full_texts<R>(aclient: AuthenticatedClient,
                               reader:  R) -> Result<()>
where R: std::io::Read + std::marker::Send + 'static + std::io::Seek
{
    use std::io::BufRead;
    use crossbeam_channel::{bounded, Receiver, Sender};
    let reader = std::io::BufReader::new(reader);

    let pb = ProgressBar::new(0);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} {msg} [{elapsed_precise}] {wide_bar} {pos}/{len} ({eta})")
        .context("Unable to set progress bar style")?
        .progress_chars("##-"));
    pb.set_message("Getting full texts");
    pb.set_position(0);

    info!("Getting full texts from input");

    let (tx, rx) : (Sender<String>, 
                    Receiver<String>) = bounded(50000);

    let aclient = Arc::new(aclient);

    info!("Starting workers");
    let mut handles = vec![];
    for _ in 0..5 {
        let aclient = aclient.clone();
        let arx     = rx.clone();
        let pb      = pb.clone();
        let handle = tokio::task::spawn(async move {
            while let Ok(txt) = arx.recv() {
                if let Ok(search_result) = serde_json::from_str::<piste::SearchResult>(&txt) {
                    if let Some(fond) = search_result.fond {
                        if let Some(cid) = search_result.titles.get(0)
                            .map(|t| t.cid.clone()) {
                            info!("Got cid {}", cid);
                            match get_full_text(&aclient, &cid, &fond).await {
                                Ok(text) => {
                                    pb.inc(1);
                                    info!("Got full text for {}", cid);
                                    let filename = format!("{}.txt", cid);
                                    let filepath = PathBuf::from("output").join(filename);
                                    let file = File::create(&filepath)
                                        .expect("Unable to create file");
                                    let mut writer = BufWriter::new(file);
                                    writer.write_all(text.as_bytes())
                                        .expect("Unable to write full text");
                                    writer.flush()
                                        .expect("Unable to flush writer");

                                    info!("Wrote full text to {}", filepath.display());
                                }
                                Err(e) => {
                                    error!("Error: {}", e);
                                }
                            }
                        }
                    } else {
                        error!("Error: no cid found in search result");
                    }
                } else {
                    error!("Error: unable to parse search result");
                }

            }
        });
        handles.push(handle);
    }

    info!("Workers started");
    let mut count = 0;
    // read the input file line by line
    // and send the cids to the channel
    for line in reader.lines() {
        count += 1;
        pb.set_length(count as u64);
        let line = line?;
        let cid = line.trim().to_string();
        if !cid.is_empty() {
            tx.send(cid)
              .context("Unable to send cid to channel")?;
        }
    }
    info!("Finished reading input file");

    // close the channel
    // this will signal the workers to stop
    drop(tx);
    info!("Waiting for workers to finish");
    // wait for all the workers to finish
    for handle in handles {
        handle.await
              .context("Unable to join worker thread")?;
    }
    info!("All workers finished");

    Ok(())
}

