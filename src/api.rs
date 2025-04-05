

use anyhow::{Result, Context};
use reqwest::Client;
use serde::{Deserialize, Serialize};

// logging
use log::{info, warn};

use std::path::PathBuf;


use indicatif::{ProgressBar, MultiProgress, ProgressStyle};
use futures::stream::StreamExt;

/// The body of the request to authenticate
#[derive(Serialize, Deserialize)]
struct AuthBody {
    grant_type: String,
    client_id: String,
    client_secret: String,
    scope: String,
}

/// The response of the authentication
/// The token is valid for 1 hour.
#[derive(Serialize, Deserialize)]
struct AuthResponse {
    access_token: String,
    token_type: String,
    expires_in: u64,
    scope: String,
}

/// Search Query
/// The search query is a JSON object
/// that contains the search criteria
#[derive(Serialize, Deserialize)]
struct SearchQuery {
    recherche: Recherche,
    fond: String,
    filtres: Option<Vec<Filtre>>,
    sort: Option<String>,
    secondSort: Option<String>,
}
#[derive(Serialize, Deserialize)]
struct Filtre {
    dates: Dates,
    facette: String,
}
#[derive(Serialize, Deserialize)]
struct Dates {
    start: String,
    end: String,
}
#[derive(Serialize, Deserialize)]
struct Recherche {
    fromAdvancedRecherche: bool,
    champs: Vec<Champ>,
    pageSize: u64,
    operateur: String,
    typePagination: String,
    pageNumber: u64,
}
#[derive(Serialize, Deserialize)]
struct Champ {
    criteres: Vec<Critere>,
    operateur: String,
    typeChamp: String,
}
#[derive(Serialize, Deserialize)]
struct Critere {
    valeur: String,
    proximite: i64,
    operateur: String,
    typeRecherche: String,
}

/// Search Response
#[derive(Serialize, Deserialize)]
struct SearchResponse {
    totalResultNumber: i64,
    results: Vec<serde_json::Value>,
}


/// The base URL of the API
const API_URL: &str = "https://api.piste.gouv.fr/dila/legifrance/lf-engine-app";

/// The base URL of the OAuth API
const OAUTH_URL: &str = "https://oauth.piste.gouv.fr/api";


/// Authenticate to the API to get a token
/// The token is valid for 1 hour.
///
/// endpoint: /oauth/token
async fn authenticate(client: &Client, client_id: &str, client_secret: &str) -> Result<AuthResponse> {
    info!("Authenticating to the API");

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::CONTENT_TYPE,
        reqwest::header::HeaderValue::from_static("application/x-www-form-urlencoded"),
    );
    let url = format!("{}/oauth/token", OAUTH_URL);
    let body = AuthBody {
        grant_type: "client_credentials".to_string(),
        client_id: client_id.trim().to_string(),
        client_secret: client_secret.trim().to_string(),
        scope: "openid".to_string(),
    };
    let encoded_body = serde_urlencoded::to_string(&body)?;
    let response = client.post(&url).headers(headers).body(encoded_body).send().await?;
    let token = response.text().await?;
    Ok(serde_json::from_str(&token)?)
}

/// Ping the API to check if it is available
async fn ping_api(client: &Client, token: &AuthResponse) -> Result<()> {
    info!("Pinging the API");
    let url = format!("{}/search/ping", API_URL);
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token.access_token))?,
    );
    headers.insert(
        reqwest::header::CONTENT_TYPE,
        reqwest::header::HeaderValue::from_static("application/json"),
    );
    let response = client.get(&url).headers(headers).send().await?;
    if response.status().is_success() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("Error: unable to ping api {}", response.status()))
    }
}

/// Possible values for the fond parameter
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "UPPERCASE")]
enum Fond {
    JORF,
    CNIL,
    CETAT,
    JURI,
    JUFI,
    CONSTIT,
    KALI,
    #[allow(non_camel_case_types)]
    CODE_DATE,
    #[allow(non_camel_case_types)]
    LODA_DATE,
    CIRC,
    ACCO,
}

const FONDS: [Fond; 11] = [
    Fond::JORF,
    Fond::CNIL,
    Fond::CETAT,
    Fond::JURI,
    Fond::JUFI,
    Fond::CONSTIT,
    Fond::KALI,
    Fond::CODE_DATE,
    Fond::LODA_DATE,
    Fond::CIRC,
    Fond::ACCO,
];

impl Fond {
    fn as_str(&self) -> &'static str {
        match self {
            Fond::JORF => "JORF",
            Fond::CNIL => "CNIL",
            Fond::CETAT => "CETAT",
            Fond::JURI => "JURI",
            Fond::JUFI => "JUFI",
            Fond::CONSTIT => "CONSTIT",
            Fond::KALI => "KALI",
            Fond::CODE_DATE => "CODE_DATE",
            Fond::LODA_DATE => "LODA_DATE",
            Fond::CIRC => "CIRC",
            Fond::ACCO => "ACCO",
        }
    }

    fn api_consult_endpoint(&self) -> Option<&'static str> {
        match self {
            Fond::JORF => Some("/consult/jorf"),
            Fond::CNIL => Some("/consult/cnil"),
            Fond::CETAT => None,
            Fond::JURI => Some("/consult/juri"),
            Fond::JUFI => None,
            Fond::CONSTIT => None,
            Fond::KALI => Some("/consult/kaliCont"),
            Fond::CODE_DATE => Some("/consult/code"),
            Fond::LODA_DATE => Some("/consult/law_decree"),
            Fond::CIRC => Some("/consult/circulaire"),
            Fond::ACCO => Some("/consult/acco"),
        }
    }
}

impl std::fmt::Display for Fond {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Abstracted Search Query
#[derive(Serialize, Deserialize, Clone, Debug)]
struct PageQuery {
    text:       String,
    page:       u64,
    start_year: Option<i64>,
    end_year:   Option<i64>,
    /// None means all at once
    fond:       Option<Fond>,
}

impl From<&PageQuery> for SearchQuery {
    fn from(pq: &PageQuery) -> Self {
        let sort = if pq.start_year.is_some() && pq.end_year.is_some() {
            Some("SIGNATURE_DATE_DESC".to_string())
        } else {
            None
        };
        let filtres = if pq.start_year.is_some() && pq.end_year.is_some() {
            Some(vec![Filtre {
                dates: Dates {
                    start: format!("{}-01-01", pq.start_year.unwrap()),
                    end:   format!("{}-01-01", pq.end_year.unwrap()),
                },
                facette: "DATE_SIGNATURE".to_string(),
            }])
        } else {
            None
        };

        let fond = match &pq.fond {
            Some(fond) => fond.as_str().to_string(),
            None => "ALL".to_string(),
        };

        SearchQuery {
            recherche: Recherche {
                fromAdvancedRecherche: false,
                champs: vec![
                    Champ {
                        criteres: vec![
                            Critere {
                                valeur: pq.text.clone(),
                                proximite: 2,
                                operateur: "ET".to_string(),
                                typeRecherche: "UN_DES_MOTS".to_string(),
                            }
                        ],
                        operateur: "ET".to_string(),
                        typeChamp: "ALL".to_string(),
                    }
                ],
                pageSize: 100,
                operateur: "ET".to_string(),
                typePagination: "DEFAUT".to_string(),
                pageNumber: pq.page,
            },
            fond,
            filtres,
            sort,
            secondSort: Some("ID".to_string()),
        }
    }
}


/// Get the search results
/// return a list of results (JSON)
async fn get_search_result(client: &Client, token: &AuthResponse, pq: &PageQuery)
    -> Result<SearchResponse> {
    let url = format!("{}/search", API_URL);
    let mut headers = reqwest::header::HeaderMap::new();

    headers.insert(
        reqwest::header::AUTHORIZATION,
        reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token.access_token))?,
    );
    headers.insert(
        reqwest::header::CONTENT_TYPE,
        reqwest::header::HeaderValue::from_static("application/json"),
    );

    let query: SearchQuery = pq.into();

    let data = serde_json::to_string(&query)?;
    let response = client.post(&url).headers(headers).body(data).send().await?;

    if response.status().is_success() {
        let text = response.text().await?;
        let res: SearchResponse = serde_json::from_str(&text)?;
        Ok(res)
    } else {
        let status = response.status();
        let text = response.text().await?;
        warn!("Error: unable to get search results {}", status);
        warn!("Response: {:?}", text);
        Err(anyhow::anyhow!("Error: unable to get search results {}", status))
    }
}

async fn get_page_and_write(client: &Client, 
                            dir:    &PathBuf,
                            token:  &AuthResponse, 
                            pq:     &PageQuery) -> Result<()> {
    // if the file already exists, skip
    let filename = match (&pq.fond, pq.start_year, pq.end_year) {
        (Some(f), Some(s), Some(e)) => format!("{}-DATE-{}-{}-PAGE-{}.json", f, s, e, pq.page),
        (None   , Some(s), Some(e)) => format!("ALL-DATE-{}-{}-PAGE-{}.json", s, e, pq.page),
        (Some(f), _, _)             => format!("{}-NODATE-PAGE-{}.json", f, pq.page),
        (None, _, _)                => format!("ALL-NODATE-PAGE-{}.json", pq.page),
    };

    let filepath = dir.join(&filename);

    if std::path::Path::new(&filepath).exists() {
        info!("File {} already exists, skipping", filename);
        return Ok(());
    }

    let res = get_search_result(client, token, pq).await?;
    let total = res.totalResultNumber;
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
async fn compute_pagination(client: &Client, 
                            token:  &AuthResponse, 
                            pq:     &PageQuery)
    -> Result<Vec<PageQuery>> {
    // get the total number of pages
    let res = get_search_result(client, token, pq).await?;
    let total = res.totalResultNumber;

    // compute the number of pages
    let page_count = (total as f64 / 100.0).ceil() as u64;

    if page_count > 100 {
        return Err(anyhow::anyhow!("Error: too many pages {} > 100", page_count));
    }

    // create a stream of pages
    let pages = (1..=page_count).map(|page| {
        PageQuery {
            text: pq.text.clone(),
            page,
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
                        start_year: Some(*start_year as i64),
                        end_year: Some(*end_year as i64),
                        fond: None,
                    }]
            }
            _ => {
                fonds.iter().map(|fond| {
                    PageQuery {
                        text: text.to_string(),
                        page: 1,
                        start_year: Some(*start_year as i64),
                        end_year: Some(*end_year as i64),
                        fond: Some(fond.clone()),
                    }
                }).collect::<Vec<_>>()
            }
        }
    })
}

async fn download_all_pages(client: &Client, 
                            token:  &AuthResponse, 
                            dir:    &PathBuf,
                            mb:     &MultiProgress,
                            pq:     &PageQuery) -> Result<()> {

    let pqs = compute_pagination(&client, &token, &pq).await?;
    let bar = mb.add(ProgressBar::new(0));
    bar.set_style(ProgressStyle::default_bar()
        .template("{msg} [{elapsed_precise}] {wide_bar} {pos:>7}/{len:7} ({eta})")
        .context("Unable to create progress bar")?
        .progress_chars("##-"));
    bar.set_message(format!("[{:?}] {:?}-{:?}-{:?}", pq.fond, pq.text, pq.start_year, pq.end_year));
    bar.set_length(pqs.len() as u64);

    let stream = futures::stream::iter(pqs)
        .map(|pq| {
            let client = &client;
            let token  = &token;
            let dir    = &dir;
            let bar   = &bar;
            async move {
                match get_page_and_write(client, dir, token, &pq).await {
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

async fn example_text_extraction(
    client: &Client,
    token: &AuthResponse,
    ) -> Result<()> {

    let asr = AbstractSearchResult {
        id: "LEGIARTI000042946979".to_string(),
        fond: Fond::CODE_DATE,
    };

    let url = format!("{}{}",
                      API_URL, 
                      asr.fond.api_consult_endpoint().unwrap());

    info!("Extracting text from {}", url);

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token.access_token))?,
    );
    headers.insert(
        reqwest::header::CONTENT_TYPE,
        reqwest::header::HeaderValue::from_static("application/json"),
    );
    let response = client
        .post(&url)
        .headers(headers)
        .body(r#"
            {
                "id":      "LEGIARTI000042946979",
                "cid":     "LEGIARTI000042946979",
                "texteId": "LEGIARTI000042946979",
            }"#)
        .send()
        .await?;
    if response.status().is_success() {
        println!("Success: {}", response.status());
        let text = response.text().await?;
        println!("Response: {:?}", text);
        let res: serde_json::Value = serde_json::from_str(&text)?;
        println!("Result: {:?}", res);
    } else {
        let status = response.status();
        warn!("Error: unable to get search results {}", status);
        let text = response.text().await?;
        warn!("Response: {:?}", text);
    }

    Ok(())

}

pub async fn call_api() -> Result<()> {
    let client = Client::new();
    // read the client_id and client_secret from a file
    // and strip the newline character
    let client_id     = std::fs::read_to_string("client-id.txt")?;
    let client_secret = std::fs::read_to_string("client-secret.txt")?;

    let token = authenticate(&client, &client_id, &client_secret).await?;

    example_text_extraction(&client, &token).await?;

    return Ok(());

    ping_api(&client, &token).await?;

    let fonds = vec![
        Fond::JORF,
        Fond::CNIL,
        // Fond::CETAT,
        Fond::JURI,
        Fond::JUFI,
        Fond::CONSTIT,
        Fond::KALI,
        Fond::CODE_DATE,
        Fond::LODA_DATE,
        Fond::CIRC,
        Fond::ACCO,
    ];
    let year_ranges = vec![
        (1950, 2000),
        (2000, 2025),
    ];
    let text = "CESEDA";



    let mb = MultiProgress::new();
    let dir    = PathBuf::from("data");
    let stream = futures::stream::iter(compute_prequeries(text, &fonds, &year_ranges))
        .map(|pq| {
            let client = &client;
            let token  = &token;
            let mb     = &mb;
            let dir    = &dir;
            async move {
                info!("Downloading pages for {:?}", &pq);
                download_all_pages(client, token, dir, mb, &pq).await
            }
        });

    // execute the stream using 10 concurrent tasks
    let _ = stream.buffer_unordered(4).collect::<Vec<Result<()>>>().await;

    Ok(())
}
