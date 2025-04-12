use anyhow::{Context, Result};
///
/// This file contains the code to wrap the interaction
/// with the API in a somewhat more user-friendly way.
/// The interface to this module should be more stable
/// than the API itself.
///
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;

use log::{error, info, warn};

use crate::api::piste::*;

/// An authenticated client to the API. This is the
/// data structure that should be used to interact with the API.
pub struct AuthenticatedClient {
    client: Client,
    token: AuthResponse,
}

/// Authenticate to the API to get a token
/// The token is valid for 1 hour.
async fn authenticate(
    client: &Client,
    client_id: &str,
    client_secret: &str,
) -> Result<AuthResponse> {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::CONTENT_TYPE,
        reqwest::header::HeaderValue::from_static("application/x-www-form-urlencoded"),
    );
    let body = AuthBody {
        grant_type: "client_credentials".to_string(),
        client_id: client_id.trim().to_string(),
        client_secret: client_secret.trim().to_string(),
        scope: "openid".to_string(),
    };
    let encoded_body = serde_urlencoded::to_string(&body)?;
    let response = client
        .post(OAUTH_URL)
        .headers(headers)
        .body(encoded_body)
        .send()
        .await?;
    if !response.status().is_success() {
        error!("Error: unable to authenticate {}", response.status());
        return Err(anyhow::anyhow!(
            "Error: unable to authenticate {}",
            response.status()
        ));
    }
    let token = response.text().await?;
    info!("Authenticated to the API");
    Ok(serde_json::from_str(&token)?)
}

impl AuthenticatedClient {
    pub async fn renew(&mut self, id: &str, secret: &str) -> Result<()> {
        info!("Renewing the token");
        let token = authenticate(&self.client, id, secret)
            .await
            .context("Unable to renew token")?;
        self.token = token;
        Ok(())
    }

    pub fn from_token(client: Client, token: AuthResponse) -> Self {
        AuthenticatedClient { client, token }
    }

    pub async fn from_secret(id: &str, secret: &str) -> Result<Self> {
        info!("Authenticating to the API");
        let client = Client::new();
        let token = authenticate(&client, id, secret)
            .await
            .context("Unable to authenticate")?;
        Ok(AuthenticatedClient { client, token })
    }

    pub async fn post_json_request(&self, entpoint: &str, body: &str) -> Result<reqwest::Response> {
        let mut headers = reqwest::header::HeaderMap::new();
        let url = format!("{}{}", API_URL, entpoint);

        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&format!("Bearer {}", self.token.access_token))?,
        );
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/json"),
        );
        headers.insert(
            reqwest::header::ACCEPT,
            reqwest::header::HeaderValue::from_static("application/json"),
        );

        let request = self
            .client
            .post(url)
            .headers(headers)
            .body(body.to_string());
        let response = request.send().await?;
        Ok(response)
    }

    pub async fn get_request(&self, endpoint: &str) -> Result<reqwest::Response> {
        let mut headers = reqwest::header::HeaderMap::new();
        let url = format!("{}{}", API_URL, endpoint);
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&format!("Bearer {}", self.token.access_token))?,
        );
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/json"),
        );
        let response = self.client.get(url).headers(headers).send().await?;
        Ok(response)
    }
}

pub async fn ping_api(aclient: &AuthenticatedClient, endpoint: &str) -> Result<()> {
    info!("Pinging the API {endpoint}");
    let response = aclient.get_request(&endpoint).await?;
    if response.status().is_success() {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "Error: unable to ping api {}",
            response.status()
        ))
    }
}

/// Abstracted Search Query.
/// This is the interface exposed to the user.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PageQuery {
    /// The text to search for
    pub text: String,
    /// The page number to get
    pub page: u8,
    pub start_year: Option<u64>,
    pub end_year: Option<u64>,
    /// The `fond` (dataset) to search in.
    /// None means all datasets simultaneously.
    pub fond: Option<Fond>,
}

impl From<&PageQuery> for SearchQuery {
    fn from(pq: &PageQuery) -> Self {
        let sort = if pq.start_year.is_some() && pq.end_year.is_some() {
            Some("SIGNATURE_DATE_DESC".to_string())
        } else {
            None
        };

        let filters = if pq.start_year.is_some() && pq.end_year.is_some() {
            Some(vec![Filter {
                dates: DateRange {
                    start: format!("{}-01-01", pq.start_year.unwrap()),
                    end: format!("{}-01-01", pq.end_year.unwrap()),
                },
                facette: FilterType::DecisionDate,
            }])
        } else {
            None
        };

        let fond = match &pq.fond {
            Some(fond) => fond.as_str().to_string(),
            None => "ALL".to_string(),
        };

        SearchQuery {
            search: Search {
                from_advanced: false,
                fields: vec![Field {
                    constraints: vec![Constraint {
                        value: pq.text.clone(),
                        fuzzy: 2,
                        operator: Operator::And,
                        match_type: MatchType::OneOfTheWords,
                    }],
                    operator: Operator::And,
                    field_type: FieldType::All,
                }],
                page_size: 100,
                operator: Operator::And,
                pagination: Pagination::Default,
                page_number: pq.page,
                filters,
                sort,
                second_sort: Some("ID".to_string()),
            },
            fond,
        }
    }
}

///
/// Get the search results
/// return a list of search results
///
pub async fn get_search_result(
    aclient: &AuthenticatedClient,
    pq:      &PageQuery,
) -> Result<SearchResponse> {
    let query: SearchQuery = pq.into();
    let data = serde_json::to_string(&query)?;

    let response = aclient.post_json_request("/search", &data).await?;

    if response.status().is_success() {
        let text = response.text().await?;
        let res: SearchResponse = serde_json::from_str(&text)?;
        Ok(res)
    } else {
        let status = response.status();
        let text = response.text().await?;
        warn!("Error: unable to get search results {}", status);
        warn!("Response: {:?}", text);
        Err(anyhow::anyhow!(
            "Error: unable to get search results {}",
            status
        ))
    }
}

///
/// Obtain the full text of an article / law decree / decision
/// based on its ID and the dataset it belongs to (fond).
///
/// It is unclear which `Fond` one should use. 
///
pub async fn get_full_text(aclient: &AuthenticatedClient, cid: &str, fond: &Fond) -> Result<String> {
    let generic_endpoint = "/consult/getArticle";
    let endpoint = fond.api_consult_endpoint().unwrap_or(generic_endpoint);

    info!(
        "Getting full document for {} using {}",
        cid, endpoint
    );

    let data = &json!({
        "id":      cid,
        "cid":     cid,
        "textId":  cid,
        "textCid": cid,
    });

    let response = aclient
        .post_json_request(endpoint, &serde_json::to_string(data)?)
        .await?;

    if response.status().is_success() {
        let text = response.text().await?;
        let parsed: serde_json::Value = serde_json::from_str(&text)?;
        let out = parsed["text"]["texte"]
            .as_str()
            .unwrap_or("")
            .to_string();
        Ok(out)
    } else {
        let status = response.status();
        let text = response.text().await?;
        error!("Error: unable to get full document {}", status);
        error!("Response: {:?}", text);
        Err(anyhow::anyhow!(
            "Error: unable to get full document {}",
            status
        ))
    }
}
