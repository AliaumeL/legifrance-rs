
use legifrance::api::client::{AuthenticatedClient, PageQuery, ping_api};
use legifrance::api::piste::Fond;
use legifrance::api::{call_search_endpoint, get_full_texts};

use std::path::PathBuf;

// json
use serde_json::json;

// Create a command line interface.
// - search query 
// - save-results
// - input ids/cids to fetch
// - populate text cache

#[tokio::main]
async fn main () {
    env_logger::init();
    
    let client_id     = std::fs::read_to_string("client-id.txt")
        .expect("Failed to read client-id.txt");
    let client_secret = std::fs::read_to_string("client-secret.txt")
        .expect("Failed to read client-secret.txt");

    let aclient = AuthenticatedClient::from_secret(&client_id, &client_secret)
        .await
        .expect("Failed to create authenticated client");

    let ping = ping_api(&aclient, "/search/ping").await
        .expect("Failed to ping API");

    let data =  &json!({
            "id": "CETATEXT000008234002",
        });
    let response = aclient.post_json_request(
        "/consult/getArticle",
        &serde_json::to_string(data).unwrap()
    ).await
        .expect("Failed to get article");


    println!("Ping response: {:?}", ping);
    println!("Response: {:?}", response);
    println!("Response status: {:?}", response.status());
    println!("Response text: {:?}", response.text().await);
    /*

    let q = PageQuery {
            text: "ceseda".to_owned(), 
            page: 1, 
            start_year: Some(2000), 
            end_year:   Some(2025), 
            fond:       Some(Fond::Cetat)
    };

    let dir = PathBuf::from("output");

    let _ = call_search_endpoint(&aclient, &dir, &q).await
        .expect("Failed to call search endpoint");

    */

    let filename = PathBuf::from("output/ceseda-CETAT.json");
    let file = std::fs::File::open(&filename)
        .expect("Failed to open file");
    let reader = std::io::BufReader::new(file);


    get_full_texts(aclient, reader)
        .await
        .expect("Failed to get full texts");


}
