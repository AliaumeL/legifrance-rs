
use legifrance::api::client::{AuthenticatedClient, PageQuery, ping_api, get_search_result};
use legifrance::api::example_text_extraction;

// json
use serde_json::json;

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
            "id": "LEGIARTI000033219357"
        });
    let response = aclient.post_json_request(
        "/consult/getArticle",
        &serde_json::to_string(data).unwrap()
    ).await
        .expect("Failed to get article");

    println!("Ping response: {:?}", ping);
    println!("Response: {:?}", response);

    let txt = example_text_extraction(&aclient).await
        .expect("Failed to extract text");

    println!("Text extraction response: {:?}", txt);


    let some_docs = get_search_result(
        &aclient,
        &PageQuery { text: "ceseda".to_owned(), page: 1, start_year: None, end_year: None, fond: None }
    ).await
        .expect("Failed to get search results");

    println!("Search results: {:?}", some_docs.total_result_number);

    println!("Search results: {:?}", some_docs.results);


}
