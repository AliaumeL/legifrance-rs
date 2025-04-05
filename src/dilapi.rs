
mod api;

use api::call_api;

#[tokio::main]
async fn main () {
    env_logger::init();
    call_api().await.unwrap()
}
