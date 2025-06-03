
use legifrance::api::client::{AuthenticatedClient, PageQuery, ping_api};
use legifrance::api::piste::Fond;
use legifrance::api::{call_search_endpoint, get_full_texts};

use clap::Parser;

use std::path::PathBuf;

// implement value parser for Fond
use clap::ValueEnum;

#[derive(Debug, Clone, Copy)]
pub enum ParseableFond { P(Fond) }
impl ParseableFond {
    fn to_fond(&self) -> Fond {
        match self {
            ParseableFond::P(f) => *f,
        }
    }
}

pub const FONDS: [ParseableFond; 11] = [
    ParseableFond::P(Fond::Jorf),
    ParseableFond::P(Fond::Cnil),
    ParseableFond::P(Fond::Cetat),
    ParseableFond::P(Fond::Juri),
    ParseableFond::P(Fond::Jufi),
    ParseableFond::P(Fond::Constit),
    ParseableFond::P(Fond::Kali),
    ParseableFond::P(Fond::CodeDate),
    ParseableFond::P(Fond::LodaDate),
    ParseableFond::P(Fond::Circ),
    ParseableFond::P(Fond::Acco),
];

impl ValueEnum for ParseableFond {
    fn value_variants<'a>() -> &'a [Self] {
        &FONDS
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        match self {
            ParseableFond::P(f) => Some(clap::builder::PossibleValue::new(f.as_str())),
        } 
    }
}


/// This is a simple program to search the Legifrance API
/// it will search for relevant texts and return the results
/// in JSON format.
#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Cli {
    #[arg(short, long)]
    query: Option<String>,
    #[arg(short, long)]
    start_year: Option<u64>,
    #[arg(short, long)]
    end_year: Option<u64>,
    #[arg(short, long)]
    fond : Option<ParseableFond>,
    #[arg(short, long)]
    output: Option<String>,
    #[arg(short, long)]
    texts: Option<String>,
}

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

    let _ping = ping_api(&aclient, "/search/ping").await
        .expect("Failed to ping API");

    let cli = Cli::parse();

    if let Some(query) = cli.query {
        let pq = PageQuery {
            text: query,
            page: 1,
            start_year: cli.start_year,
            end_year: cli.end_year,
            fond: cli.fond.map(|f| f.to_fond()),
        };

        if let Some(output) = cli.output {
            let dir = PathBuf::from(output);
            let file = std::fs::File::create(&dir)
                .expect("Failed to create file");
            let writer = std::io::BufWriter::new(file);
            call_search_endpoint(&aclient, writer, &pq).await
                .expect("Failed to call search endpoint");
        } else {
            // use stdout
            let writer = std::io::stdout();
            call_search_endpoint(&aclient, writer, &pq).await
                .expect("Failed to call search endpoint");
        }
    }

    if let Some(texts) = cli.texts {
        let filename = PathBuf::from(texts);
        let file = std::fs::File::open(&filename)
            .expect("Failed to open file");
        let reader = std::io::BufReader::new(file);
        let dir = PathBuf::from("dilapi-full-texts");
        if !dir.exists() {
            std::fs::create_dir_all(&dir)
                .expect("Failed to create directory");
        }

        get_full_texts(aclient, &dir, reader)
            .await
            .expect("Failed to get full texts");
    }
}
