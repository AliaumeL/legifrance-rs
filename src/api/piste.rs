///
/// This files contains the data structures needed to represent queries
/// and responses to the Piste API for legifrance.
/// It is separated from any logic to make it easier to test and 
/// maintain, in particular to witness changes in the API.
///
/// The api is documented at
/// <https://piste.gouv.fr/index.php?option=com_apiportal&view=apitester&usage=api&apitab=tests&apiName=L%C3%A9gifrance&apiId=7e5a0e1d-ffcc-40be-a405-a1a5c1afe950&managerId=3&type=rest&apiVersion=2.4.2&Itemid=202&swaggerVersion=2.0&lang=en>
///
/// We do not fully implement the API, but only the parts we need.
///
///
/// We also translate the API documentation to English, 
/// so that it feels more consistent with the rest of the code.

use serde::{Deserialize, Serialize};

/// The version of the API we are interacting with.
pub const VERSION  : &str = "2.4.2";

/// The endpoint for authenticating to the API.
pub const OAUTH_URL: &str = "https://oauth.piste.gouv.fr/api/oauth/token";

/// The endpoint for the API (production).
pub const API_URL : &str = "https://api.piste.gouv.fr/dila/legifrance/lf-engine-app";

/// All available fonds (datasets) in the API.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "UPPERCASE")]
pub enum Fond {
    /// The official journal of the French Republic.
    Jorf,
    /// The French National Commission on Informatics and Liberty.
    Cnil,
    Cetat,
    Juri,
    Jufi,
    Constit,
    Kali,
    #[serde(rename = "CODE_DATE")]
    CodeDate,
    #[serde(rename = "LODA_DATE")]
    LodaDate,
    Circ,
    Acco,
}

pub const FONDS: [Fond; 11] = [
    Fond::Jorf,
    Fond::Cnil,
    Fond::Cetat,
    Fond::Juri,
    Fond::Jufi,
    Fond::Constit,
    Fond::Kali,
    Fond::CodeDate,
    Fond::LodaDate,
    Fond::Circ,
    Fond::Acco,
];

impl Fond {
    pub fn as_str(&self) -> &'static str {
        match self {
            Fond::Jorf => "JORF",
            Fond::Cnil => "CNIL",
            Fond::Cetat => "CETAT",
            Fond::Juri => "JURI",
            Fond::Jufi => "JUFI",
            Fond::Constit => "CONSTIT",
            Fond::Kali => "KALI",
            Fond::CodeDate => "CODE_DATE",
            Fond::LodaDate => "LODA_DATE",
            Fond::Circ => "CIRC",
            Fond::Acco => "ACCO",
        }
    }

    pub fn api_consult_endpoint(&self) -> Option<&'static str> {
        match self {
            Fond::Jorf => Some("/consult/jorf"),
            Fond::Cnil => Some("/consult/cnil"),
            Fond::Cetat => None,
            Fond::Juri => Some("/consult/juri"),
            Fond::Jufi => None,
            Fond::Constit => None,
            Fond::Kali => Some("/consult/kaliCont"),
            Fond::CodeDate => Some("/consult/code"),
            Fond::LodaDate => Some("/consult/law_decree"),
            Fond::Circ => Some("/consult/circulaire"),
            Fond::Acco => Some("/consult/acco"),
        }
    }
}

impl std::fmt::Display for Fond {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}



/// The body of the request to authenticate to the API.
/// 
/// This should be sent as a JSON object in the body of a POST request
/// to the authentication endpoint in the global variable `API_OAUTH`.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AuthBody {
    pub grant_type:    String,
    pub client_id:     String,
    pub client_secret: String,
    pub scope:         String,
}

/// The response from the API when authenticating.
/// 
/// Note that typically the token is valid for 1 hour.
/// To use this token, it suffices to place it in the header of the request
/// to the API, in the `Authorization` field, prefixed by the string `Bearer `.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AuthResponse {
    pub access_token: String,
    pub token_type:   String,
    pub expires_in:   u64,
    pub scope:        String,
}


/// An object representing a search query.
/// This is the object that is sent to the API on /search-like endpoints.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SearchQuery {
    #[serde(rename = "recherche")]
    pub search:      Search,
    pub fond:        String,
}

/// The object representing the search criteria.
/// 
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Search {
    /// Whether the search is “advanced”, this is 
    /// tied to the way the search bar of <legifrance.fr> is
    /// built and should probably be set to false
    #[serde(rename = "fromAdvancedRecherche")]
    pub from_advanced: bool,
    /// Here we specify the type of search we want to do
    /// by listing the fields we want to search in
    /// and their respective constraints.
    #[serde(rename = "champs")]
    pub fields: Vec<Field>,
    /// Page size should be between 1 and 100 inclusive.
    #[serde(rename = "pageSize")]
    pub page_size: u8,
    /// The operator to use for the search.
    /// This is can be `AND` or `OR`, and it controls
    /// how the search terms are combined in `champs`.
    #[serde(rename = "operateur")]
    pub operator: Operator,
    /// The type of pagination to use. This is probably best left as `Default`.
    #[serde(rename = "typePagination")]
    pub pagination: Pagination,
    /// Page numbers should be between 1 and 100 inclusive.
    #[serde(rename = "pageNumber")]
    pub page_number: u8,
    /// Filters to apply to the search.
    #[serde(rename = "filtres")]
    pub filters:     Option<Vec<Filter>>,
    /// The sort order to use for the search.
    pub sort:        Option<String>,
    /// The second sort order to use for the search (in case of ties).
    #[serde(rename = "secondSort")]
    pub second_sort: Option<String>,
}

/// The type of pagination to use. 
/// The behavior of `Article` is not yet documented.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Pagination {
    #[serde(rename = "DEFAUT")]
    Default,
    #[serde(rename = "ARTICLE")]
    Article
}

/// A logical operator for combining constraints
/// can only be conjunction or disjunction.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Operator {
    #[serde(rename = "ET")]
    And,
    #[serde(rename = "OU")]
    Or,
}

/// A filter to apply to the search. For now,
/// only date filters are supported.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Filter {
    /// The date range to use as a filter.
    pub dates:   DateRange,
    /// The field over which to apply the filter.
    pub facette: FilterType,
}

/// The field over which to apply a given filter.
/// For now, we only listed some 
/// possibilities for date fields.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum FilterType {
    #[serde(rename = "DATE_DECISION")]
    DecisionDate,
    #[serde(rename = "DATE_SIGNATURE")]
    SignatureDate,
    #[serde(rename = "DATE_PUBLICATION")]
    PublicationDate,
    #[serde(rename = "DATE_EFFET")]
    EffectDate
}

/// The date range to use for the search.
/// Dates should be strings in the format `YYYY-MM-DD`.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DateRange {
    pub start: String,
    pub end:   String,
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Field {
    #[serde(rename = "criteres")]
    pub constraints: Vec<Constraint>,
    #[serde(rename = "operateur")]
    pub operator: Operator,
    #[serde(rename = "typeChamp")]
    pub field_type: FieldType,
}

/// The criteria to use for the search.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Constraint {
    /// The expected value (e.g. search term).
    #[serde(rename = "valeur")]
    pub value: String,
    /// Maximal edit distance to use for the search.
    /// Can be 0, 1, or 2.
    #[serde(rename = "proximite")]
    pub fuzzy: u8,
    #[serde(rename = "operateur")]
    pub operator: Operator,
    #[serde(rename = "typeRecherche")]
    pub match_type: MatchType,
}

/// The response from the API when searching.
/// Note that we leave unspecified the `results` field
/// for now.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SearchResponse {
    /// The total number of results that could match this query
    /// (note that this can be larger than the number of pages × page size,
    /// so some of them can be inaccessible).
    #[serde(rename = "totalResultNumber")]
    pub total_result_number: u64,
    pub results: Vec<SearchResult>,
}

/// The type of search to use for a criteria.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum MatchType {
    #[serde(rename = "UN_DES_MOTS")]
    OneOfTheWords,
    #[serde(rename = "EXACTE")]
    Exact,
    #[serde(rename = "TOUS_LES_MOTS_DANS_UN_CHAMP")]
    AllOfTheWordsInAField,
    #[serde(rename = "AUCUN_DES_MOTS")]
    NoneOfTheWords,
    #[serde(rename = "AUCUNE_CORRESPONDANCE_A_CETTE_EXPRESSION")]
    NoMatchToThisExpression,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "UPPERCASE")]
pub enum FieldType {
    All,
    Title,
    Text,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SearchResult {
    /// The “date utile” of the result.
    /// (unclear what this means)
    pub date: Option<String>,
    /// The date of the result.
    #[serde(rename = "datePublication")]
    pub date_publication: Option<String>,
    /// The date of the result's signature.
    #[serde(rename = "dateSignature")]
    pub date_signature: Option<String>,
    /// The date of the result's diffusion.
    #[serde(rename = "dateDiffusion")]
    pub date_diffusion: Option<String>,
    /// The array of “titles” of the result.
    pub titles: Vec<SearchResultTitle>,
    /// The text of the result.
    pub text : Option<String>,
    /// The current legal status of the result.
    #[serde(rename = "etat")]
    pub legal_status: Option<String>,
    /// The origin of the result (dataset)
    #[serde(rename = "origin")]
    pub fond: Option<Fond>,
    /// The type of document of the result.
    #[serde(rename = "type")]
    pub doctype: Option<String>,
    pub nature: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SearchResultTitle {
    /// The title of the result.
    pub title: String,
    /// The id of the result.
    pub id: String,
    /// The chronological id of the result.
    pub cid: String,
}
