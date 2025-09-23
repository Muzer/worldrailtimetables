use crate::error::Error;
use crate::fetcher::StreamingFetcher;
use async_trait::async_trait;
use futures::stream::TryStreamExt;
use reqwest::Client;
// I tried to use ReadZiptreaming here, but sadly these files sometimes have malformed local
// headers (with size == 0) which means this is impossible
use rc_zip_tokio::ReadZip;
use serde::{Deserialize, Serialize};

use tokio::io::{AsyncBufRead, AsyncReadExt, BufReader};
use tokio_util::io::StreamReader;

use std::fmt;
use std::io::Cursor;

pub struct NirFetcher {}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NirCkanOrganization {
    id: String,
    name: String,
    title: Option<String>,
    #[serde(rename = "type")]
    _type: String,
    description: Option<String>,
    image_url: Option<String>,
    created: Option<String>,
    is_organization: Option<bool>,
    approval_status: Option<String>,
    state: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NirCkanGroup {
    description: Option<String>,
    display_name: Option<String>,
    id: String,
    image_display_url: Option<String>,
    name: String,
    title: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NirCkanResource {
    cache_last_updated: Option<String>,
    cache_url: Option<String>,
    created: Option<String>,
    datastore_active: Option<bool>,
    description: Option<String>,
    format: Option<String>,
    hash: Option<String>,
    id: String,
    last_modified: Option<String>,
    metadata_modified: Option<String>,
    mimetype: Option<String>,
    mimetype_inner: Option<String>,
    name: String,
    package_id: Option<String>,
    position: Option<usize>,
    resource_type: Option<String>,
    size: Option<usize>,
    state: Option<String>,
    url: Option<String>,
    url_type: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NirCkanTag {
    display_name: Option<String>,
    id: String,
    name: String,
    state: Option<String>,
    vocabulary_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NirCkanResult {
    additional_info: Option<String>,
    author: Option<String>,
    author_email: Option<String>,
    contact_email: Option<String>,
    contact_name: Option<String>,
    creator_user_id: Option<String>,
    dashboard_link: Option<String>,
    frequency: Option<String>,
    id: String,
    isopen: Option<bool>,
    license_id: Option<String>,
    license_title: Option<String>,
    license_url: Option<String>,
    lineage: Option<String>,
    maintainer: Option<String>,
    maintainer_email: Option<String>,
    metadata_created: Option<String>, // Maybe should be datetime of some sort but who cares
    metadata_modified: Option<String>,
    metatags: Option<String>,
    name: String,
    notes: Option<String>,
    num_resources: usize,
    num_tags: usize,
    organization: NirCkanOrganization,
    owner_org: Option<String>,
    private: Option<bool>,
    state: Option<String>,
    time_period: Option<String>,
    title: Option<String>,
    title_tags: Option<String>,
    topic_category: Option<Vec<String>>,
    #[serde(rename = "type")]
    _type: String,
    url: Option<String>,
    version: Option<String>,
    groups: Option<Vec<NirCkanGroup>>,
    resources: Option<Vec<NirCkanResource>>,
    tags: Option<Vec<NirCkanTag>>,
    relationships_as_subject: Option<Vec<String>>,
    relationships_as_object: Option<Vec<String>>,
    total_downloads: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NirCkanResponse {
    help: String,
    success: bool,
    result: Option<NirCkanResult>,
}

#[derive(Clone, Debug)]
pub enum CkanErrorType {
    NotSuccess,
    NoResult,
    NoResources,
    ResourceNotFound(String),
    NoUrl,
}

impl fmt::Display for CkanErrorType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CkanErrorType::NotSuccess => write!(f, "The request to the NIR Open Data service returned a failure code"),
            CkanErrorType::NoResult => write!(f, "The request to the NIR Open Data service reported success but returned no result"),
            CkanErrorType::NoResources => write!(f, "The request to the NIR Open Data service reported success but returned no resources"),
            CkanErrorType::ResourceNotFound(x) => write!(f, "The request to the NIR Open Data service did not return resource {}", x),
            CkanErrorType::NoUrl => write!(f, "The request to the NIR Open Data service did not return a URL for the required resource"),
        }
    }
}

#[derive(Debug)]
pub struct CkanError {
    error_type: CkanErrorType,
    field_name: String,
}

impl fmt::Display for CkanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Could not fetch NIR CIF URL; error reading Ckan JSON field {}: {}",
            self.field_name, self.error_type
        )
    }
}

#[derive(Clone, Debug)]
pub enum NirFetcherErrorType {
    NoCifEntry,
}

impl fmt::Display for NirFetcherErrorType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            NirFetcherErrorType::NoCifEntry => write!(f, "Zip file did not contain a .CIF entry"),
        }
    }
}

#[derive(Debug)]
pub struct NirFetcherError {
    error_type: NirFetcherErrorType,
}

impl fmt::Display for NirFetcherError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Error fetching NIR CIF: {}",
            self.error_type
        )
    }
}

impl NirFetcher {
    pub fn new() -> Self {
        Self {}
    }

    fn extract_url_from_ckan(&self, json: NirCkanResponse) -> Result<String, CkanError> {
        if !json.success {
            return Err(CkanError {
                error_type: CkanErrorType::NotSuccess,
                field_name: "success".to_string(),
            })
        }

        let result = match json.result {
            None => return Err(CkanError {
                error_type: CkanErrorType::NoResult,
                field_name: "result".to_string(),
            }),
            Some(x) => x,
        };

        let resources = match result.resources {
            None => return Err(CkanError {
                error_type: CkanErrorType::NoResources,
                field_name: "resources".to_string(),
            }),
            Some(x) => x,
        };

        let resource = match resources.iter().find(|&x| x.name == "NIR Rail cif data") {
            None => return Err(CkanError {
                error_type: CkanErrorType::ResourceNotFound("NIR Rail cif data".to_string()),
                field_name: "name".to_string(),
            }),
            Some(x) => x,
        };

        match &resource.url {
            None => return Err(CkanError {
                error_type: CkanErrorType::NoUrl,
                field_name: "url".to_string(),
            }),
            Some(x) => Ok(x.clone()),
        }
    }

    async fn get_url(&self) -> Result<String, Error> {
        let client = Client::new();
        let response = client
            .get("https://admin.opendatani.gov.uk/api/3/action/package_show?id=nir20160126v2")
            .send()
            .await?
            .error_for_status()?;
        let reader = StreamReader::new(
            response
                .bytes_stream()
                .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
        );
        let mut json_str = String::new();
        BufReader::new(reader).read_to_string(&mut json_str).await?;
        let json = serde_json::from_str::<NirCkanResponse>(&json_str)?;

        Ok(self.extract_url_from_ckan(json)?)
    }
}

#[async_trait]
impl StreamingFetcher for NirFetcher {
    async fn fetch(&self) -> Result<Box<dyn AsyncBufRead + Unpin + Send>, Error> {
        println!("Fetching NIR Rail CIF data from OpenDataNI");
        let client = Client::new();
        let url = self.get_url().await?;
        println!("{}", url);
        let response = client
            .get(url)
            .send()
            .await?
            .error_for_status()?;
        let response_bytes = Vec::<u8>::from(response.bytes().await?);
        let reader = response_bytes.read_zip().await?;
        for entry in reader.entries() {
            if entry.sanitized_name().unwrap_or("").to_ascii_lowercase().ends_with(".cif") {
                return Ok(Box::new(BufReader::new(Cursor::new(entry.bytes().await?))))
            }
        }
        Err(Error::NirFetcherError(NirFetcherError {
                    error_type: NirFetcherErrorType::NoCifEntry,
                }))
    }
}
