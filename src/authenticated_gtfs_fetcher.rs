use crate::error::Error;
use crate::fetcher::GtfsFetcher;

use async_trait::async_trait;

use gtfs_structures::{Gtfs, GtfsReader};

use reqwest::{Client, StatusCode, retry};

use serde::Deserialize;

use std::io::Cursor;

pub struct AuthenticatedGtfsFetcher {
    config: AuthenticatedGtfsFetcherConfig,
    url: String,
    source: String,
    namespace: String,
}

#[derive(Clone, Deserialize)]
pub struct AuthenticatedGtfsFetcherConfig {
    header: String,
    primary_key: String,
    secondary_key: Option<String>,
}

impl AuthenticatedGtfsFetcher {
    pub fn new(
        config: AuthenticatedGtfsFetcherConfig, url: &str, source: &str, namespace: &str
    ) -> Self {
        Self {
            config,
            url: url.to_string(),
            source: source.to_string(),
            namespace: namespace.to_string(),
        }
    }
}

#[async_trait]
impl GtfsFetcher for AuthenticatedGtfsFetcher {
    async fn fetch(&self) -> Result<Gtfs, Error> {
        println!("[{}] Fetching GTFS from {}", self.namespace, self.source);
        let client = Client::builder().retry(retry::for_host(self.url.clone())).build()?;
        let response = client
            .get(self.url.clone())
            .header(self.config.header.clone(), self.config.primary_key.clone())
            .send()
            .await?;
        let response = if response.status() == StatusCode::UNAUTHORIZED {
            match &self.config.secondary_key {
                Some(secondary_key) => client
                    .get(self.url.clone())
                    .header("x-api-key".to_string(), secondary_key.clone())
                    .send()
                    .await?
                    .error_for_status()?,
                None => response.error_for_status()?,
            }
        } else {
            response.error_for_status()?
        };

        // We have to read the whole thing as the result needs to be seekable
        let reader = Cursor::new(response.bytes().await?);

        Ok(Gtfs::try_from(
            GtfsReader::default()
                .read_shapes(false)
                .unkown_enum_as_default(false)
                .raw()
                .read_from_reader(reader)?
            )?
        )
    }
}
