use crate::error::Error;
use crate::fetcher::StreamingFetcher;

use async_trait::async_trait;

use futures::TryStreamExt;

use reqwest::{Client, StatusCode, retry};

use serde::Deserialize;

use tokio::io::AsyncBufRead;
use tokio::io::BufReader;
use tokio_util::compat::FuturesAsyncReadCompatExt;

pub struct AuthenticatedGtfsRtFetcher {
    config: AuthenticatedGtfsRtFetcherConfig,
    url: String,
    source: String,
    namespace: String,
}

#[derive(Clone, Deserialize)]
pub struct AuthenticatedGtfsRtFetcherConfig {
    header: String,
    primary_key: String,
    secondary_key: Option<String>,
}

impl AuthenticatedGtfsRtFetcher {
    pub fn new(
        config: AuthenticatedGtfsRtFetcherConfig, url: &str, source: &str, namespace: &str
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
impl StreamingFetcher for AuthenticatedGtfsRtFetcher {
    async fn fetch(&self) -> Result<Box<dyn AsyncBufRead + Unpin + Send>, Error> {
        println!("[{}] Fetching GTFS-RT from {}", self.namespace, self.source);
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
        let reader = response
            .bytes_stream()
            .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
            .into_async_read()
            .compat();
        Ok(Box::new(BufReader::new(reader)))
    }
}
