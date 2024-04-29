use async_compression::tokio::bufread::GzipDecoder;
use async_trait::async_trait;
use reqwest::Client;
use crate::fetcher::Fetcher;
use crate::error::Error;
use futures::stream::TryStreamExt;
use serde::Deserialize;

use tokio::io::AsyncBufRead;
use tokio::io::BufReader;
use tokio_util::compat::FuturesAsyncReadCompatExt;

pub struct NrFetcher {
    config: NrFetcherConfig,
}

#[derive(Clone, Deserialize)]
pub struct NrFetcherConfig {
    username: String,
    password: String,
}

impl NrFetcher {
    pub fn new(config: NrFetcherConfig) -> Self {
        Self {
            config
        }
    }
}

#[async_trait]
impl Fetcher for NrFetcher {
    async fn fetch(&self) -> Result<Box<dyn AsyncBufRead + Unpin + Send>, Error> {
        println!("Fetching SCHEDULE from Network Rail");
        let client = Client::new();
        let response = client
            .get("https://publicdatafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_FULL_DAILY&day=toc-full.CIF.gz")
            .basic_auth(self.config.username.clone(), Some(self.config.password.clone()))
            .send()
            .await?
            .error_for_status()?;
        let reader = response
            .bytes_stream()
            .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
            .into_async_read()
            .compat();
        let gz = GzipDecoder::new(BufReader::new(reader));
        Ok(Box::new(BufReader::new(gz)))
    }
}
