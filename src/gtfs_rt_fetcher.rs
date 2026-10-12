use crate::error::Error;
use crate::fetcher::StreamingFetcher;

use async_trait::async_trait;

use futures::TryStreamExt;

use reqwest::{Client, retry};

use tokio::io::AsyncBufRead;
use tokio::io::BufReader;
use tokio_util::compat::FuturesAsyncReadCompatExt;

pub struct GtfsRtFetcher {
    url: String,
    source: String,
    namespace: String,
}

impl GtfsRtFetcher {
    pub fn new(url: &str, source: &str, namespace: &str) -> Self {
        Self {
            url: url.to_string(),
            source: source.to_string(),
            namespace: namespace.to_string(),
        }
    }
}

#[async_trait]
impl StreamingFetcher for GtfsRtFetcher {
    async fn fetch(&self) -> Result<Box<dyn AsyncBufRead + Unpin + Send>, Error> {
        println!("[{}] Fetching GTFS-RT from {}", self.namespace, self.source);
        let client = Client::builder().retry(retry::for_host(self.url.clone())).build()?;
        let response = client
            .get(self.url.clone())
            .send()
            .await?
            .error_for_status()?;
        let reader = response
            .bytes_stream()
            .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
            .into_async_read()
            .compat();
        Ok(Box::new(BufReader::new(reader)))
    }
}
