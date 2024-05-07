use crate::error::Error;
use crate::fetcher::StreamingFetcher;
use async_trait::async_trait;
use futures::stream::TryStreamExt;
use rc_zip_tokio::ReadZipStreaming;
use reqwest::Client;

use tokio::io::{AsyncBufRead, BufReader};
use tokio_util::compat::FuturesAsyncReadCompatExt;

use std::fmt;

pub struct SncfFetcher {
    url: String,
    subset: String,
    source: String,
}

impl SncfFetcher {
    pub fn new(url: &str, subset: &str, source: &str) -> Self {
        Self {
            url: url.to_string(),
            subset: subset.to_string(),
            source: source.to_string(),
        }
    }
}

#[derive(Debug)]
pub struct SncfFetcherError {
    what: String,
}

impl fmt::Display for SncfFetcherError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error fetching SNCF data: {}", self.what)
    }
}

#[async_trait]
impl StreamingFetcher for SncfFetcher {
    async fn fetch(&self) -> Result<Box<dyn AsyncBufRead + Unpin + Send>, Error> {
        println!("Fetching SNCF {} data from {}", self.subset, self.source);
        let client = Client::new();
        let response = client.get(self.url.clone()).send().await?.error_for_status()?;
        let mut reader = response
            .bytes_stream()
            .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
            .into_async_read()
            .compat()
            .stream_zip_entries_throwing_caution_to_the_wind()
            .await?;
        if reader.entry().name == "trf2netex.log" {
            reader = match reader.finish().await? {
                Some(x) => x,
                None => {
                    return Err(SncfFetcherError {
                        what: "Zip entry not found".to_string(),
                    })?;
                }
            }
        }
        Ok(Box::new(BufReader::new(reader)))
    }
}
