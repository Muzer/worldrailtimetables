use crate::error::Error;
use crate::fetcher::Fetcher;
use async_trait::async_trait;
use futures::stream::TryStreamExt;
use reqwest::Client;
use rc_zip_tokio::ReadZipStreaming;

use tokio::io::{AsyncBufRead, BufReader};
use tokio_util::compat::FuturesAsyncReadCompatExt;

pub struct NirFetcher {}

impl NirFetcher {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl Fetcher for NirFetcher {
    async fn fetch(&self) -> Result<Box<dyn AsyncBufRead + Unpin + Send>, Error> {
        println!("Fetching NIR Rail CIF data from OpenDataNI");
        let client = Client::new();
        let response = client
            .get("https://admin.opendatani.gov.uk/dataset/e41b1057-b0bd-4419-95eb-77057c8ad6b0/resource/ff8c5dd8-dcfd-4141-a270-df242f114215/download/nir.zip")
            .send()
            .await?
            .error_for_status()?;
        let reader = response
            .bytes_stream()
            .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
            .into_async_read()
            .compat()
            .stream_zip_entries_throwing_caution_to_the_wind().await?;
        Ok(Box::new(BufReader::new(reader)))
    }
}
