use crate::error::Error;
use crate::fetcher::MultiFileFetcher;

use async_trait::async_trait;

use reqwest::{Client, retry};

use rc_zip_tokio::ReadZip;

use tokio::io::{AsyncBufRead, BufReader};

use std::collections::HashMap;
use std::io::Cursor;

pub struct MultiFileZipFetcher {
    url: String,
    source: String,
    namespace: String,
}

impl MultiFileZipFetcher {
    pub fn new(url: &str, source: &str, namespace: &str) -> Self {
        Self {
            url: url.to_string(),
            source: source.to_string(),
            namespace: namespace.to_string(),
        }
    }
}

#[async_trait]
impl MultiFileFetcher for MultiFileZipFetcher {
    async fn fetch(&self) -> Result<HashMap<String, Box<dyn AsyncBufRead + Unpin + Send>>, Error> {
        println!("[{}] Fetching zip file from {}", self.namespace, self.source);
        let client = Client::builder().retry(retry::for_host(self.url.clone())).build()?;
        let response = client.get(self.url.clone()).send().await?.error_for_status()?;
        let response_bytes = Vec::<u8>::from(response.bytes().await?);
        let reader = response_bytes.read_zip().await?;
        let mut readers: HashMap<String, Box<dyn AsyncBufRead + Unpin + Send>> = HashMap::new();
        for entry in reader.entries() {
            readers.insert(
                entry.sanitized_name().unwrap_or("").to_ascii_lowercase(),
                Box::new(BufReader::new(Cursor::new(entry.bytes().await?))),
            );
        }
        Ok(readers)
    }
}
