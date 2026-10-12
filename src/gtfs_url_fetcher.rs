use crate::error::Error;
use crate::fetcher::GtfsFetcher;

use async_trait::async_trait;

use gtfs_structures::{Gtfs, GtfsReader};

use reqwest::{Client, retry};

use std::io::Cursor;

pub struct GtfsUrlFetcher {
    url: String,
    source: String,
    namespace: String,
}

impl GtfsUrlFetcher {
    pub fn new(url: &str, source: &str, namespace: &str) -> Self {
        Self {
            url: url.to_string(),
            source: source.to_string(),
            namespace: namespace.to_string(),
        }
    }
}

#[async_trait]
impl GtfsFetcher for GtfsUrlFetcher {
    async fn fetch(&self) -> Result<Gtfs, Error> {
        println!("[{}] Fetching GTFS from {}", self.namespace, self.source);
        let client = Client::builder()
            .retry(retry::for_host(self.url.clone()))
            .user_agent("WorldRailTimetables/0.10.0")
            .build()?;
        let response = client
            .get(self.url.clone())
            .send()
            .await?
            .error_for_status()?;

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
