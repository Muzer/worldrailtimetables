use crate::error::Error;
use crate::fetcher::GtfsFetcher;

use async_trait::async_trait;

use gtfs_structures::{Gtfs, GtfsReader};

pub struct GtfsUrlFetcher {
    url: String,
    source: String,
}

impl GtfsUrlFetcher {
    pub fn new(url: &str, source: &str) -> Self {
        Self {
            url: url.to_string(),
            source: source.to_string(),
        }
    }
}

#[async_trait]
impl GtfsFetcher for GtfsUrlFetcher {
    async fn fetch(&self) -> Result<Gtfs, Error> {
        println!("Fetching GTFS from {}", self.source);
        Ok(GtfsReader::default()
            .read_shapes(false)
            .unkown_enum_as_default(false)
            .read_from_url_async(self.url.clone())
            .await?)
    }
}
