use crate::error::Error;

use async_trait::async_trait;

use tokio::io::AsyncBufRead;

use gtfs_structures::Gtfs;

use std::collections::HashMap;

#[async_trait]
pub trait StreamingFetcher {
    async fn fetch(&self) -> Result<Box<dyn AsyncBufRead + Unpin + Send>, Error>;
}

#[async_trait]
pub trait MultiFileFetcher {
    async fn fetch(&self) -> Result<HashMap<String, Box<dyn AsyncBufRead + Unpin + Send>>, Error>;
}

#[async_trait]
pub trait GtfsFetcher {
    async fn fetch(&self) -> Result<Gtfs, Error>;
}
