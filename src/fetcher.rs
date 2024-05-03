use crate::error::Error;

use async_trait::async_trait;

use tokio::io::AsyncBufRead;

use gtfs_structures::Gtfs;

#[async_trait]
pub trait StreamingFetcher {
    async fn fetch(&self) -> Result<Box<dyn AsyncBufRead + Unpin + Send>, Error>;
}

#[async_trait]
pub trait GtfsFetcher {
    async fn fetch(&self) -> Result<Gtfs, Error>;
}
