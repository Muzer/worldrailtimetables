use crate::error::Error;

use async_trait::async_trait;

use tokio::io::AsyncBufRead;

use gtfs_structures::Gtfs;

#[async_trait]
pub trait StreamingFetcher {
    async fn fetch(&self) -> Result<Box<dyn AsyncBufRead + Unpin + Send>, Error>;
}

// WIP
/*#[async_trait]
pub trait IteratedStreamingFetcher {
    type Stream: IteratedStream;

    async fn fetch(self) -> Result<Box<Self::Stream>, Error>;
}

#[async_trait]
pub trait IteratedStream {
    type Reader: AsyncBufRead + Unpin + Send;

    async fn next(self) -> Result<Option<Self>, Error>;

    fn reader(&mut self) -> &mut Self::Reader;
}*/

#[async_trait]
pub trait GtfsFetcher {
    async fn fetch(&self) -> Result<Gtfs, Error>;
}
