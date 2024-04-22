use crate::error::Error;

use async_trait::async_trait;

use tokio::io::AsyncBufRead;

#[async_trait]
pub trait Fetcher {
    async fn fetch(&self) -> Result<Box<dyn AsyncBufRead + Unpin + Send>, Error>;
}
