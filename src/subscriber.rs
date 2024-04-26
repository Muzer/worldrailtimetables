use crate::error::Error;

use async_trait::async_trait;

use tokio::io::AsyncBufRead;

#[async_trait]
pub trait Subscriber {
    async fn subscribe(&mut self) -> Result<(), Error>;
    async fn receive(&mut self) -> Result<Box<dyn AsyncBufRead + Unpin + Send>, Error>;
}
