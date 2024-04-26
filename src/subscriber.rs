use crate::error::Error;

use async_trait::async_trait;

#[async_trait]
pub trait Subscriber {
    async fn subscribe(&mut self) -> Result<(), Error>;
    async fn receive(&mut self) -> Result<Vec<u8>, Error>;
}
