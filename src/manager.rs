use crate::error::Error;

use async_trait::async_trait;

#[async_trait]
pub trait Manager {
    async fn run(&mut self) -> Result<(), Error>;
}
