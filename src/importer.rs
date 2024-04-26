use crate::schedule::Schedule;
use crate::error::Error;

use async_trait::async_trait;

use tokio::io::AsyncBufReadExt;

#[async_trait]
pub trait Importer {
    async fn overlay(&mut self, reader: impl AsyncBufReadExt + Unpin + Send, schedule: Schedule) -> Result<Schedule, Error>;
    async fn repopulate(&mut self, schedule: Schedule) -> Result<Schedule, Error>;
}
