use crate::schedule::Schedule;
use crate::error::Error;

use async_trait::async_trait;

use tokio::io::AsyncBufReadExt;

#[async_trait]
pub trait SlowImporter {
    async fn overlay(&mut self, reader: impl AsyncBufReadExt + Unpin + Send, schedule: Schedule) -> Result<Schedule, Error>;
    async fn repopulate(&mut self, schedule: Schedule) -> Result<Schedule, Error>;
}

#[async_trait]
pub trait FastImporter {
    fn overlay(&mut self, data: Vec<u8>, schedule: Schedule) -> Result<Schedule, Error>;
    async fn repopulate(&mut self, schedule: Schedule) -> Result<Schedule, Error>;
}
