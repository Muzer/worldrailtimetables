use crate::error::Error;
use crate::schedule::Schedule;

use async_trait::async_trait;

use tokio::io::AsyncBufReadExt;

use gtfs_structures::Gtfs;

#[async_trait]
pub trait SlowStreamingImporter {
    async fn overlay(
        &mut self,
        reader: impl AsyncBufReadExt + Unpin + Send,
        schedule: Schedule,
    ) -> Result<Schedule, Error>;
}

#[async_trait]
pub trait SlowGtfsImporter {
    async fn overlay(&mut self, gtfs: Gtfs, schedule: Schedule) -> Result<Schedule, Error>;
}

#[async_trait]
pub trait FastImporter {
    fn overlay(&self, data: Vec<u8>, schedule: Schedule) -> Result<Schedule, Error>;
}

#[async_trait]
pub trait EphemeralImporter {
    async fn repopulate(&self, schedule: Schedule) -> Result<Schedule, Error>;
    async fn persist(&self) -> Result<(), Error>;
}
