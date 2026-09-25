use crate::error::Error;
use crate::schedule::schedule;

use async_trait::async_trait;

use sea_orm::DatabaseTransaction;

use tokio::io::AsyncBufReadExt;

use gtfs_structures::Gtfs;

#[async_trait]
pub trait SlowStreamingImporter {
    async fn overlay(
        &mut self,
        reader: impl AsyncBufReadExt + Unpin + Send,
        schedule: &schedule::ModelEx,
        transaction: &DatabaseTransaction,
    ) -> Result<(), Error>;
}

#[async_trait]
pub trait SlowGtfsImporter {
    async fn overlay(
        &mut self, gtfs: Gtfs, schedule: &schedule::ModelEx, transaction: &DatabaseTransaction,
    ) -> Result<(), Error>;
}

#[async_trait]
pub trait FastImporter {
    async fn overlay(
        &self, data: Vec<u8>, schedule: &schedule::ModelEx, transaction: &DatabaseTransaction,
    ) -> Result<(), Error>;
}
