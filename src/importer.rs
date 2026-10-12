use crate::error::Error;
use crate::schedule::schedule;

use async_trait::async_trait;

use sea_orm::DatabaseTransaction;

use tokio::io::{AsyncBufRead, AsyncBufReadExt};

use gtfs_structures::Gtfs;

use std::collections::HashMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MainScheduleStatus {
    UpToDate,
    NeedsUpdate,
    NoData,
}

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
pub trait SlowMultiFileImporter {
    async fn overlay(
        &mut self,
        readers: HashMap<String, Box<dyn AsyncBufRead + Unpin + Send>>,
        schedule: &schedule::ModelEx,
        transaction: &DatabaseTransaction,
    ) -> Result<(), Error>;
}

#[async_trait]
pub trait SlowGtfsImporter {
    async fn overlay(
        &self, gtfs: Gtfs, schedule: &schedule::ModelEx, transaction: &DatabaseTransaction,
    ) -> Result<(), Error>;
}

#[async_trait]
pub trait FastImporter {
    async fn overlay(
        &self, data: Vec<u8>, schedule: &schedule::ModelEx, transaction: &DatabaseTransaction,
    ) -> Result<MainScheduleStatus, Error>;
}
