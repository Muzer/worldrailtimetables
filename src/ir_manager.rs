use crate::authenticated_gtfs_rt_fetcher::{
    AuthenticatedGtfsRtFetcher, AuthenticatedGtfsRtFetcherConfig
};
use crate::error::Error;
use crate::fetcher::{GtfsFetcher, StreamingFetcher};
use crate::gtfs_importer::GtfsImporter;
use crate::gtfs_rt_importer::GtfsRtImporter;
use crate::gtfs_url_fetcher::GtfsUrlFetcher;
use crate::importer::{FastImporter, MainScheduleStatus, SlowGtfsImporter};
use crate::manager::Manager;
use crate::schedule::schedule;
use crate::schedule_manager::ScheduleManager;

use chrono::offset::Utc;
use chrono::{Days, NaiveTime, TimeZone};
use chrono_tz::Europe::Dublin;

use sea_orm::{EntityTrait, QueryFilter};
use sea_orm::entity::ActiveValue;

use serde::Deserialize;

use tokio::time;
use tokio::io::AsyncReadExt;
use tokio::time::Duration;

use async_trait::async_trait;

use std::sync::Arc;

#[derive(Clone, Deserialize)]
pub struct IrConfig {
    authenticated_gtfs_rt_fetcher: AuthenticatedGtfsRtFetcherConfig,
}

pub struct IrManager {
    config: IrConfig,
    schedule_manager: Arc<ScheduleManager>,
}

impl IrManager {
    pub async fn new(
        config: IrConfig, schedule_manager: Arc<ScheduleManager>
    ) -> Result<IrManager, Error> {
        Ok(IrManager { config, schedule_manager })
    }

    async fn reload_gtfs(
        &self,
        gtfs_fetcher: &GtfsUrlFetcher,
        gtfs_importer: &GtfsImporter,
    ) -> Result<(), Error> {
        {
            let transaction = self.schedule_manager.transactional_write().await?;

            // Clear all the old data — all the deletes are set as cascades so should just need to
            // clear the database.
            schedule::Entity::delete_by_id("ieir")
                .exec(&*transaction)
                .await?;

            let schedule = schedule::ActiveModelEx {
                namespace: ActiveValue::Set("ieir".to_owned()),
                description: ActiveValue::Set("Ireland — Irish Rail/Iarnród Éireann".to_owned()),
                ..Default::default()
            }.insert(&*transaction).await?;

            let gtfs = gtfs_fetcher.fetch().await?;
            gtfs_importer.overlay(gtfs, &schedule, &*transaction).await?;

            transaction.commit().await?;
        }

        Ok(())
    }

    async fn update_gtfs(
        &self,
        gtfs_fetcher: &GtfsUrlFetcher,
        gtfs_importer: &GtfsImporter,
    ) -> Result<(), Error> {
        loop {
            let now = Dublin.from_utc_datetime(&Utc::now().naive_utc());
            let new_time = if now.time() > NaiveTime::from_hms_opt(4, 4, 0).unwrap() {
                Dublin
                    .from_local_datetime(
                        &now.date_naive()
                            .checked_add_days(Days::new(1))
                            .unwrap()
                            .and_hms_opt(4, 4, 0)
                            .unwrap(),
                    )
                    .unwrap()
            } else {
                Dublin
                    .from_local_datetime(&now.date_naive().and_hms_opt(4, 4, 0).unwrap())
                    .unwrap()
            };
            let mut interval = time::interval(Duration::from_secs(15));
            while Dublin.from_utc_datetime(&Utc::now().naive_utc()) < new_time {
                interval.tick().await;
            }

            self.reload_gtfs(gtfs_fetcher, gtfs_importer).await?;
        }
    }

    async fn update_gtfs_rt(
        &self,
        gtfs_rt_fetcher: &AuthenticatedGtfsRtFetcher,
        gtfs_rt_importer: &GtfsRtImporter,
        gtfs_fetcher: &GtfsUrlFetcher,
        gtfs_importer: &GtfsImporter,
    ) -> Result<(), Error> {
        loop {
            let mut gtfs_rt_protobuf = gtfs_rt_fetcher.fetch().await?;

            let mut buf = Vec::new();

            gtfs_rt_protobuf.read_to_end(&mut buf).await?;

            let main_schedule_status = {
                let transaction = self.schedule_manager.transactional_write().await?;

                let schedule = schedule::Entity::load()
                    .filter(schedule::COLUMN.namespace.eq("ieir"))
                    .one(&*transaction)
                    .await?
                    .unwrap();

                let main_schedule_status
                    = gtfs_rt_importer.overlay(buf, &schedule, &transaction).await?;

                transaction.commit().await?;

                main_schedule_status
            };

            match main_schedule_status {
                MainScheduleStatus::UpToDate | MainScheduleStatus::NoData => (),
                MainScheduleStatus::NeedsUpdate => {
                    println!("[ieir] Reloading GTFS due to GTFS-RT indicating update");
                    self.reload_gtfs(gtfs_fetcher, gtfs_importer).await?;
                },
            };

            // Still wait the interval even after reloading, in case the new version isn't
            // available.

            let mut interval = time::interval(Duration::from_secs(60));
            interval.tick().await;
            interval.tick().await;
        }
    }
}

#[async_trait]
impl Manager for IrManager {
    async fn run(&mut self) -> Result<(), Error> {
        let gtfs_fetcher = GtfsUrlFetcher::new(
            "https://www.transportforireland.ie/transitData/Data/GTFS_Irish_Rail.zip",
            "the National Transport Authority",
            "ieir",
        );
        let gtfs_importer = GtfsImporter::new();

        let gtfs_rt_fetcher = AuthenticatedGtfsRtFetcher::new(
            self.config.authenticated_gtfs_rt_fetcher.clone(),
            "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates",
            "the National Transport Authority",
            "ieir",
        );
        let gtfs_rt_importer = GtfsRtImporter::new(true, false, vec![]);

        match self.reload_gtfs(&gtfs_fetcher, &gtfs_importer).await {
            Ok(()) => (),
            Err(x) => {
                println!("Startup error! {}", x);
                return Err(x.into());
            },
        };

        tokio::try_join!(
            async {
                return self.update_gtfs(&gtfs_fetcher, &gtfs_importer).await;
            },
            async {
                return self.update_gtfs_rt(
                    &gtfs_rt_fetcher, &gtfs_rt_importer, &gtfs_fetcher, &gtfs_importer
                ).await;
            },
        )?;

        Ok(())
    }
}
