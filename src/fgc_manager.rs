use crate::error::Error;
use crate::fetcher::{GtfsFetcher, StreamingFetcher};
use crate::gtfs_importer::GtfsImporter;
use crate::gtfs_rt_fetcher::GtfsRtFetcher;
use crate::gtfs_rt_importer::GtfsRtImporter;
use crate::gtfs_url_fetcher::GtfsUrlFetcher;
use crate::importer::{FastImporter, MainScheduleStatus, SlowGtfsImporter};
use crate::manager::Manager;
use crate::schedule::schedule;
use crate::schedule_manager::ScheduleManager;

use chrono::offset::Utc;
use chrono::{Days, NaiveTime, TimeZone};
use chrono_tz::Europe::Madrid;

use sea_orm::{EntityTrait, QueryFilter};
use sea_orm::entity::ActiveValue;

use tokio::time;
use tokio::io::AsyncReadExt;
use tokio::time::Duration;

use async_trait::async_trait;

use std::sync::Arc;

pub struct FgcManager {
    schedule_manager: Arc<ScheduleManager>,
}

impl FgcManager {
    pub async fn new(schedule_manager: Arc<ScheduleManager>) -> Result<FgcManager, Error> {
        Ok(FgcManager { schedule_manager })
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
            schedule::Entity::delete_by_id("esfc")
                .exec(&*transaction)
                .await?;

            let schedule = schedule::ActiveModelEx {
                namespace: ActiveValue::Set("esfc".to_owned()),
                description: ActiveValue::Set(
                    "Spain — Ferrocarrils de la Generalitat de Catalunya".to_owned()
                ),
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
            let now = Madrid.from_utc_datetime(&Utc::now().naive_utc());
            let new_time = if now.time() > NaiveTime::from_hms_opt(4, 9, 0).unwrap() {
                Madrid
                    .from_local_datetime(
                        &now.date_naive()
                            .checked_add_days(Days::new(1))
                            .unwrap()
                            .and_hms_opt(4, 9, 0)
                            .unwrap(),
                    )
                    .unwrap()
            } else {
                Madrid
                    .from_local_datetime(&now.date_naive().and_hms_opt(4, 9, 0).unwrap())
                    .unwrap()
            };
            let mut interval = time::interval(Duration::from_secs(15));
            while Madrid.from_utc_datetime(&Utc::now().naive_utc()) < new_time {
                interval.tick().await;
            }

            self.reload_gtfs(gtfs_fetcher, gtfs_importer).await?;
        }
    }

    async fn update_gtfs_rt(
        &self,
        gtfs_rt_fetcher: &GtfsRtFetcher,
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
                    .filter(schedule::COLUMN.namespace.eq("esfc"))
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
                    println!("[esfc] Reloading GTFS due to GTFS-RT indicating update");
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
impl Manager for FgcManager {
    async fn run(&mut self) -> Result<(), Error> {
        let gtfs_fetcher = GtfsUrlFetcher::new(
            "https://www.fgc.cat/google/google_transit.zip",
            "fgc.cat",
            "esfc",
        );
        let gtfs_importer = GtfsImporter::new();

        let gtfs_rt_fetcher = GtfsRtFetcher::new(
            "https://dadesobertes.fgc.cat/api/explore/v2.1/catalog/datasets/trip-updates-gtfs_realtime/files/735985017f62fd33b2fe46e31ce53829",
            "fgc.cat",
            "esfc",
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
