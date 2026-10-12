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
use chrono_tz::Europe::Paris;

use sea_orm::{EntityTrait, QueryFilter};
use sea_orm::entity::ActiveValue;

use tokio::time;
use tokio::io::AsyncReadExt;
use tokio::time::Duration;

use async_trait::async_trait;

use std::sync::Arc;

pub struct TransdevRailSudManager {
    schedule_manager: Arc<ScheduleManager>,
}

impl TransdevRailSudManager {
    pub async fn new(
        schedule_manager: Arc<ScheduleManager>
    ) -> Result<TransdevRailSudManager, Error> {
        Ok(TransdevRailSudManager { schedule_manager })
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
            schedule::Entity::delete_by_id("frts")
                .exec(&*transaction)
                .await?;

            let schedule = schedule::ActiveModelEx {
                namespace: ActiveValue::Set("frts".to_owned()),
                description: ActiveValue::Set("France — Transdev Rail Sud".to_owned()),
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
            let now = Paris.from_utc_datetime(&Utc::now().naive_utc());
            let new_time = if now.time() > NaiveTime::from_hms_opt(3, 12, 0).unwrap() {
                Paris
                    .from_local_datetime(
                        &now.date_naive()
                            .checked_add_days(Days::new(1))
                            .unwrap()
                            .and_hms_opt(3, 12, 0)
                            .unwrap(),
                    )
                    .unwrap()
            } else {
                Paris
                    .from_local_datetime(&now.date_naive().and_hms_opt(3, 12, 0).unwrap())
                    .unwrap()
            };
            let mut interval = time::interval(Duration::from_secs(15));
            while Paris.from_utc_datetime(&Utc::now().naive_utc()) < new_time {
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
                    .filter(schedule::COLUMN.namespace.eq("frts"))
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
                    println!("[frts] Reloading GTFS due to GTFS-RT indicating update");
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
impl Manager for TransdevRailSudManager {
    async fn run(&mut self) -> Result<(), Error> {
        let gtfs_fetcher = GtfsUrlFetcher::new(
            "https://www.data.gouv.fr/fr/datasets/r/30782be6-90aa-40bb-940a-cb881a95ad26",
            "data.gouv.fr",
            "frts",
        );
        let gtfs_importer = GtfsImporter::new();

        let gtfs_rt_fetcher = GtfsRtFetcher::new(
            "https://www.data.gouv.fr/api/1/datasets/r/74db080b-3d7c-4f30-8811-b344e79f4092",
            "data.gouv.fr",
            "frts",
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
