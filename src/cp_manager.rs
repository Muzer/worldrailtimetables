use crate::error::Error;
use crate::fetcher::GtfsFetcher;
use crate::gtfs_importer::GtfsImporter;
use crate::gtfs_url_fetcher::GtfsUrlFetcher;
use crate::importer::SlowGtfsImporter;
use crate::manager::Manager;
use crate::schedule::schedule;
use crate::schedule_manager::ScheduleManager;

use chrono::offset::Utc;
use chrono::{Days, NaiveTime, TimeZone};
use chrono_tz::Europe::Lisbon;

use sea_orm::EntityTrait;
use sea_orm::entity::ActiveValue;

use tokio::time;
use tokio::time::Duration;

use async_trait::async_trait;

use std::sync::Arc;

pub struct CpManager {
    schedule_manager: Arc<ScheduleManager>,
}

impl CpManager {
    pub async fn new(schedule_manager: Arc<ScheduleManager>) -> Result<CpManager, Error> {
        Ok(CpManager { schedule_manager })
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
            schedule::Entity::delete_by_id("ptcp")
                .exec(&*transaction)
                .await?;

            let schedule = schedule::ActiveModelEx {
                namespace: ActiveValue::Set("ptcp".to_owned()),
                description: ActiveValue::Set(
                    "Portugal — Comboios de Portugal".to_owned()
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
            let now = Lisbon.from_utc_datetime(&Utc::now().naive_utc());
            let new_time = if now.time() > NaiveTime::from_hms_opt(3, 27, 0).unwrap() {
                Lisbon
                    .from_local_datetime(
                        &now.date_naive()
                            .checked_add_days(Days::new(1))
                            .unwrap()
                            .and_hms_opt(3, 27, 0)
                            .unwrap(),
                    )
                    .unwrap()
            } else {
                Lisbon
                    .from_local_datetime(&now.date_naive().and_hms_opt(3, 27, 0).unwrap())
                    .unwrap()
            };
            let mut interval = time::interval(Duration::from_secs(15));
            while Lisbon.from_utc_datetime(&Utc::now().naive_utc()) < new_time {
                interval.tick().await;
            }

            self.reload_gtfs(gtfs_fetcher, gtfs_importer).await?;
        }
    }
}

#[async_trait]
impl Manager for CpManager {
    async fn run(&mut self) -> Result<(), Error> {
        let gtfs_fetcher = GtfsUrlFetcher::new(
            "https://publico.cp.pt/gtfs/gtfs.zip",
            "CP",
            "ptcp",
        );
        let gtfs_importer = GtfsImporter::new();

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
        )?;

        Ok(())
    }
}
