use crate::error::Error;
use crate::fetcher::GtfsFetcher;
use crate::gtfs_importer::GtfsImporter;
use crate::gtfs_url_fetcher::GtfsUrlFetcher;
use crate::importer::SlowGtfsImporter;
use crate::manager::Manager;
use crate::schedule::Schedule;
use crate::schedule_manager::ScheduleManager;

use chrono::offset::Utc;
use chrono::{Days, NaiveTime, TimeZone};
use chrono_tz::Europe::Paris;

use tokio::time;
use tokio::time::Duration;

use async_trait::async_trait;

use std::sync::Arc;

pub struct EurostarManager {
    schedule_manager: Arc<ScheduleManager>,
}

impl EurostarManager {
    pub async fn new(schedule_manager: Arc<ScheduleManager>) -> Result<EurostarManager, Error> {
        Ok(EurostarManager { schedule_manager })
    }

    async fn reload_gtfs(
        &self,
        gtfs_fetcher: &GtfsUrlFetcher,
        gtfs_importer: &mut GtfsImporter,
    ) -> Result<(), Error> {
        {
            // lock for writing now, such that there will be no chance of smaller updates being
            // lost
            let mut transaction = self.schedule_manager.transactional_write().await;

            let mut schedule = Schedule::new(
                "zzes".to_string(),
                "International — Eurostar".to_string(),
            );

            let gtfs = gtfs_fetcher.fetch().await?;
            schedule = gtfs_importer.overlay(gtfs, schedule).await?;

            // always replace the schedule
            transaction.insert("zzes".to_string(), schedule);
            transaction.commit();
        }

        Ok(())
    }

    async fn update_gtfs(
        &self,
        gtfs_fetcher: &GtfsUrlFetcher,
        gtfs_importer: &mut GtfsImporter,
    ) -> Result<(), Error> {
        loop {
            let now = Paris.from_utc_datetime(&Utc::now().naive_utc());
            let new_time = if now.time() > NaiveTime::from_hms_opt(4, 4, 0).unwrap() {
                Paris
                    .from_local_datetime(
                        &now.date_naive()
                            .checked_add_days(Days::new(1))
                            .unwrap()
                            .and_hms_opt(4, 4, 0)
                            .unwrap(),
                    )
                    .unwrap()
            } else {
                Paris
                    .from_local_datetime(&now.date_naive().and_hms_opt(4, 4, 0).unwrap())
                    .unwrap()
            };
            let mut interval = time::interval(Duration::from_secs(15));
            while Paris.from_utc_datetime(&Utc::now().naive_utc()) < new_time {
                interval.tick().await;
            }

            self.reload_gtfs(gtfs_fetcher, gtfs_importer).await?;
        }
    }
}

#[async_trait]
impl Manager for EurostarManager {
    async fn run(&mut self) -> Result<(), Error> {
        let gtfs_fetcher = GtfsUrlFetcher::new(
            "https://integration-storage.dm.eurostar.com/gtfs-prod/gtfs_static_commercial_v2.zip",
            "eurostar.com",
        );
        let mut gtfs_importer = GtfsImporter::new();

        self.reload_gtfs(&gtfs_fetcher, &mut gtfs_importer).await?;

        tokio::try_join!(async {
            return self.update_gtfs(&gtfs_fetcher, &mut gtfs_importer).await;
        },)?;

        Ok(())
    }
}
