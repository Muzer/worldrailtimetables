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
use chrono_tz::Europe::Dublin;

use sea_orm::EntityTrait;
use sea_orm::entity::ActiveValue;

use tokio::time;
use tokio::time::Duration;

use async_trait::async_trait;

use std::sync::Arc;

pub struct IrManager {
    schedule_manager: Arc<ScheduleManager>,
}

impl IrManager {
    pub async fn new(schedule_manager: Arc<ScheduleManager>) -> Result<IrManager, Error> {
        Ok(IrManager { schedule_manager })
    }

    async fn reload_gtfs(
        &self,
        gtfs_fetcher: &GtfsUrlFetcher,
        gtfs_importer: &mut GtfsImporter,
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
        gtfs_importer: &mut GtfsImporter,
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
}

#[async_trait]
impl Manager for IrManager {
    async fn run(&mut self) -> Result<(), Error> {
        let gtfs_fetcher = GtfsUrlFetcher::new(
            "https://www.transportforireland.ie/transitData/Data/GTFS_Irish_Rail.zip",
            "the National Transport Authority",
            "ieir",
        );
        let mut gtfs_importer = GtfsImporter::new();

        match self.reload_gtfs(&gtfs_fetcher, &mut gtfs_importer).await {
            Ok(()) => (),
            Err(x) => {
                println!("Startup error! {}", x);
                return Err(x.into());
            },
        };

        tokio::try_join!(async {
            return self.update_gtfs(&gtfs_fetcher, &mut gtfs_importer).await;
        },)?;

        Ok(())
    }
}
