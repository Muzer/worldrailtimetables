use crate::error::Error;
use crate::fetcher::MultiFileFetcher;
use crate::iff_importer::IffImporter;
use crate::importer::SlowMultiFileImporter;
use crate::manager::Manager;
use crate::multi_file_zip_fetcher::MultiFileZipFetcher;
use crate::schedule::schedule;
use crate::schedule_manager::ScheduleManager;

use chrono::offset::Utc;
use chrono::{Days, NaiveTime, TimeZone};
use chrono_tz::Europe::Amsterdam;

use sea_orm::EntityTrait;
use sea_orm::entity::ActiveValue;

use tokio::time;
use tokio::time::Duration;

use async_trait::async_trait;

use std::sync::Arc;

pub struct NsManager {
    schedule_manager: Arc<ScheduleManager>,
}

impl NsManager {
    pub async fn new(schedule_manager: Arc<ScheduleManager>) -> Result<NsManager, Error> {
        Ok(NsManager { schedule_manager })
    }

    async fn reload_iff(
        &self,
        iff_fetcher: &MultiFileZipFetcher,
        iff_importer: &mut IffImporter,
    ) -> Result<(), Error> {
        {
            let transaction = self.schedule_manager.transactional_write().await?;

            // Clear all the old data — all the deletes are set as cascades so should just need to
            // clear the database.
            schedule::Entity::delete_by_id("nlns")
                .exec(&*transaction)
                .await?;

            let schedule = schedule::ActiveModelEx {
                namespace: ActiveValue::Set("nlns".to_owned()),
                description: ActiveValue::Set(
                    "the Netherlands — Nederlandse Spoorwegen".to_owned()
                ),
                ..Default::default()
            }.insert(&*transaction).await?;

            let readers = iff_fetcher.fetch().await?;
            iff_importer.overlay(readers, &schedule, &*transaction).await?;

            transaction.commit().await?;
        }

        Ok(())
    }

    async fn update_iff(
        &self,
        iff_fetcher: &MultiFileZipFetcher,
        iff_importer: &mut IffImporter,
    ) -> Result<(), Error> {
        loop {
            let now = Amsterdam.from_utc_datetime(&Utc::now().naive_utc());
            let new_time = if now.time() > NaiveTime::from_hms_opt(2, 17, 0).unwrap() {
                Amsterdam
                    .from_local_datetime(
                        &now.date_naive()
                            .checked_add_days(Days::new(1))
                            .unwrap()
                            .and_hms_opt(2, 17, 0)
                            .unwrap(),
                    )
                    .unwrap()
            } else {
                Amsterdam
                    .from_local_datetime(&now.date_naive().and_hms_opt(2, 17, 0).unwrap())
                    .unwrap()
            };
            let mut interval = time::interval(Duration::from_secs(15));
            while Amsterdam.from_utc_datetime(&Utc::now().naive_utc()) < new_time {
                interval.tick().await;
            }

            self.reload_iff(iff_fetcher, iff_importer).await?;
        }
    }
}

#[async_trait]
impl Manager for NsManager {
    async fn run(&mut self) -> Result<(), Error> {
        let iff_fetcher = MultiFileZipFetcher::new(
            "https://data.ndovloket.nl/ns/ns-latest.zip",
            "NDOV Loket",
            "nlns",
        );
        let mut iff_importer = IffImporter::new();

        match self.reload_iff(&iff_fetcher, &mut iff_importer).await {
            Ok(()) => (),
            Err(x) => {
                println!("Startup error! {}", x);
                return Err(x.into());
            },
        };

        tokio::try_join!(
            async {
                return self.update_iff(&iff_fetcher, &mut iff_importer).await;
            },
        )?;

        Ok(())
    }
}
