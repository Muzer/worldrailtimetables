use crate::authenticated_gtfs_fetcher::{AuthenticatedGtfsFetcher, AuthenticatedGtfsFetcherConfig};
use crate::error::Error;
use crate::fetcher::GtfsFetcher;
use crate::gtfs_importer::GtfsImporter;
use crate::importer::SlowGtfsImporter;
use crate::manager::Manager;
use crate::schedule::schedule;
use crate::schedule_manager::ScheduleManager;

use chrono::offset::Utc;
use chrono::{Days, NaiveTime, TimeZone};
use chrono_tz::Europe::Madrid;

use sea_orm::EntityTrait;
use sea_orm::entity::ActiveValue;

use serde::Deserialize;

use tokio::time;
use tokio::time::Duration;

use async_trait::async_trait;

use std::sync::Arc;

#[derive(Clone, Deserialize)]
pub struct OuigoEsConfig {
    authenticated_gtfs_fetcher: AuthenticatedGtfsFetcherConfig,
}

pub struct OuigoEsManager {
    config: OuigoEsConfig,
    schedule_manager: Arc<ScheduleManager>,
}

impl OuigoEsManager {
    pub async fn new(
        config: OuigoEsConfig, schedule_manager: Arc<ScheduleManager>
    ) -> Result<OuigoEsManager, Error> {
        Ok(OuigoEsManager { config, schedule_manager })
    }

    async fn reload_gtfs(
        &self,
        gtfs_fetcher: &AuthenticatedGtfsFetcher,
        gtfs_importer: &GtfsImporter,
    ) -> Result<(), Error> {
        {
            let transaction = self.schedule_manager.transactional_write().await?;

            // Clear all the old data — all the deletes are set as cascades so should just need to
            // clear the database.
            schedule::Entity::delete_by_id("esoe")
                .exec(&*transaction)
                .await?;

            let schedule = schedule::ActiveModelEx {
                namespace: ActiveValue::Set("esoe".to_owned()),
                description: ActiveValue::Set(
                    "Spain — Ouigo España".to_owned()
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
        gtfs_fetcher: &AuthenticatedGtfsFetcher,
        gtfs_importer: &GtfsImporter,
    ) -> Result<(), Error> {
        loop {
            let now = Madrid.from_utc_datetime(&Utc::now().naive_utc());
            let new_time = if now.time() > NaiveTime::from_hms_opt(4, 11, 0).unwrap() {
                Madrid
                    .from_local_datetime(
                        &now.date_naive()
                            .checked_add_days(Days::new(1))
                            .unwrap()
                            .and_hms_opt(4, 11, 0)
                            .unwrap(),
                    )
                    .unwrap()
            } else {
                Madrid
                    .from_local_datetime(&now.date_naive().and_hms_opt(4, 11, 0).unwrap())
                    .unwrap()
            };
            let mut interval = time::interval(Duration::from_secs(15));
            while Madrid.from_utc_datetime(&Utc::now().naive_utc()) < new_time {
                interval.tick().await;
            }

            self.reload_gtfs(gtfs_fetcher, gtfs_importer).await?;
        }
    }
}

#[async_trait]
impl Manager for OuigoEsManager {
    async fn run(&mut self) -> Result<(), Error> {
        let gtfs_fetcher = AuthenticatedGtfsFetcher::new(
            self.config.authenticated_gtfs_fetcher.clone(),
            "https://nap.transportes.gob.es/api/Fichero/download/1766",
            "NAP",
            "esoe",
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
