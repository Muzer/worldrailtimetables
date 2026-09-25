use crate::error::Error;
use crate::fetcher::StreamingFetcher;
use crate::importer::SlowStreamingImporter;
use crate::manager::Manager;
use crate::nir_fetcher::NirFetcher;
use crate::schedule::schedule;
use crate::schedule_manager::ScheduleManager;
use crate::uk_importer::{CifImporter, CifImporterConfig};

use chrono::offset::Utc;
use chrono::{Days, NaiveTime, TimeZone};
use chrono_tz::Europe::London;

use sea_orm::EntityTrait;
use sea_orm::entity::ActiveValue;

use tokio::time;
use tokio::time::Duration;

use serde::Deserialize;

use async_trait::async_trait;

use std::sync::Arc;

#[derive(Clone, Deserialize)]
pub struct NirConfig {
    cif_importer: CifImporterConfig,
}

pub struct NirManager {
    schedule_manager: Arc<ScheduleManager>,
    config: NirConfig,
}

impl NirManager {
    pub async fn new(
        config: NirConfig,
        schedule_manager: Arc<ScheduleManager>,
    ) -> Result<NirManager, Error> {
        Ok(NirManager {
            schedule_manager,
            config,
        })
    }

    async fn reload_cif(
        &self,
        nir_fetcher: &NirFetcher,
        cif_importer: &mut CifImporter,
    ) -> Result<(), Error> {
        {
            let transaction = self.schedule_manager.transactional_write().await?;

            // Clear all the old data — all the deletes are set as cascades so should just need to
            // clear the database.
            schedule::Entity::delete_by_id("gbni")
                .exec(&*transaction)
                .await?;

            let schedule = schedule::ActiveModelEx {
                namespace: ActiveValue::Set("gbni".to_owned()),
                description: ActiveValue::Set("United Kingdom — Translink NI Railways".to_owned()),
                ..Default::default()
            }.insert(&*transaction).await?;

            let mut reader = nir_fetcher.fetch().await?;
            cif_importer.overlay(&mut reader, &schedule, &transaction).await?;

            transaction.commit().await?;
        }

        Ok(())
    }

    async fn update_cif(
        &self,
        nir_fetcher: &NirFetcher,
        cif_importer: &mut CifImporter,
    ) -> Result<(), Error> {
        loop {
            let now = London.from_utc_datetime(&Utc::now().naive_utc());
            let new_time = if now.time() > NaiveTime::from_hms_opt(3, 12, 0).unwrap() {
                London
                    .from_local_datetime(
                        &now.date_naive()
                            .checked_add_days(Days::new(1))
                            .unwrap()
                            .and_hms_opt(3, 12, 0)
                            .unwrap(),
                    )
                    .unwrap()
            } else {
                London
                    .from_local_datetime(&now.date_naive().and_hms_opt(3, 12, 0).unwrap())
                    .unwrap()
            };
            let mut interval = time::interval(Duration::from_secs(15));
            while London.from_utc_datetime(&Utc::now().naive_utc()) < new_time {
                interval.tick().await;
            }

            self.reload_cif(nir_fetcher, cif_importer).await?;
        }
    }
}

#[async_trait]
impl Manager for NirManager {
    async fn run(&mut self) -> Result<(), Error> {
        let nir_fetcher = NirFetcher::new();
        let mut cif_importer = CifImporter::new(self.config.cif_importer.clone());

        self.reload_cif(&nir_fetcher, &mut cif_importer).await?;

        tokio::try_join!(async {
            return self.update_cif(&nir_fetcher, &mut cif_importer).await;
        },)?;

        Ok(())
    }
}
