use crate::error::Error;
use crate::fetcher::StreamingFetcher;
use crate::importer::SlowStreamingImporter;
use crate::manager::Manager;
use crate::nir_fetcher::NirFetcher;
use crate::schedule::Schedule;
use crate::schedule_manager::ScheduleManager;
use crate::uk_importer::{CifImporter, CifImporterConfig};

use chrono::offset::Utc;
use chrono::{Days, NaiveTime, TimeZone};
use chrono_tz::Europe::London;

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
            // lock for writing now, such that there will be no chance of smaller updates being
            // lost
            let mut transaction = self.schedule_manager.transactional_write().await;

            let mut schedule = Schedule::new(
                "gbni".to_string(),
                "United Kingdom — Translink NI Railways".to_string(),
            );

            let mut reader = nir_fetcher.fetch().await?;
            schedule = cif_importer.overlay(&mut reader, schedule).await?;

            // always replace the schedule
            transaction.insert("gbni".to_string(), schedule);
            transaction.commit();
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

            self.reload_cif(nir_fetcher, cif_importer)
                .await?;
        }
    }
}

#[async_trait]
impl Manager for NirManager {
    async fn run(&mut self) -> Result<(), Error> {
        let nir_fetcher = NirFetcher::new();
        let mut cif_importer = CifImporter::new(self.config.cif_importer.clone());

        self.reload_cif(&nir_fetcher, &mut cif_importer)
            .await?;

        tokio::try_join!(
            async {
                return self
                    .update_cif(&nir_fetcher, &mut cif_importer)
                    .await;
            },
        )?;

        Ok(())
    }
}
