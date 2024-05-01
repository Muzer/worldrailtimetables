use crate::error::Error;
use crate::fetcher::Fetcher;
use crate::importer::{EphemeralImporter, FastImporter, SlowImporter};
use crate::manager::Manager;
use crate::nr_fetcher::{NrFetcher, NrFetcherConfig};
use crate::nr_vstp_subscriber::{NrVstpSubscriber, NrVstpSubscriberConfig};
use crate::schedule::Schedule;
use crate::schedule_manager::ScheduleManager;
use crate::subscriber::Subscriber;
use crate::uk_importer::{CifImporter, CifImporterConfig, NrJsonImporter, NrJsonImporterConfig};

use chrono::offset::Utc;
use chrono::{Days, NaiveTime, TimeZone};
use chrono_tz::Europe::London;

use tokio::time;
use tokio::time::Duration;

use async_trait::async_trait;

use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub struct NrConfig {
    fetcher: NrFetcherConfig,
    vstp_subscriber: NrVstpSubscriberConfig,
    json_importer: NrJsonImporterConfig,
    cif_importer: CifImporterConfig,
}

pub struct NrManager<'a> {
    schedule_manager: &'a ScheduleManager,
    config: NrConfig,
}

impl NrManager<'_> {
    pub async fn new<'a>(
        config: NrConfig,
        schedule_manager: &'a ScheduleManager,
    ) -> Result<NrManager<'a>, Error> {
        Ok(NrManager {
            schedule_manager,
            config,
        })
    }

    async fn reload_cif(
        &self,
        nr_fetcher: &NrFetcher,
        cif_importer: &mut CifImporter,
        nr_json_importer: &NrJsonImporter,
    ) -> Result<(), Error> {
        {
            // lock for writing now, such that there will be no chance of smaller updates being
            // lost
            let mut transaction = self.schedule_manager.transactional_write().await;

            let mut schedule = Schedule::new(
                "gbnr".to_string(),
                "United Kingdom — Network Rail".to_string(),
            );

            let mut reader = nr_fetcher.fetch().await?;
            schedule = cif_importer.overlay(&mut reader, schedule).await?;
            schedule = nr_json_importer.repopulate(schedule).await?;

            // always replace the schedule
            transaction.insert("gbnr".to_string(), schedule);
            transaction.commit();
        }

        nr_json_importer.persist().await?;

        Ok(())
    }

    async fn read_vstp(
        &self,
        nr_json_importer: &NrJsonImporter,
        nr_vstp_subscriber: &mut NrVstpSubscriber,
    ) -> Result<(), Error> {
        loop {
            let res = nr_vstp_subscriber.receive().await?;
            {
                let mut schedules = self.schedule_manager.immediate_write().await;
                let mut schedule = match schedules.remove("gbnr") {
                    Some(x) => x,
                    None => Schedule::new(
                        "gbnr".to_string(),
                        "United Kingdom — Network Rail".to_string(),
                    ),
                };
                schedule = nr_json_importer.overlay(res, schedule)?;
                schedules.insert("gbnr".to_string(), schedule);
            }
            nr_json_importer.persist().await?;
        }
    }

    async fn update_cif(
        &self,
        nr_fetcher: &NrFetcher,
        cif_importer: &mut CifImporter,
        nr_json_importer: &NrJsonImporter,
    ) -> Result<(), Error> {
        loop {
            let now = London.from_utc_datetime(&Utc::now().naive_utc());
            let new_time = if now.time() > NaiveTime::from_hms_opt(2, 9, 0).unwrap() {
                London
                    .from_local_datetime(
                        &now.date_naive()
                            .checked_add_days(Days::new(1))
                            .unwrap()
                            .and_hms_opt(2, 9, 0)
                            .unwrap(),
                    )
                    .unwrap()
            } else {
                London
                    .from_local_datetime(&now.date_naive().and_hms_opt(2, 9, 0).unwrap())
                    .unwrap()
            };
            let mut interval = time::interval(Duration::from_secs(15));
            while London.from_utc_datetime(&Utc::now().naive_utc()) < new_time {
                interval.tick().await;
            }

            self.reload_cif(nr_fetcher, cif_importer, nr_json_importer)
                .await?;
        }
    }
}

#[async_trait]
impl Manager for NrManager<'_> {
    async fn run(&mut self) -> Result<(), Error> {
        let nr_fetcher = NrFetcher::new(self.config.fetcher.clone());
        let mut cif_importer = CifImporter::new(self.config.cif_importer.clone());
        let mut nr_vstp_subscriber = NrVstpSubscriber::new(self.config.vstp_subscriber.clone());
        let nr_json_importer = NrJsonImporter::new(self.config.json_importer.clone()).await?;

        nr_vstp_subscriber.subscribe().await?;

        self.reload_cif(&nr_fetcher, &mut cif_importer, &nr_json_importer)
            .await?;

        tokio::try_join!(
            async {
                return self
                    .read_vstp(&nr_json_importer, &mut nr_vstp_subscriber)
                    .await;
            },
            async {
                return self
                    .update_cif(&nr_fetcher, &mut cif_importer, &nr_json_importer)
                    .await;
            },
        )?;

        Ok(())
    }
}
