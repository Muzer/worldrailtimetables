use crate::error::Error;
use crate::fetcher::StreamingFetcher;
use crate::importer::{EphemeralImporter, FastImporter, SlowStreamingImporter};
use crate::manager::Manager;
use crate::nr_fetcher::{NrFetcher, NrFetcherConfig};
use crate::nr_vstp_subscriber::{NrVstpSubscriber, NrVstpSubscriberConfig};
use crate::schedule::Schedule;
use crate::schedule_manager::ScheduleManager;
use crate::subscriber::Subscriber;
use crate::uk_importer::{CifImporter, CifImporterConfig, NrJsonImporter, NrJsonImporterConfig};

use chrono::offset::Utc;
use chrono::{Datelike, Days, NaiveTime, TimeZone};
use chrono_tz::Europe::London;

use tokio::time;
use tokio::time::Duration;

use async_trait::async_trait;

use serde::Deserialize;

use std::sync::Arc;

#[derive(Clone, Deserialize)]
pub struct NrConfig {
    fetcher: NrFetcherConfig,
    vstp_subscriber: NrVstpSubscriberConfig,
    json_importer: NrJsonImporterConfig,
    cif_importer: CifImporterConfig,
}

pub struct NrManager {
    schedule_manager: Arc<ScheduleManager>,
    config: NrConfig,
}

impl NrManager {
    pub async fn new(
        config: NrConfig,
        schedule_manager: Arc<ScheduleManager>,
    ) -> Result<NrManager, Error> {
        Ok(NrManager {
            schedule_manager,
            config,
        })
    }

    // TODO fetch these circular-ly for the daily updates as we are supposed to
    async fn reload_cif(
        &self,
        nr_fetcher: &NrFetcher,
        nr_update_fetcher: &Vec<NrFetcher>,
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

            let now = London.from_utc_datetime(&Utc::now().naive_utc());
            let mut reader = nr_fetcher.fetch().await?;
            schedule = cif_importer.overlay(&mut reader, schedule).await?;

            let mut current_day: usize = now
                .date_naive()
                .weekday()
                .number_from_sunday()
                .try_into()
                .unwrap(); // 1-indexed
            if current_day == 7 {
                current_day = 0;
            }
            if now.time() <= NaiveTime::from_hms_opt(1, 0, 0).unwrap() {
                if current_day == 0 {
                    current_day = 7;
                }
                current_day -= 1;
            }

            for i in 0..current_day {
                println!("Fetching updates for day {}", i);
                let mut reader = nr_update_fetcher[i].fetch().await?;
                schedule = cif_importer.overlay(&mut reader, schedule).await?;
            }

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

    // TODO fetch these circular-ly for the daily updates as we are supposed to
    async fn update_cif(
        &self,
        nr_fetcher: &NrFetcher,
        nr_update_fetcher: &Vec<NrFetcher>,
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

            let current_day: usize = now
                .date_naive()
                .weekday()
                .number_from_sunday()
                .try_into()
                .unwrap(); // 1-indexed
            if current_day == 7 {
                self.reload_cif(
                    nr_fetcher,
                    nr_update_fetcher,
                    cif_importer,
                    nr_json_importer,
                )
                .await?;
            } else {
                {
                    let mut transaction = self.schedule_manager.transactional_write().await;

                    let mut schedule = match transaction.remove("gbnr") {
                        Some(x) => x,
                        None => Schedule::new(
                            "gbnr".to_string(),
                            "United Kingdom — Network Rail".to_string(),
                        ),
                    };
                    let mut reader = nr_update_fetcher[current_day].fetch().await?;
                    schedule = cif_importer.overlay(&mut reader, schedule).await?;
                    transaction.insert("gbnr".to_string(), schedule);

                    transaction.commit();
                }
            }
        }
    }
}

#[async_trait]
impl Manager for NrManager {
    async fn run(&mut self) -> Result<(), Error> {
        let nr_main_fetcher = NrFetcher::new(self.config.fetcher.clone(), "https://publicdatafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_FULL_DAILY&day=toc-full.CIF.gz");
        let nr_update_fetchers = vec![
            NrFetcher::new(self.config.fetcher.clone(), "https://publicdatafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_UPDATE_DAILY&day=toc-update-sat.CIF.gz"),
            NrFetcher::new(self.config.fetcher.clone(), "https://publicdatafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_UPDATE_DAILY&day=toc-update-sun.CIF.gz"),
            NrFetcher::new(self.config.fetcher.clone(), "https://publicdatafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_UPDATE_DAILY&day=toc-update-mon.CIF.gz"),
            NrFetcher::new(self.config.fetcher.clone(), "https://publicdatafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_UPDATE_DAILY&day=toc-update-tue.CIF.gz"),
            NrFetcher::new(self.config.fetcher.clone(), "https://publicdatafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_UPDATE_DAILY&day=toc-update-wed.CIF.gz"),
            NrFetcher::new(self.config.fetcher.clone(), "https://publicdatafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_UPDATE_DAILY&day=toc-update-thu.CIF.gz"),
            NrFetcher::new(self.config.fetcher.clone(), "https://publicdatafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_UPDATE_DAILY&day=toc-update-fri.CIF.gz"),
        ];
        let mut cif_importer = CifImporter::new(self.config.cif_importer.clone());
        let mut nr_vstp_subscriber = NrVstpSubscriber::new(self.config.vstp_subscriber.clone());
        let nr_json_importer = NrJsonImporter::new(self.config.json_importer.clone()).await?;

        nr_vstp_subscriber.subscribe().await?;

        self.reload_cif(
            &nr_main_fetcher,
            &nr_update_fetchers,
            &mut cif_importer,
            &nr_json_importer,
        )
        .await?;

        tokio::try_join!(
            async {
                return self
                    .read_vstp(&nr_json_importer, &mut nr_vstp_subscriber)
                    .await;
            },
            async {
                return self
                    .update_cif(
                        &nr_main_fetcher,
                        &nr_update_fetchers,
                        &mut cif_importer,
                        &nr_json_importer,
                    )
                    .await;
            },
        )?;

        Ok(())
    }
}
