use crate::error::Error;
use crate::fetcher::StreamingFetcher;
use crate::importer::{FastImporter, SlowStreamingImporter};
use crate::manager::Manager;
use crate::nr_fetcher::{NrFetcher, NrFetcherConfig};
use crate::nr_vstp_subscriber::{NrVstpSubscriber, NrVstpSubscriberConfig};
use crate::schedule::schedule;
use crate::schedule_manager::{ScheduleManager, TransactionalWriter};
use crate::subscriber::Subscriber;
use crate::uk_importer::{CifImporter, CifImporterConfig, NrJsonImporter};

use chrono::offset::Utc;
use chrono::{Datelike, Days, NaiveTime, TimeZone};
use chrono_tz::Europe::London;

use sea_orm::{EntityTrait, QueryFilter};
use sea_orm::entity::ActiveValue;

use tokio::time;
use tokio::time::Duration;

use async_trait::async_trait;

use serde::Deserialize;

use std::sync::Arc;

#[derive(Clone, Deserialize)]
pub struct NrConfig {
    fetcher: NrFetcherConfig,
    vstp_subscriber: NrVstpSubscriberConfig,
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

    async fn resynchronise_cif(
        &self,
        nr_fetcher: &NrFetcher,
        nr_update_fetcher: &Vec<NrFetcher>,
        cif_importer: &mut CifImporter,
    ) -> Result<(), Error> {
        {
            let transaction = self.schedule_manager.transactional_write().await?;

            // If we have run in the past 7 days, we can still resynchronise from incremental
            // updates. Otherwise, we have to start afresh.

            // CIF is advertised as being available from around 1am. Add 1h to this for safety, and
            // an arbitrary minute offset to avoid on-the-hour spikes in demand. Thus we assume the
            // new file starts at 02:09.

            let schedule = schedule::Entity::load()
                .filter(schedule::COLUMN.namespace.eq("gbnr"))
                .one(&*transaction)
                .await?;

            // If no schedule, do a full reload
            let mut schedule = match schedule {
                Some(x) => x,
                None => return self.reload_cif(
                    nr_fetcher, nr_update_fetcher, cif_importer, Some(transaction)
                ).await,
            };

            let now = London.from_utc_datetime(&Utc::now().naive_utc());

            // This should always be set unless only VSTP has written to this schedule, which is an
            // error
            let last_updated = schedule.last_updated.unwrap();
            
            let last_updated_timetable_date
                = if last_updated.time() > NaiveTime::from_hms_opt(2, 9, 0).unwrap() {
                    last_updated.date().checked_add_days(Days::new(1)).unwrap()
                } else {
                    last_updated.date()
                };

            let expected_timetable_date
                = if now.time() > NaiveTime::from_hms_opt(2, 9, 0).unwrap() {
                now.date_naive()
            } else {
                now.date_naive().checked_sub_days(Days::new(1)).unwrap()
            };

            let date_diff = expected_timetable_date - last_updated_timetable_date;

            // Although in theory this could be 7, in practice we don't know precisely when NR
            // uploads the new data file so this would then produce an edge case. Even ignoring
            // that, handling this case is actually quite tricky as we'd need to go once round the
            // circle and so defining a stop condition is awkward. So do a full reload after 7 days
            // of downtime rather than 8.
            if date_diff.num_days() > 6 {
                return self.reload_cif(
                    nr_fetcher, nr_update_fetcher, cif_importer, Some(transaction)
                ).await;
            }

            let current_day: usize = expected_timetable_date
                .weekday()
                .number_from_sunday()
                .try_into()
                .unwrap();

            // 1-indexed, becomes 1 past the end of the array of fetchers
            let current_day = current_day % 7;

            let fetch_day: usize = last_updated_timetable_date
                .weekday()
                .number_from_sunday()
                .try_into()
                .unwrap();

            // 1-indexed, becomes the start date of the array of fetchers
            let fetch_day = fetch_day % 7;

            let mut i: usize = fetch_day;

            while i != current_day {
                println!("[gbnr] Fetching updates for day {}", i);
                let mut reader = nr_update_fetcher[i].fetch().await?;
                cif_importer.overlay(&mut reader, &schedule, &transaction).await?;
                // Reload the schedule after every overlay
                schedule = schedule::Entity::load()
                    .filter(schedule::COLUMN.namespace.eq("gbnr"))
                    .one(&*transaction)
                    .await?
                    .unwrap();
                i = (i + 1) % 7;
            }
        }

        Ok(())
    }

    async fn reload_cif(
        &self,
        nr_fetcher: &NrFetcher,
        nr_update_fetcher: &Vec<NrFetcher>,
        cif_importer: &mut CifImporter,
        transaction: Option<TransactionalWriter>,
    ) -> Result<(), Error> {
        {
            let transaction = match transaction {
                Some(x) => x,
                None => self.schedule_manager.transactional_write().await?,
            };

            // Clear all the old data — all the deletes are set as cascades so should just need to
            // clear the database.
            schedule::Entity::delete_by_id("gbnr")
                .exec(&*transaction)
                .await?;

            let schedule = schedule::ActiveModelEx {
                namespace: ActiveValue::Set("gbnr".to_owned()),
                description: ActiveValue::Set("United Kingdom — Network Rail".to_owned()),
                ..Default::default()
            }.insert(&*transaction).await?;

            let now = London.from_utc_datetime(&Utc::now().naive_utc());
            let mut reader = nr_fetcher.fetch().await?;
            cif_importer.overlay(&mut reader, &schedule, &transaction).await?;

            // Reload the schedule after every overlay
            let mut schedule = schedule::Entity::load()
                .filter(schedule::COLUMN.namespace.eq("gbnr"))
                .one(&*transaction)
                .await?
                .unwrap();

            let current_day: usize = now
                .date_naive()
                .weekday()
                .number_from_sunday()
                .try_into()
                .unwrap();

            // 1-indexed, becomes 1 past the end of the array of fetchers
            let mut current_day = current_day % 7;

            if now.time() <= NaiveTime::from_hms_opt(2, 9, 0).unwrap() {
                if current_day == 0 {
                    current_day = 7;
                }
                current_day -= 1;
            }

            for i in 0..current_day {
                println!("[gbnr] Fetching updates for day {}", i);
                let mut reader = nr_update_fetcher[i].fetch().await?;
                cif_importer.overlay(&mut reader, &schedule, &transaction).await?;
                // Reload the schedule after every overlay
                schedule = schedule::Entity::load()
                    .filter(schedule::COLUMN.namespace.eq("gbnr"))
                    .one(&*transaction)
                    .await?
                    .unwrap();
            }

            transaction.commit().await?;
        }

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
                let transaction = self.schedule_manager.transactional_write().await?;
                let schedule = schedule::Entity::load()
                    .filter(schedule::COLUMN.namespace.eq("gbnr"))
                    .one(&*transaction)
                    .await?
                    .unwrap();
                nr_json_importer.overlay(res, &schedule, &transaction).await?;
                transaction.commit().await?;
            }
        }
    }

    // TODO fetch these circular-ly for the daily updates as we are supposed to
    async fn update_cif(
        &self,
        nr_update_fetcher: &Vec<NrFetcher>,
        cif_importer: &mut CifImporter,
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
                .unwrap();

            // 1-indexed, refers to the previous day so becomes the day we want to fetch for the
            // next day
            let current_day = current_day % 7;

            {
                let transaction = self.schedule_manager.transactional_write().await?;
                let schedule = match self.schedule_manager.get_schedule_by_id("gbnr").await? {
                    Some(x) => x,
                    None => schedule::ActiveModelEx {
                        namespace: ActiveValue::Set("gbnr".to_owned()),
                        description:
                            ActiveValue::Set("United Kingdom — Network Rail".to_owned()),
                        ..Default::default()
                    }.insert(&*transaction).await?
                };

                let mut reader = nr_update_fetcher[current_day].fetch().await?;
                cif_importer.overlay(&mut reader, &schedule, &transaction).await?;

                transaction.commit().await?;
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
        let nr_json_importer = NrJsonImporter::new().await?;

        nr_vstp_subscriber.subscribe().await?;

        self.resynchronise_cif(
            &nr_main_fetcher,
            &nr_update_fetchers,
            &mut cif_importer,
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
                        &nr_update_fetchers,
                        &mut cif_importer,
                    )
                    .await;
            },
        )?;

        Ok(())
    }
}
