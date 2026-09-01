use crate::error::Error;
use crate::fetcher::StreamingFetcher;
use crate::importer::SlowStreamingImporter;
use crate::manager::Manager;
use crate::netex_importer::NetexImporter;
use crate::schedule::Schedule;
use crate::schedule_manager::ScheduleManager;
use crate::sncf_fetcher::SncfFetcher;

use chrono::offset::Utc;
use chrono::{Days, NaiveTime, TimeZone};
use chrono_tz::Europe::Paris;

use tokio::time;
use tokio::time::Duration;

use serde::Deserialize;

use async_trait::async_trait;

use std::sync::Arc;

pub struct SncfManager {
    schedule_manager: Arc<ScheduleManager>,
}

impl SncfManager {
    pub async fn new(
        schedule_manager: Arc<ScheduleManager>,
    ) -> Result<SncfManager, Error> {
        Ok(SncfManager {
            schedule_manager,
        })
    }

    async fn reload_netex(
        &self,
        sncf_fetcher: &SncfFetcher,
        netex_importer: &mut NetexImporter,
    ) -> Result<(), Error> {
        {
            // lock for writing now, such that there will be no chance of smaller updates being
            // lost
            let mut transaction = self.schedule_manager.transactional_write().await;

            let mut schedule = Schedule::new(
                "frsn".to_string(),
                "France — SNCF Réseau".to_string(),
            );

            let mut reader = sncf_fetcher.fetch().await?;
            schedule = netex_importer.overlay(&mut reader, schedule).await?;

            // always replace the schedule
            transaction.insert("frsn".to_string(), schedule);
            transaction.commit();
        }

        Ok(())
    }

    async fn update_netex(
        &self,
        sncf_fetcher: &SncfFetcher,
        netex_importer: &mut NetexImporter,
    ) -> Result<(), Error> {
        loop {
            let now = Paris.from_utc_datetime(&Utc::now().naive_utc());
            let new_time = if now.time() > NaiveTime::from_hms_opt(3, 12, 0).unwrap() {
                Paris
                    .from_local_datetime(
                        &now.date_naive()
                            .checked_add_days(Days::new(1))
                            .unwrap()
                            .and_hms_opt(3, 12, 0)
                            .unwrap(),
                    )
                    .unwrap()
            } else {
                Paris
                    .from_local_datetime(&now.date_naive().and_hms_opt(3, 12, 0).unwrap())
                    .unwrap()
            };
            let mut interval = time::interval(Duration::from_secs(15));
            while Paris.from_utc_datetime(&Utc::now().naive_utc()) < new_time {
                interval.tick().await;
            }

            self.reload_netex(sncf_fetcher, netex_importer).await?;
        }
    }
}

#[async_trait]
impl Manager for SncfManager {
    async fn run(&mut self) -> Result<(), Error> {
        // TODO multiple of these for each region
        let sncf_fetcher = SncfFetcher::new(
            "https://eu.ftp.opendatasoft.com/sncf/plandata/export-opendata-sncf-netex.zip",
            "SNCF Voyageurs TGV/Intercités/TER",
            "opendatasoft",
        );
        // TODO SNCF Transilien (hopefully don't need the whole Ile de France dataset)
        // https://eu.ftp.opendatasoft.com/sncf/gtfs/transilien-netexfr.zip 
        // TODO pausing on this because 1) it doesn't use ordinary frames, 2) multiple files per zip
        // TODO Transdev Rail Sud Intermétropole (GTFS seems better here, NeTEx available but
        // outdated and poor quality)
        // https://www.data.gouv.fr/fr/datasets/r/30782be6-90aa-40bb-940a-cb881a95ad26
        // TODO Renfe AVE international operations (GTFS only)
        // https://ssl.renfe.com/gtransit/Fichero_AV_INT/Renfe_AVE_Int.zip
        // TODO E* (GTFS)
        // https://integration-storage.dm.eurostar.com/gtfs-prod/gtfs_static_commercial_v2.zip
        // TODO Trenitalia France (GTFS)
        // https://thello.axelor.com/public/gtfs/gtfs.zip
        // Fluo is included in the SNCF Voyageurs set
        // Zou is also available separately, TODO see if it's worthwhile getting, especially instead
        // of Transdev above maybe?
        // https://proxy-data.zou.maregionsud.fr/NeTEx/NETEX_SIBR_zou_ferre_datasud.zip
        // Breizhgo TER also available separately, though should be the same as SNCF TER in theory
        // https://www.korrigo.bzh/ftp/OPENDATA/BREIZHGO_TER.netex.zip
        // TODO Corsica
        // https://www.data.gouv.fr/api/1/datasets/r/69c3db8a-a5fd-471f-b59f-46b9faded381
        let mut netex_importer = NetexImporter::new();

        self.reload_netex(&sncf_fetcher, &mut netex_importer).await?;

        tokio::try_join!(async {
            return self.update_netex(&sncf_fetcher, &mut netex_importer).await;
        },)?;

        Ok(())
    }
}
