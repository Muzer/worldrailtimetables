mod error;
mod fetcher;
mod importer;
mod manager;
mod nr_fetcher;
mod nr_importer;
mod nr_manager;
mod nr_vstp_subscriber;
mod schedule;
mod schedule_manager;
mod subscriber;

use config_file::FromConfigFile;
use serde::Deserialize;

use crate::manager::Manager;
use crate::nr_manager::{NrConfig, NrManager};

#[derive(Clone, Deserialize)]
struct Config {
    nr: NrConfig,
}

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    let config = Config::from_config_file("./config.toml")?; // TODO improve

    let schedule_manager = schedule_manager::ScheduleManager::new();

    let mut nr_manager = NrManager::new(config.nr, &schedule_manager).await?;

    tokio::try_join!(nr_manager.run(),)?;

    Ok(())
}
