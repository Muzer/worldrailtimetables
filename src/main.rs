mod error;
mod eurostar_manager;
mod fetcher;
mod gtfs_importer;
mod gtfs_url_fetcher;
mod importer;
mod ir_manager;
mod manager;
mod netex_importer;
mod nir_fetcher;
mod nir_manager;
mod nr_fetcher;
mod nr_manager;
mod nr_vstp_subscriber;
mod schedule;
mod schedule_manager;
mod sncf_fetcher;
mod sncf_manager;
mod subscriber;
mod uk_importer;
mod webui;

use config_file::FromConfigFile;
use serde::Deserialize;

use crate::eurostar_manager::EurostarManager;
use crate::ir_manager::IrManager;
use crate::manager::Manager;
use crate::nir_manager::{NirConfig, NirManager};
use crate::nr_manager::{NrConfig, NrManager};
use crate::sncf_manager::SncfManager;

use std::sync::Arc;

#[derive(Clone, Deserialize)]
struct Config {
    nr: NrConfig,
    nir: NirConfig,
}

async fn do_main() -> Result<(), error::Error> {
    let config = Config::from_config_file("./config.toml")?; // TODO improve

    let mut schedule_manager = schedule_manager::ScheduleManager::new();
    schedule_manager.load_location_associations().await?;
    let schedule_manager = Arc::new(schedule_manager);

    //let mut nr_manager = NrManager::new(config.nr, schedule_manager.clone()).await?;
    let mut nir_manager = NirManager::new(config.nir, schedule_manager.clone()).await?;
    let mut ir_manager = IrManager::new(schedule_manager.clone()).await?;
    let mut sncf_manager = SncfManager::new(schedule_manager.clone()).await?;
    let mut eurostar_manager = EurostarManager::new(schedule_manager.clone()).await?;

    //let nr_manager_fut = tokio::spawn(async move { nr_manager.run().await });
    let nir_manager_fut = tokio::spawn(async move { nir_manager.run().await });
    let ir_manager_fut = tokio::spawn(async move { ir_manager.run().await });
    let sncf_manager_fut = tokio::spawn(async move { sncf_manager.run().await });
    let eurostar_manager_fut = tokio::spawn(async move { eurostar_manager.run().await });
    let webui_fut = tokio::spawn(async move { webui::rocket(schedule_manager.clone()).await });
    tokio::select!(
        //x = nr_manager_fut => x,
        x = nir_manager_fut => x,
        x = ir_manager_fut => x,
        x = sncf_manager_fut => x,
        x = eurostar_manager_fut => x,
        x = webui_fut => x
    )??;

    Ok(())
}

#[rocket::main]
async fn main() -> Result<(), error::Error> {
    //tracing_subscriber::fmt().with_max_level(tracing::Level::TRACE).init();

    match do_main().await {
        Ok(()) => Ok(()),
        Err(x) => {
            println!("Error! {}", x);
            Err(x)
        },
    }
}
