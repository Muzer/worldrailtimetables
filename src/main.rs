mod error;
mod eurostar_manager;
mod fetcher;
mod gtfs_importer;
mod gtfs_url_fetcher;
mod importer;
mod ir_manager;
mod manager;
mod netex_importer;
//mod nir_fetcher;
//mod nir_manager;
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

use sea_orm::{ConnectOptions, ConnectionTrait, Database, DbBackend, Statement};
use sea_orm::sea_query::index::Index;

use serde::Deserialize;

use crate::eurostar_manager::EurostarManager;
use crate::ir_manager::IrManager;
use crate::manager::Manager;
//use crate::nir_manager::{NirConfig, NirManager};
use crate::nr_manager::{NrConfig, NrManager};
use crate::schedule::{
    association_node, line, location, train_allocation_vehicle, train_location, train_operator,
    train_variant, variable_train
};
use crate::sncf_manager::SncfManager;

use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Deserialize)]
struct SystemConfig {
    database_url: String,
    location_associations: String,
}

#[derive(Clone, Deserialize)]
struct Config {
    nr: NrConfig,
    //nir: NirConfig,
    system: SystemConfig,
}

async fn do_main() -> Result<(), error::Error> {
    let config = Config::from_config_file("./config.toml")?; // TODO improve

    let mut opt = ConnectOptions::new(config.system.database_url.to_owned());
    opt.sqlx_logging(false); // This is incredibly noisy and slows things down
    opt.connect_timeout(Duration::from_hours(24));
    let db = Database::connect(opt).await?;
    // TODO use proper migration
    // For now, enable WAL for speed
    let stmt = Statement::from_string(
        DbBackend::Sqlite, "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;"
    );
    db.query_one_raw(stmt).await?;
    db.get_schema_registry("worldrailtimetables::schedule::*").sync(&db).await?;

    // TODO use proper migrations
    // For now, we manually add indices for the missing tables
    db.execute(
        Index::create()
            .if_not_exists()
            .name("idx_location_public_id_namespace")
            .table(location::Entity)
            .col(location::COLUMN.public_id)
            .col(location::COLUMN.namespace)
    ).await?;
    db.execute(
        Index::create()
            .if_not_exists()
            .name("idx_train_allocation_vehicle_train_allocation_id_namespace")
            .table(train_allocation_vehicle::Entity)
            .col(train_allocation_vehicle::COLUMN.train_allocation_id)
            .col(train_allocation_vehicle::COLUMN.namespace)
    ).await?;
    db.execute(
        Index::create()
            .if_not_exists()
            .name("idx_train_allocation_vehicle_train_vehicle_id_namespace")
            .table(train_allocation_vehicle::Entity)
            .col(train_allocation_vehicle::COLUMN.train_vehicle_id)
            .col(train_allocation_vehicle::COLUMN.namespace)
    ).await?;
    db.execute(
        Index::create()
            .if_not_exists()
            .name("idx_train_operator_public_id_namespace")
            .table(train_operator::Entity)
            .col(train_operator::COLUMN.public_id)
            .col(train_operator::COLUMN.namespace)
    ).await?;
    db.execute(
        Index::create()
            .if_not_exists()
            .name("idx_association_node_other_train_id_namespace")
            .table(association_node::Entity)
            .col(association_node::COLUMN.other_train_id)
            .col(association_node::COLUMN.namespace)
    ).await?;
    db.execute(
        Index::create()
            .if_not_exists()
            .name("idx_train_location_train_variant_id_index")
            .table(train_location::Entity)
            .col(train_location::COLUMN.train_variant_id)
            .col(train_location::COLUMN.index)
    ).await?;
    db.execute(
        Index::create()
            .if_not_exists()
            .name("idx_train_location_location_id_namespace")
            .table(train_location::Entity)
            .col(train_location::COLUMN.location_id)
            .col(train_location::COLUMN.namespace)
    ).await?;
    db.execute(
        Index::create()
            .if_not_exists()
            .name("idx_line_public_id_namespace")
            .table(line::Entity)
            .col(line::COLUMN.public_id)
            .col(line::COLUMN.namespace)
    ).await?;
    db.execute(
        Index::create()
            .if_not_exists()
            .name("idx_variable_train_actual_allocation_id_namespace")
            .table(variable_train::Entity)
            .col(variable_train::COLUMN.actual_allocation_id)
            .col(variable_train::COLUMN.namespace)
    ).await?;
    db.execute(
        Index::create()
            .if_not_exists()
            .name("idx_variable_train_timing_allocation_id_namespace")
            .table(variable_train::Entity)
            .col(variable_train::COLUMN.timing_allocation_id)
            .col(variable_train::COLUMN.namespace)
    ).await?;
    db.execute(
        Index::create()
            .if_not_exists()
            .name("idx_variable_train_line_id_namespace")
            .table(variable_train::Entity)
            .col(variable_train::COLUMN.line_id)
            .col(variable_train::COLUMN.namespace)
    ).await?;
    db.execute(
        Index::create()
            .if_not_exists()
            .name("idx_variable_train_operator_id_namespace")
            .table(variable_train::Entity)
            .col(variable_train::COLUMN.operator_id)
            .col(variable_train::COLUMN.namespace)
    ).await?;
    db.execute(
        Index::create()
            .if_not_exists()
            .name("idx_variable_train_public_id_namespace")
            .table(variable_train::Entity)
            .col(variable_train::COLUMN.public_id)
            .col(variable_train::COLUMN.namespace)
    ).await?;
    db.execute(
        Index::create()
            .if_not_exists()
            .name("idx_train_variant_train_id_namespace")
            .table(train_variant::Entity)
            .col(train_variant::COLUMN.train_id)
            .col(train_variant::COLUMN.namespace)
    ).await?;

    let mut schedule_manager = schedule_manager::ScheduleManager::new(db);
    schedule_manager.load_location_associations(config.system.location_associations).await?;
    let schedule_manager = Arc::new(schedule_manager);

    let mut nr_manager = NrManager::new(config.nr, schedule_manager.clone()).await?;
    //let mut nir_manager = NirManager::new(config.nir, schedule_manager.clone()).await?;
    let mut ir_manager = IrManager::new(schedule_manager.clone()).await?;
    let mut sncf_manager = SncfManager::new(schedule_manager.clone()).await?;
    let mut eurostar_manager = EurostarManager::new(schedule_manager.clone()).await?;

    let nr_manager_fut = tokio::spawn(async move { nr_manager.run().await });
    //let nir_manager_fut = tokio::spawn(async move { nir_manager.run().await });
    let ir_manager_fut = tokio::spawn(async move { ir_manager.run().await });
    let sncf_manager_fut = tokio::spawn(async move { sncf_manager.run().await });
    let eurostar_manager_fut = tokio::spawn(async move { eurostar_manager.run().await });
    let webui_fut = tokio::spawn(async move { webui::rocket(schedule_manager.clone()).await });
    tokio::select!(
        x = nr_manager_fut => x,
        //x = nir_manager_fut => x,
        x = ir_manager_fut => x,
        x = sncf_manager_fut => x,
        x = eurostar_manager_fut => x,
        x = webui_fut => x
    )??;

    Ok(())
}

#[rocket::main]
async fn main() -> Result<(), error::Error> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).with_test_writer().init();

    match do_main().await {
        Ok(()) => Ok(()),
        Err(x) => {
            println!("Error! {}", x);
            Err(x)
        },
    }
}
