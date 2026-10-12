mod authenticated_gtfs_fetcher;
mod authenticated_gtfs_rt_fetcher;
mod corse_manager;
mod cp_manager;
mod error;
mod eurostar_manager;
mod euskotren_manager;
mod fertagus_manager;
mod fetcher;
mod fgc_manager;
mod fgv_manager;
mod gtfs_importer;
mod gtfs_rt_fetcher;
mod gtfs_rt_importer;
mod gtfs_url_fetcher;
mod iff_importer;
mod importer;
mod ir_manager;
mod mallorca_manager;
mod manager;
mod multi_file_zip_fetcher;
mod netex_importer;
//mod nir_fetcher;
//mod nir_manager;
mod nmbs_sncb_manager;
mod nr_fetcher;
mod nr_manager;
mod nr_vstp_subscriber;
mod ns_manager;
mod ouigo_es_manager;
mod renfe_av_ld_manager;
mod renfe_cercanias_manager;
mod schedule;
mod schedule_manager;
mod sncf_fetcher;
mod sncf_manager;
mod subscriber;
mod transdev_rail_sud_manager;
mod transilien_manager;
mod uk_importer;
mod webui;

use config_file::FromConfigFile;

use sea_orm::{ConnectOptions, ConnectionTrait, Database, DbBackend, Statement};
use sea_orm::sea_query::index::Index;

use serde::Deserialize;

use crate::corse_manager::CorseManager;
use crate::cp_manager::CpManager;
use crate::eurostar_manager::EurostarManager;
use crate::euskotren_manager::EuskotrenManager;
use crate::fertagus_manager::FertagusManager;
use crate::fgc_manager::FgcManager;
use crate::fgv_manager::{FgvConfig, FgvManager};
use crate::ir_manager::{IrConfig, IrManager};
use crate::mallorca_manager::MallorcaManager;
use crate::manager::Manager;
//use crate::nir_manager::{NirConfig, NirManager};
use crate::nmbs_sncb_manager::{NmbsSncbConfig, NmbsSncbManager};
use crate::nr_manager::{NrConfig, NrManager};
use crate::ns_manager::NsManager;
use crate::ouigo_es_manager::{OuigoEsConfig, OuigoEsManager};
use crate::renfe_av_ld_manager::RenfeAvLdManager;
use crate::renfe_cercanias_manager::RenfeCercaniasManager;
use crate::schedule::{
    association_node, line, location, train_allocation_vehicle, train_location, train_operator,
    train_variant, variable_train
};
use crate::sncf_manager::SncfManager;
use crate::transdev_rail_sud_manager::TransdevRailSudManager;
use crate::transilien_manager::TransilienManager;

use sea_orm::entity::prelude::Expr;

use tokio::task::JoinSet;

use std::env;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Deserialize)]
struct ManagersEnabledConfig {
    nr: bool,
    nir: bool,
    ir: bool,
    sncf: bool,
    eurostar: bool,
    transdev_rail_sud: bool,
    transilien: bool,
    corse: bool,
    renfe_av_ld: bool,
    renfe_cercanias: bool,
    euskotren: bool,
    fgc: bool,
    mallorca: bool,
    ouigo_es: bool,
    fgv: bool,
    cp: bool,
    fertagus: bool,
    nmbs_sncb: bool,
    ns: bool,
}

#[derive(Clone, Deserialize)]
struct SystemConfig {
    database_url: String,
    location_associations: String,
    managers_enabled: ManagersEnabledConfig,
}

#[derive(Clone, Deserialize)]
struct Config {
    ir: IrConfig,
    nr: NrConfig,
    //nir: NirConfig,
    ouigo_es: OuigoEsConfig,
    fgv: FgvConfig,
    nmbs_sncb: NmbsSncbConfig,
    system: SystemConfig,
}

#[derive(Debug, PartialEq)]
enum Mode {
    Server,
    Fetcher,
    Both,
}

async fn do_main() -> Result<(), error::Error> {
    let config = Config::from_config_file("./config.toml")?; // TODO improve

    // Figure out which mode we are in — by default we do server and fetcher but this is not ideal
    // for many reasons. We can run in server-only or fetcher-only mode.
    let args: Vec<String> = env::args().collect();

    let mode = match args.len() {
        0 => Mode::Both,
        1 => Mode::Both,
        2 => match args[1].as_str() {
            "--server" => Mode::Server,
            "--fetcher" => Mode::Fetcher,
            "--both" => Mode::Both,
            _ => panic!("Invalid argument, expected --server, --fetcher, or --both (default)"),
        },
        _ => panic!("Too many arguments, expected --server, --fetcher, or --both (default)"),
    };

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

    if mode != Mode::Server {
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
                .name("idx_train_location_train_variant_id_id")
                .table(train_location::Entity)
                .col(train_location::COLUMN.train_variant_id)
                .col(train_location::COLUMN.id)
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
                .name("idx_variable_train_brand_id_namespace")
                .table(variable_train::Entity)
                .col(variable_train::COLUMN.brand_id)
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
        // TODO fix
        /*db.execute(
            Index::create()
                .if_not_exists()
                .name("idx_variable_train_nocase_public_id_namespace")
                .table(variable_train::Entity)
                // Allows indexing on prefix searches
                .col(Expr::cust("\"public_id\" COLLATE NOCASE"))
                .col(variable_train::COLUMN.namespace)
        ).await?;*/
        db.execute(
            Index::create()
                .if_not_exists()
                .name("idx_variable_train_line_id_direction_namespace")
                .table(variable_train::Entity)
                .col(variable_train::COLUMN.line_id)
                .col(variable_train::COLUMN.direction)
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
        db.execute(
            Index::create()
                .if_not_exists()
                .name("idx_line_operator_id_namespace")
                .table(line::Entity)
                .col(line::COLUMN.operator_id)
                .col(line::COLUMN.namespace)
        ).await?;
    }

    let mut schedule_manager = schedule_manager::ScheduleManager::new(db);
    schedule_manager.load_location_associations(config.system.location_associations).await?;
    let schedule_manager = Arc::new(schedule_manager);

    let mut futs = JoinSet::new();

    if mode != Mode::Server {
        if config.system.managers_enabled.nr {
            let mut nr_manager = NrManager::new(config.nr, schedule_manager.clone()).await?;
            futs.spawn(async move { nr_manager.run().await });
        }
        if config.system.managers_enabled.nir {
            //let mut nir_manager = NirManager::new(config.nir, schedule_manager.clone()).await?;
            //futs.spawn(async move { nir_manager.run().await });
        }
        if config.system.managers_enabled.ir {
            let mut ir_manager = IrManager::new(config.ir, schedule_manager.clone()).await?;
            futs.spawn(async move { ir_manager.run().await });
        }
        if config.system.managers_enabled.sncf {
            let mut sncf_manager = SncfManager::new(schedule_manager.clone()).await?;
            futs.spawn(async move { sncf_manager.run().await });
        }
        if config.system.managers_enabled.eurostar {
            let mut eurostar_manager = EurostarManager::new(schedule_manager.clone()).await?;
            futs.spawn(async move { eurostar_manager.run().await });
        }
        if config.system.managers_enabled.transdev_rail_sud {
            let mut transdev_rail_sud_manager
                = TransdevRailSudManager::new(schedule_manager.clone()).await?;
            futs.spawn(async move { transdev_rail_sud_manager.run().await });
        }
        if config.system.managers_enabled.transilien {
            let mut transilien_manager = TransilienManager::new(schedule_manager.clone()).await?;
            futs.spawn(async move { transilien_manager.run().await });
        }
        if config.system.managers_enabled.corse {
            let mut corse_manager = CorseManager::new(schedule_manager.clone()).await?;
            futs.spawn(async move { corse_manager.run().await });
        }
        if config.system.managers_enabled.renfe_av_ld {
            let mut renfe_av_ld_manager = RenfeAvLdManager::new(schedule_manager.clone()).await?;
            futs.spawn(async move { renfe_av_ld_manager.run().await });
        }
        if config.system.managers_enabled.renfe_cercanias {
            let mut renfe_cercanias_manager
                = RenfeCercaniasManager::new(schedule_manager.clone()).await?;
            futs.spawn(async move { renfe_cercanias_manager.run().await });
        }
        if config.system.managers_enabled.euskotren {
            let mut euskotren_manager = EuskotrenManager::new(schedule_manager.clone()).await?;
            futs.spawn(async move { euskotren_manager.run().await });
        }
        if config.system.managers_enabled.fgc {
            let mut fgc_manager = FgcManager::new(schedule_manager.clone()).await?;
            futs.spawn(async move { fgc_manager.run().await });
        }
        if config.system.managers_enabled.mallorca {
            let mut mallorca_manager = MallorcaManager::new(schedule_manager.clone()).await?;
            futs.spawn(async move { mallorca_manager.run().await });
        }
        if config.system.managers_enabled.ouigo_es {
            let mut ouigo_es_manager
                = OuigoEsManager::new(config.ouigo_es, schedule_manager.clone()).await?;
            futs.spawn(async move { ouigo_es_manager.run().await });
        }
        if config.system.managers_enabled.fgv {
            let mut fgv_manager = FgvManager::new(config.fgv, schedule_manager.clone()).await?;
            futs.spawn(async move { fgv_manager.run().await });
        }
        if config.system.managers_enabled.cp {
            let mut cp_manager = CpManager::new(schedule_manager.clone()).await?;
            futs.spawn(async move { cp_manager.run().await });
        }
        if config.system.managers_enabled.fertagus {
            let mut fertagus_manager = FertagusManager::new(schedule_manager.clone()).await?;
            futs.spawn(async move { fertagus_manager.run().await });
        }
        if config.system.managers_enabled.nmbs_sncb {
            let mut nmbs_sncb_manager
                = NmbsSncbManager::new(config.nmbs_sncb, schedule_manager.clone()).await?;
            futs.spawn(async move { nmbs_sncb_manager.run().await });
        }
        if config.system.managers_enabled.ns {
            let mut ns_manager = NsManager::new(schedule_manager.clone()).await?;
            futs.spawn(async move { ns_manager.run().await });
        }
    }

    if mode != Mode::Fetcher {
        futs.spawn(async move { webui::rocket(schedule_manager.clone()).await });
    }

    futs.join_next().await.unwrap()??;

    Ok(())
}

#[rocket::main]
async fn main() -> Result<(), error::Error> {
    //tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).with_test_writer().init();

    match do_main().await {
        Ok(()) => Ok(()),
        Err(x) => {
            println!("Error! {}", x);
            Err(x)
        },
    }
}
