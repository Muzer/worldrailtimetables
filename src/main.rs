mod schedule;
mod importer;
mod fetcher;
mod cif_importer;
mod nr_fetcher;
mod error;

use config_file::FromConfigFile;
use serde::Deserialize;

use crate::fetcher::Fetcher;
use crate::importer::Importer;

#[derive(Deserialize)]
struct Config {
    nr_fetcher_config: nr_fetcher::NrFetcherConfig,
}

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    let config = Config::from_config_file("./config.toml")?; // TODO improve

    let nr_fetcher = nr_fetcher::NrFetcher::new(config.nr_fetcher_config);
    let mut cif_importer = cif_importer::CifImporter::new();

    let mut schedule = schedule::Schedule::new("gbnr".to_string());

    let mut reader = nr_fetcher.fetch().await?;
    schedule = cif_importer.overlay(&mut reader, schedule).await?;
    print!("{:#?}\n", schedule.trains.get("Y01160"));
    print!("{:#?}\n", schedule.trains.get("Y04271"));
    Ok(())
}
