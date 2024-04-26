mod schedule;
mod importer;
mod fetcher;
mod subscriber;
mod nr_importer;
mod nr_fetcher;
mod nr_vstp_subscriber;
mod error;

use config_file::FromConfigFile;
use serde::Deserialize;

use crate::fetcher::Fetcher;
use crate::importer::Importer;
use crate::subscriber::Subscriber;

#[derive(Deserialize)]
struct Config {
    nr_fetcher_config: nr_fetcher::NrFetcherConfig,
    nr_vstp_subscriber_config: nr_vstp_subscriber::NrVstpSubscriberConfig,
}

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    let config = Config::from_config_file("./config.toml")?; // TODO improve

    let nr_fetcher = nr_fetcher::NrFetcher::new(config.nr_fetcher_config);
    let mut cif_importer = nr_importer::CifImporter::new();
    let mut nr_vstp_subscriber = nr_vstp_subscriber::NrVstpSubscriber::new(config.nr_vstp_subscriber_config);
    let mut nr_json_importer = nr_importer::NrJsonImporter::new();

    let mut schedule = schedule::Schedule::new("gbnr".to_string());

    let mut reader = nr_fetcher.fetch().await?;
    nr_vstp_subscriber.subscribe().await?;
    schedule = cif_importer.overlay(&mut reader, schedule).await?;

    loop {
        let res = nr_vstp_subscriber.receive().await?;
        schedule = nr_json_importer.overlay(res, schedule).await?;
    }
}
