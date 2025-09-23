use crate::gtfs_importer::GtfsImportError;
use crate::nir_fetcher::{CkanError, NirFetcherError};
use crate::nr_vstp_subscriber::NrVstpError;
use crate::sncf_fetcher::SncfFetcherError;
use crate::uk_importer::{CifError, NrJsonError};
use crate::webui::WebUiError;
use anyhow;
use config_file::ConfigFileError;
use rc_zip_tokio::rc_zip::error::Error as RcZipError;
use reqwest;
use tokio::task::JoinError;

use std::fmt;

#[derive(Debug)]
pub enum Error {
    ConfigFileError(ConfigFileError),
    HttpRequestError(reqwest::Error),
    IoError(std::io::Error),
    CifError(CifError),
    NrJsonError(NrJsonError),
    AnyhowError(anyhow::Error),
    NrVstpError(NrVstpError),
    SerdeJsonError(serde_json::Error),
    RocketError(rocket::Error),
    WebUiError(WebUiError),
    RcZipError(RcZipError),
    GtfsError(gtfs_structures::error::Error),
    JoinError(JoinError),
    GtfsImportError(GtfsImportError),
    SncfFetcherError(SncfFetcherError),
    CkanError(CkanError),
    NirFetcherError(NirFetcherError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::ConfigFileError(x) => write!(f, "WorldRailTimetables error: {}", x),
            Error::HttpRequestError(x) => write!(f, "WorldRailTimetables error: {}", x),
            Error::IoError(x) => write!(f, "WorldRailTimetables error: {}", x),
            Error::CifError(x) => write!(f, "WorldRailTimetables error: {}", x),
            Error::NrJsonError(x) => write!(f, "WorldRailTimetables error: {}", x),
            Error::AnyhowError(x) => write!(f, "WorldRailTimetables error: {}", x),
            Error::NrVstpError(x) => write!(f, "WorldRailTimetables error: {}", x),
            Error::SerdeJsonError(x) => write!(f, "WorldRailTimetables error: {}", x),
            Error::RocketError(x) => write!(f, "WorldRailTimetables error: {}", x.pretty_print()),
            Error::WebUiError(x) => write!(f, "WorldRailTimetables error: {}", x),
            Error::RcZipError(x) => write!(f, "WorldRailTimetables error: {}", x),
            Error::GtfsError(x) => write!(f, "WorldRailTimetables error: {}", x),
            Error::JoinError(x) => write!(f, "WorldRailTimetables error: {}", x),
            Error::GtfsImportError(x) => write!(f, "WorldRailTimetables error: {}", x),
            Error::SncfFetcherError(x) => write!(f, "WorldRailTimetables error: {}", x),
            Error::CkanError(x) => write!(f, "WorldRailTimetables error: {}", x),
            Error::NirFetcherError(x) => write!(f, "WorldRailTimetables error: {}", x),
        }
    }
}

impl From<ConfigFileError> for Error {
    fn from(error: ConfigFileError) -> Self {
        Error::ConfigFileError(error)
    }
}

impl From<reqwest::Error> for Error {
    fn from(error: reqwest::Error) -> Self {
        Error::HttpRequestError(error)
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::IoError(error)
    }
}

impl From<CifError> for Error {
    fn from(error: CifError) -> Self {
        Error::CifError(error)
    }
}

impl From<NrJsonError> for Error {
    fn from(error: NrJsonError) -> Self {
        Error::NrJsonError(error)
    }
}

impl From<anyhow::Error> for Error {
    fn from(error: anyhow::Error) -> Self {
        Error::AnyhowError(error)
    }
}

impl From<NrVstpError> for Error {
    fn from(error: NrVstpError) -> Self {
        Error::NrVstpError(error)
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Error::SerdeJsonError(error)
    }
}

impl From<rocket::Error> for Error {
    fn from(error: rocket::Error) -> Self {
        Error::RocketError(error)
    }
}

impl From<RcZipError> for Error {
    fn from(error: RcZipError) -> Self {
        Error::RcZipError(error)
    }
}

impl From<gtfs_structures::error::Error> for Error {
    fn from(error: gtfs_structures::error::Error) -> Self {
        Error::GtfsError(error)
    }
}

impl From<JoinError> for Error {
    fn from(error: JoinError) -> Self {
        Error::JoinError(error)
    }
}

impl From<GtfsImportError> for Error {
    fn from(error: GtfsImportError) -> Self {
        Error::GtfsImportError(error)
    }
}

impl From<SncfFetcherError> for Error {
    fn from(error: SncfFetcherError) -> Self {
        Error::SncfFetcherError(error)
    }
}

impl From<CkanError> for Error {
    fn from(error: CkanError) -> Self {
        Error::CkanError(error)
    }
}

impl From<NirFetcherError> for Error {
    fn from(error: NirFetcherError) -> Self {
        Error::NirFetcherError(error)
    }
}
