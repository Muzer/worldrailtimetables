use crate::nr_importer::CifError;
use crate::nr_importer::NrJsonError;
use crate::nr_vstp_subscriber::NrVstpError;
use anyhow;
use config_file::ConfigFileError;
use reqwest;

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
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            other => write!(f, "WorldTrainTimes error: {}", other),
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
