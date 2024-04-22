use config_file::ConfigFileError;
use crate::cif_importer::CifError;
use reqwest;

use std::fmt;

#[derive(Debug)]
pub enum Error {
    ConfigFileError(ConfigFileError),
    HttpRequestError(reqwest::Error),
    IoError(std::io::Error),
    CifError(CifError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            other => write!(f, "WorldTrainTimes error: {}", other)
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
