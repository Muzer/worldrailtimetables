use crate::error::Error;
use crate::importer::SlowGtfsImporter;
use crate::schedule::{Location, Schedule, Train, VariableTrain};

use async_trait::async_trait;

use chrono_tz::{ParseError, Tz};

use gtfs_structures::{Gtfs, LocationType, Stop};

use tokio::task::block_in_place;

use std::fmt;
use std::str::FromStr;

pub struct GtfsImporter {
    base_gtfs: Option<Gtfs>,
}

#[derive(Clone, Debug)]
pub enum GtfsErrorType {
    InvalidEmptyStopName(String),
    UnknownLocationType(i16),
    InvalidTimezone(String, ParseError),
    NoAgencyDefined,
}

impl fmt::Display for GtfsErrorType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GtfsErrorType::InvalidEmptyStopName(x) => write!(
                f,
                "Station/stop name is empty when it is required, for {}",
                x
            ),
            GtfsErrorType::UnknownLocationType(x) => write!(f, "Location type {} unknown", x),
            GtfsErrorType::InvalidTimezone(x, err) => write!(f, "Invalid timezone {}: {}", x, err),
            GtfsErrorType::NoAgencyDefined => write!(f, "No transport agency was defined"),
        }
    }
}

#[derive(Debug)]
pub struct GtfsImportError {
    error_type: GtfsErrorType,
    file: String,
}

impl fmt::Display for GtfsImportError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Error reading GTFS file {}: {}",
            self.file, self.error_type
        )
    }
}

fn load_stop(stop: &Stop, default_timezone: &str) -> Result<Location, GtfsImportError> {
    let timezone = stop.timezone.as_ref().unwrap_or(&default_timezone.to_string()).clone();
    Ok(Location {
        id: stop.id.clone(),
        name: match &stop.name {
            Some(x) => x.clone(),
            None => {
                return Err(GtfsImportError {
                    error_type: GtfsErrorType::InvalidEmptyStopName(stop.id.clone()),
                    file: "stops".to_string(),
                })
            }
        },
        public_id: stop.code.clone(),
        timezone: match Tz::from_str(&timezone) {
            Ok(x) => x,
            Err(x) => {
                return Err(GtfsImportError {
                    error_type: GtfsErrorType::InvalidTimezone(timezone, x),
                    file: "stops".to_string(),
                })
            }
        },
    })
}

impl GtfsImporter {
    pub fn new() -> GtfsImporter {
        GtfsImporter { base_gtfs: None }
    }

    fn overlay_worker(
        &mut self,
        gtfs: Gtfs,
        mut schedule: Schedule,
    ) -> Result<Schedule, GtfsImportError> {
        if gtfs.agencies.len() == 0 {
            return Err(GtfsImportError {
                error_type: GtfsErrorType::NoAgencyDefined,
                file: "agency".to_string(),
            });
        }

        let default_timezone = gtfs.agencies[0].timezone.clone();

        for (stop_id, stop) in &gtfs.stops {
            match stop.location_type {
                LocationType::StopPoint => {
                    if stop.parent_station.is_none() {
                        schedule
                            .locations
                            .insert(stop_id.clone(), load_stop(stop, &default_timezone)?);
                    }
                }
                LocationType::StopArea => {
                    schedule
                        .locations
                        .insert(stop_id.clone(), load_stop(stop, &default_timezone)?);
                }
                LocationType::StationEntrance => (), // don't care
                LocationType::GenericNode => (),     // also don't care
                LocationType::BoardingArea => (), // also don't care, will be looked up later if needed
                LocationType::Unknown(x) => {
                    return Err(GtfsImportError {
                        error_type: GtfsErrorType::UnknownLocationType(x),
                        file: "stops".to_string(),
                    })
                }
            }
        }

        /*for (trip_id, trip) in &gtfs.trips {
            let train = Train {
                id: trip_id.clone(),
                validity: calculate_validities(&gtfs.calendar.get(trip.service_id), &gtfs.calendar_dates.get(trip.service_id)),
                cancellations: calculate_cancellations(&gtfs.calendar.get(trip.service_id), &gtfs.calendar_dates.get(trip.service_id)),
                replacements: vec![], // not a thing in GTFS
                days_of_week: calculate_days_of_week(&gtfs.calendar.get(trip.service_id)),
                variable_train: VariableTrain {
                },
                source: TrainSource::LongTerm, // no distinction between long and short in GTFS
                runs_as_required: false, // not a thing in GTFS
                performance_monitoring: None, // not a thing in GTFS
                route: calculate_route(&
            };
        }*/
        Ok(schedule)
    }
}

/*#[derive(Clone, Debug, Serialize)]
pub struct VariableTrain {
    pub train_type: TrainType,
    pub public_id: Option<String>,
    pub headcode: Option<String>,
    pub service_group: Option<String>,
    pub power_type: Option<TrainPower>,
    pub timing_allocation: Option<TrainAllocation>,
    pub actual_allocation: Option<TrainAllocation>,
    pub timing_speed_m_per_s: Option<f64>,
    pub operating_characteristics: OperatingCharacteristics,
    pub has_first_class_seats: bool,
    pub has_second_class_seats: bool,
    pub has_first_class_sleepers: bool,
    pub has_second_class_sleepers: bool,
    pub carries_vehicles: bool,
    pub reservations: Reservations,
    pub catering: Catering,
    pub brand: Option<String>,
    pub name: Option<String>,
    pub uic_code: Option<String>,
    pub operator: Option<TrainOperator>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Train {
    pub id: String,
    pub validity: Vec<TrainValidityPeriod>,
    pub cancellations: Vec<(TrainValidityPeriod, DaysOfWeek)>, // TODO should include TrainSource?
    pub replacements: Vec<Train>,
    pub days_of_week: DaysOfWeek,
    pub variable_train: VariableTrain,
    pub source: Option<TrainSource>,
    pub runs_as_required: bool,
    pub performance_monitoring: Option<bool>,
    pub route: Vec<TrainLocation>,
}*/

#[async_trait]
impl SlowGtfsImporter for GtfsImporter {
    async fn overlay(&mut self, gtfs: Gtfs, mut schedule: Schedule) -> Result<Schedule, Error> {
        schedule = block_in_place(move || self.overlay_worker(gtfs, schedule))?;
        Ok(schedule)
    }
}
