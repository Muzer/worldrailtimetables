use crate::error::Error;
use crate::importer::SlowGtfsImporter;
use crate::schedule::{
    Activities, DaysOfWeek, Location, ReservationField, Reservations, Schedule, Train,
    TrainLocation, TrainOperator, TrainSource, TrainType, TrainValidityPeriod, VariableTrain,
};

use async_trait::async_trait;

use chrono::{Datelike, NaiveTime, TimeZone};
use chrono_tz::{ParseError, Tz};

use gtfs_structures::{
    Availability, BikesAllowedType, Calendar, CalendarDate, Exception, Gtfs, LocationType,
    PickupDropOffType, RouteType, Stop, StopTime, TimepointType,
};

use tokio::task::block_in_place;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

pub struct GtfsImporter {
    base_gtfs: Option<Gtfs>,
}

#[derive(Clone, Debug)]
pub enum GtfsErrorType {
    InvalidEmptyStopName(String),
    UnknownLocationType(i16),
    InvalidTimezone(String, ParseError),
    NoAgencyDefined,
    UnknownRouteType(RouteType),
    AgencyNotPresent(String),
    RouteNotPresent(String),
    UnknownWheelchairAccessibility(Availability),
    UnknownBicyclesAllowed(BikesAllowedType),
    NotEnoughStops,
    UnknownStopType(PickupDropOffType),
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
            GtfsErrorType::UnknownRouteType(x) => write!(f, "Route type {:#?} unknown", x),
            GtfsErrorType::AgencyNotPresent(x) => write!(f, "Agency {} not present", x),
            GtfsErrorType::RouteNotPresent(x) => write!(f, "Route {} not present", x),
            GtfsErrorType::UnknownWheelchairAccessibility(x) => {
                write!(f, "Wheelchair accessibility {:#?} unknown", x)
            }
            GtfsErrorType::UnknownBicyclesAllowed(x) => {
                write!(f, "Bicycles allowed {:#?} unknown", x)
            }
            GtfsErrorType::NotEnoughStops => write!(f, "Not enough stops present"),
            GtfsErrorType::UnknownStopType(x) => {
                write!(f, "Stop type {:#?} unknown", x)
            }
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
    let timezone = stop
        .timezone
        .as_ref()
        .unwrap_or(&default_timezone.to_string())
        .clone();
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

fn calculate_days_of_week(calendar: &Calendar) -> DaysOfWeek {
    DaysOfWeek {
        monday: calendar.monday,
        tuesday: calendar.tuesday,
        wednesday: calendar.wednesday,
        thursday: calendar.thursday,
        friday: calendar.friday,
        saturday: calendar.saturday,
        sunday: calendar.sunday,
    }
}

fn calculate_validities(
    calendar: &Option<&Calendar>,
    calendar_dates: &Option<&Vec<CalendarDate>>,
    timezone: &str,
) -> Result<Vec<TrainValidityPeriod>, GtfsImportError> {
    let timezone = match Tz::from_str(&timezone) {
        Ok(x) => x,
        Err(x) => {
            return Err(GtfsImportError {
                error_type: GtfsErrorType::InvalidTimezone(timezone.to_string(), x),
                file: "agency".to_string(),
            })
        }
    };

    let mut validity = match calendar {
        Some(x) => vec![TrainValidityPeriod {
            valid_begin: timezone
                .from_local_datetime(&x.start_date.and_hms_opt(0, 0, 0).unwrap())
                .unwrap(),
            valid_end: timezone
                .from_local_datetime(&x.end_date.and_hms_opt(0, 0, 0).unwrap())
                .unwrap(),
            days_of_week: calculate_days_of_week(x),
        }],
        None => vec![],
    };

    match calendar_dates {
        None => (),
        Some(x) => {
            for calendar_date in &**x {
                match calendar_date.exception_type {
                    Exception::Added => validity.push(TrainValidityPeriod {
                        valid_begin: timezone
                            .from_local_datetime(&calendar_date.date.and_hms_opt(0, 0, 0).unwrap())
                            .unwrap(),
                        valid_end: timezone
                            .from_local_datetime(&calendar_date.date.and_hms_opt(0, 0, 0).unwrap())
                            .unwrap(),
                        days_of_week: DaysOfWeek::from_single_weekday(calendar_date.date.weekday()),
                    }),
                    Exception::Deleted => (),
                }
            }
        }
    }

    Ok(validity)
}

fn calculate_cancellations(
    calendar_dates: &Option<&Vec<CalendarDate>>,
    timezone: &str,
) -> Result<Vec<(TrainValidityPeriod, TrainSource)>, GtfsImportError> {
    let timezone = match Tz::from_str(&timezone) {
        Ok(x) => x,
        Err(x) => {
            return Err(GtfsImportError {
                error_type: GtfsErrorType::InvalidTimezone(timezone.to_string(), x),
                file: "agency".to_string(),
            })
        }
    };

    let mut cancellations = vec![];

    match calendar_dates {
        None => (),
        Some(x) => {
            for calendar_date in &**x {
                match calendar_date.exception_type {
                    Exception::Deleted => cancellations.push((
                        TrainValidityPeriod {
                            valid_begin: timezone
                                .from_local_datetime(
                                    &calendar_date.date.and_hms_opt(0, 0, 0).unwrap(),
                                )
                                .unwrap(),
                            valid_end: timezone
                                .from_local_datetime(
                                    &calendar_date.date.and_hms_opt(0, 0, 0).unwrap(),
                                )
                                .unwrap(),
                            days_of_week: DaysOfWeek::from_single_weekday(
                                calendar_date.date.weekday(),
                            ),
                        },
                        TrainSource::ShortTerm,
                    )),
                    Exception::Added => (),
                }
            }
        }
    }

    Ok(cancellations)
}

fn calculate_route(
    stop_times: &Vec<StopTime>,
    variable_train: &VariableTrain,
    timezone: &str,
    stops: &HashMap<String, Arc<Stop>>,
    train_id: &str,
    schedule: &mut Schedule,
) -> Result<Vec<TrainLocation>, GtfsImportError> {
    let mut current_variable_train = variable_train.clone();

    if stop_times.len() < 2 {
        return Err(GtfsImportError {
            error_type: GtfsErrorType::NotEnoughStops,
            file: "stop_times".to_string(),
        });
    }

    let mut route = vec![];

    for (i, stop_time) in stop_times.iter().enumerate() {
        let (working_arr, working_arr_day) = match stop_time.drop_off_type {
            PickupDropOffType::NotAvailable => match stop_time.arrival_time {
                Some(x) => (
                    Some(
                        NaiveTime::from_num_seconds_from_midnight_opt(x % (60 * 60 * 24), 0)
                            .unwrap(),
                    ),
                    Some(u8::try_from(x / (60 * 60 * 24)).unwrap()),
                ),
                None => (None, None),
            },
            PickupDropOffType::Regular
            | PickupDropOffType::ArrangeByPhone
            | PickupDropOffType::CoordinateWithDriver => (None, None),
            x => {
                return Err(GtfsImportError {
                    error_type: GtfsErrorType::UnknownStopType(x),
                    file: "stop_times".to_string(),
                })
            }
        };
        let (working_dep, working_dep_day) = match stop_time.pickup_type {
            PickupDropOffType::NotAvailable => match stop_time.departure_time {
                Some(x) => (
                    Some(
                        NaiveTime::from_num_seconds_from_midnight_opt(x % (60 * 60 * 24), 0)
                            .unwrap(),
                    ),
                    Some(u8::try_from(x / (60 * 60 * 24)).unwrap()),
                ),
                None => (None, None),
            },
            PickupDropOffType::Regular
            | PickupDropOffType::ArrangeByPhone
            | PickupDropOffType::CoordinateWithDriver => (None, None),
            x => {
                return Err(GtfsImportError {
                    error_type: GtfsErrorType::UnknownStopType(x),
                    file: "stop_times".to_string(),
                })
            }
        };
        let (public_arr, public_arr_day) = match stop_time.drop_off_type {
            PickupDropOffType::NotAvailable => (None, None),
            PickupDropOffType::Regular
            | PickupDropOffType::ArrangeByPhone
            | PickupDropOffType::CoordinateWithDriver => match stop_time.arrival_time {
                Some(x) => (
                    Some(
                        NaiveTime::from_num_seconds_from_midnight_opt(x % (60 * 60 * 24), 0)
                            .unwrap(),
                    ),
                    Some(u8::try_from(x / (60 * 60 * 24)).unwrap()),
                ),
                None => (None, None),
            },
            x => {
                return Err(GtfsImportError {
                    error_type: GtfsErrorType::UnknownStopType(x),
                    file: "stop_times".to_string(),
                })
            }
        };
        let (public_dep, public_dep_day) = match stop_time.pickup_type {
            PickupDropOffType::NotAvailable => (None, None),
            PickupDropOffType::Regular
            | PickupDropOffType::ArrangeByPhone
            | PickupDropOffType::CoordinateWithDriver => match stop_time.departure_time {
                Some(x) => (
                    Some(
                        NaiveTime::from_num_seconds_from_midnight_opt(x % (60 * 60 * 24), 0)
                            .unwrap(),
                    ),
                    Some(u8::try_from(x / (60 * 60 * 24)).unwrap()),
                ),
                None => (None, None),
            },
            x => {
                return Err(GtfsImportError {
                    error_type: GtfsErrorType::UnknownStopType(x),
                    file: "stop_times".to_string(),
                })
            }
        };

        let (actual_stop_id, actual_platform_id, actual_zone_id) =
            match &stop_time.stop.parent_station {
                None => (stop_time.stop.id.clone(), stop_time.stop.id.clone(), None),
                Some(x) => match &stops.get(x).unwrap().parent_station {
                    Some(y) => (y.clone(), x.clone(), Some(stop_time.stop.id.clone())),
                    None => (x.clone(), stop_time.stop.id.clone(), None),
                },
            };

        let change_en_route = {
            if (stop_time.stop_headsign.is_some()
                && stop_time.stop_headsign != current_variable_train.headcode)
                || (stop_time.stop_headsign.is_none()
                    && variable_train.headcode != current_variable_train.headcode)
            {
                current_variable_train.headcode = match &stop_time.stop_headsign {
                    Some(x) => Some(x.clone()),
                    None => variable_train.headcode.clone(),
                };
                Some(current_variable_train.clone())
            } else {
                None
            }
        };

        let train_location = TrainLocation {
            timing_tz: Some(match Tz::from_str(&timezone) {
                Ok(x) => x,
                Err(x) => {
                    return Err(GtfsImportError {
                        error_type: GtfsErrorType::InvalidTimezone(timezone.to_string(), x),
                        file: "agency".to_string(),
                    })
                }
            }),
            id: actual_stop_id.clone(),
            id_suffix: Some(stop_time.stop_sequence.to_string()),
            working_arr,
            working_arr_day,
            working_dep,
            working_dep_day,
            working_pass: None,
            working_pass_day: None,
            public_arr,
            public_arr_day,
            public_dep,
            public_dep_day,
            platform: stops
                .get(&actual_platform_id)
                .unwrap()
                .platform_code
                .clone(),
            platform_zone: match actual_zone_id {
                None => None,
                Some(x) => stops.get(&x).unwrap().name.clone(),
            },
            line: None,
            path: None,
            engineering_allowance_s: None,
            pathing_allowance_s: None,
            performance_allowance_s: None,
            activities: Activities {
                set_down_only: stop_time.pickup_type == PickupDropOffType::NotAvailable
                    && stop_time.drop_off_type != PickupDropOffType::NotAvailable,
                pick_up_only: stop_time.pickup_type != PickupDropOffType::NotAvailable
                    && stop_time.drop_off_type == PickupDropOffType::NotAvailable,
                unadvertised_stop: stop_time.pickup_type == PickupDropOffType::NotAvailable
                    && stop_time.drop_off_type == PickupDropOffType::NotAvailable,
                request_pick_up: stop_time.pickup_type == PickupDropOffType::CoordinateWithDriver,
                request_set_down: stop_time.drop_off_type
                    == PickupDropOffType::CoordinateWithDriver,
                request_pick_up_by_telephone: stop_time.pickup_type
                    == PickupDropOffType::ArrangeByPhone,
                request_set_down_by_telephone: stop_time.drop_off_type
                    == PickupDropOffType::ArrangeByPhone,
                normal_passenger_stop: stop_time.pickup_type != PickupDropOffType::NotAvailable
                    && stop_time.drop_off_type != PickupDropOffType::NotAvailable,
                train_begins: i == 0,
                train_finishes: i == stop_times.len() - 1,
                times_approximate: match stop_time.timepoint {
                    TimepointType::Approximate => true,
                    TimepointType::Exact => false,
                },
                ..Default::default()
            },
            change_en_route,
            divides_to_form: vec![],
            joins_to: vec![],
            becomes: None, // TODO implement
            divides_from: vec![],
            is_joined_to_by: vec![],
            forms_from: None, // TODO implement
        };

        schedule
            .trains_indexed_by_location
            .entry(train_location.id.clone())
            .or_insert(HashSet::new())
            .insert(train_id.to_string());

        route.push(train_location);
    }

    Ok(route)
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

        let default_timezone_tz = match Tz::from_str(&default_timezone) {
            Ok(x) => x,
            Err(x) => {
                return Err(GtfsImportError {
                    error_type: GtfsErrorType::InvalidTimezone(default_timezone.to_string(), x),
                    file: "agency".to_string(),
                })
            }
        };

        for feed_info in &gtfs.feed_info {
            schedule.their_id = feed_info.version.clone();
            schedule.valid_begin = feed_info.start_date.map(|x| {
                default_timezone_tz
                    .from_local_datetime(&x.and_hms_opt(0, 0, 0).unwrap())
                    .unwrap()
            });
            schedule.valid_end = feed_info.end_date.map(|x| {
                default_timezone_tz
                    .from_local_datetime(&x.and_hms_opt(0, 0, 0).unwrap())
                    .unwrap()
            });
        }

        for (stop_id, stop) in &gtfs.stops {
            match stop.location_type {
                LocationType::StopPoint => {
                    if stop.parent_station.is_none() {
                        schedule
                            .locations
                            .insert(stop_id.clone(), load_stop(stop, &default_timezone)?);
                        match &stop.code {
                            Some(x) => {
                                schedule
                                    .locations_indexed_by_public_id
                                    .entry(x.clone())
                                    .or_insert(HashSet::new())
                                    .insert(stop_id.clone());
                            }
                            None => (),
                        }
                    }
                }
                LocationType::StopArea => {
                    schedule
                        .locations
                        .insert(stop_id.clone(), load_stop(stop, &default_timezone)?);
                    match &stop.code {
                        Some(x) => {
                            schedule
                                .locations_indexed_by_public_id
                                .entry(x.clone())
                                .or_insert(HashSet::new())
                                .insert(stop_id.clone());
                        }
                        None => (),
                    }
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

        for (trip_id, trip) in &gtfs.trips {
            let route = match &gtfs.routes.get(&trip.route_id) {
                Some(x) => (*x).clone(),
                None => {
                    return Err(GtfsImportError {
                        error_type: GtfsErrorType::RouteNotPresent(trip.route_id.clone()),
                        file: "trips".to_string(),
                    })
                }
            };

            let agency = match &route.agency_id {
                Some(x) => match &gtfs.agencies.iter().find(|y| y.id == Some(x.clone())) {
                    Some(x) => (*x).clone(),
                    None => {
                        return Err(GtfsImportError {
                            error_type: GtfsErrorType::AgencyNotPresent(x.to_string()),
                            file: "routes".to_string(),
                        })
                    }
                },
                None => gtfs.agencies[0].clone(),
            };

            let variable_train = VariableTrain {
                train_type: match gtfs.routes.get(&trip.route_id).unwrap().route_type {
                    RouteType::Tramway => TrainType::Tram,
                    RouteType::Subway => TrainType::Metro,
                    RouteType::Rail => TrainType::OrdinaryPassenger,
                    RouteType::Bus => TrainType::Bus,
                    RouteType::Ferry => TrainType::Ship,
                    RouteType::CableCar => TrainType::CableTram,
                    RouteType::Gondola => TrainType::CableCar,
                    RouteType::Funicular => TrainType::Funicular,
                    RouteType::Coach => TrainType::Coach,
                    RouteType::Taxi => TrainType::Taxi,
                    RouteType::Air => TrainType::Air,
                    RouteType::Other(11) => TrainType::Trolleybus,
                    RouteType::Other(12) => TrainType::Monorail,
                    x => {
                        return Err(GtfsImportError {
                            error_type: GtfsErrorType::UnknownRouteType(x),
                            file: "routes".to_string(),
                        })
                    }
                },
                public_id: trip.trip_short_name.clone(),
                headcode: trip.trip_headsign.clone(),
                service_group: gtfs.routes.get(&trip.route_id).unwrap().long_name.clone(),
                power_type: None,
                timing_allocation: None,
                actual_allocation: None,
                timing_speed_m_per_s: None,
                operating_characteristics: None,
                has_first_class_seats: None,
                has_second_class_seats: None,
                has_first_class_sleepers: None,
                has_second_class_sleepers: None,
                carries_vehicles: None,
                reservations: Reservations {
                    seats: ReservationField::Unknown,
                    bicycles: ReservationField::Unknown,
                    sleepers: ReservationField::Unknown,
                    vehicles: ReservationField::Unknown,
                    wheelchairs: ReservationField::Unknown,
                },
                catering: None,
                brand: None,
                name: gtfs.routes.get(&trip.route_id).unwrap().short_name.clone(),
                uic_code: None,
                operator: Some(TrainOperator {
                    id: match &agency.id {
                        Some(x) => x.clone(),
                        None => agency.name.clone(),
                    },
                    description: Some(agency.name.clone()),
                }),
                wheelchair_accessible: match trip.wheelchair_accessible {
                    Availability::InformationNotAvailable => None,
                    Availability::Available => Some(true),
                    Availability::NotAvailable => Some(false),
                    x => {
                        return Err(GtfsImportError {
                            error_type: GtfsErrorType::UnknownWheelchairAccessibility(x),
                            file: "trips".to_string(),
                        })
                    }
                },
                bicycles_allowed: match trip.bikes_allowed {
                    BikesAllowedType::NoBikeInfo => None,
                    BikesAllowedType::AtLeastOneBike => Some(true),
                    BikesAllowedType::NoBikesAllowed => Some(false),
                    x => {
                        return Err(GtfsImportError {
                            error_type: GtfsErrorType::UnknownBicyclesAllowed(x),
                            file: "trips".to_string(),
                        })
                    }
                },
            };

            let train = Train {
                id: trip_id.clone(),
                validity: calculate_validities(
                    &gtfs.calendar.get(&trip.service_id),
                    &gtfs.calendar_dates.get(&trip.service_id),
                    &default_timezone,
                )?,
                cancellations: calculate_cancellations(
                    &gtfs.calendar_dates.get(&trip.service_id),
                    &default_timezone,
                )?,
                replacements: vec![], // not a thing in GTFS
                variable_train: variable_train.clone(),
                source: Some(TrainSource::LongTerm), // no distinction between long and short in GTFS
                runs_as_required: false,             // not a thing in GTFS
                performance_monitoring: None,        // not a thing in GTFS
                route: calculate_route(
                    &trip.stop_times,
                    &variable_train,
                    &default_timezone,
                    &gtfs.stops,
                    &trip_id,
                    &mut schedule,
                )?,
            };

            match &train.variable_train.public_id {
                Some(x) => {
                    schedule
                        .trains_indexed_by_public_id
                        .entry(x.clone())
                        .or_insert(HashSet::new())
                        .insert(train.id.clone());
                }
                None => (),
            }
            schedule
                .trains
                .entry(train.id.clone())
                .or_insert(vec![])
                .push(train);
        }
        self.base_gtfs = Some(gtfs);
        Ok(schedule)
    }
}

#[async_trait]
impl SlowGtfsImporter for GtfsImporter {
    async fn overlay(&mut self, gtfs: Gtfs, mut schedule: Schedule) -> Result<Schedule, Error> {
        schedule = block_in_place(move || self.overlay_worker(gtfs, schedule))?;
        Ok(schedule)
    }
}
