use crate::error::Error;
use crate::importer::SlowGtfsImporter;
use crate::schedule::{
    line, location, schedule, train, train_cancellation, train_location, train_operator,
    TrainSource, TrainType, train_validity_period, train_variant, variable_train,
};

use async_trait::async_trait;

use chrono::{Datelike, NaiveTime};
use chrono_tz::{ParseError, Tz};

use gtfs_structures::{
    Availability, BikesAllowedType, Calendar, CalendarDate, Exception, ExtendedRouteType, Gtfs,
    LocationType, PickupDropOffType, RouteType, Stop, StopTime, TimepointType,
};

use rgb::RGB8;

use sea_orm::DatabaseTransaction;
use sea_orm::entity::{ActiveHasMany, ActiveHasOne, ActiveValue};

use std::collections::{HashMap};
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
    UnknownExtendedRouteType(ExtendedRouteType),
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
            GtfsErrorType::UnknownExtendedRouteType(x) => {
                write!(f, "Extended route type {:#?} unknown", x)
            },
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

fn load_stop(
    stop: &Stop, default_timezone: &str, namespace: &str
) -> Result<location::ActiveModelEx, GtfsImportError> {
    let timezone = stop
        .timezone
        .as_ref()
        .unwrap_or(&default_timezone.to_string())
        .clone();
    Ok(location::ActiveModelEx {
        id: ActiveValue::Set(stop.id.clone()),
        namespace: ActiveValue::Set(namespace.to_owned()),
        name: match &stop.name {
            Some(x) => ActiveValue::Set(x.clone()),
            None => {
                return Err(GtfsImportError {
                    error_type: GtfsErrorType::InvalidEmptyStopName(stop.id.clone()),
                    file: "stops".to_string(),
                })
            }
        },
        public_id: ActiveValue::Set(match &stop.code {
            Some(x) if x == "0" => None, // Irish Rail publishes many stops with stop code 0
            Some(x) => Some(x.clone()),
            None => None,
        }),
        timezone: ActiveValue::Set(match Tz::from_str(&timezone) {
            Ok(x) => x.name().to_owned(),
            Err(x) => {
                return Err(GtfsImportError {
                    error_type: GtfsErrorType::InvalidTimezone(timezone, x),
                    file: "stops".to_string(),
                })
            }
        }),
        ..Default::default()
    })
}

fn calculate_days_of_week(calendar: &Calendar) -> train_validity_period::DaysOfWeek {
    train_validity_period::DaysOfWeek {
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
) -> Result<Vec<train_validity_period::ActiveModelEx>, GtfsImportError> {
    let timezone = match Tz::from_str(&timezone) {
        Ok(x) => x.name(),
        Err(x) => {
            return Err(GtfsImportError {
                error_type: GtfsErrorType::InvalidTimezone(timezone.to_string(), x),
                file: "agency".to_string(),
            })
        }
    };

    let mut validity = match calendar {
        Some(x) => {
            let mut validity = train_validity_period::ActiveModelEx {
                timezone: ActiveValue::Set(timezone.to_owned()),
                valid_begin: ActiveValue::Set(x.start_date.and_hms_opt(0, 0, 0).unwrap()),
                valid_end: ActiveValue::Set(x.end_date.and_hms_opt(0, 0, 0).unwrap()),
                ..Default::default()
            };
            validity.populate_days_of_week(&calculate_days_of_week(x));
            vec![validity]
        },
        None => vec![],
    };

    match calendar_dates {
        None => (),
        Some(x) => {
            for calendar_date in &**x {
                match calendar_date.exception_type {
                    Exception::Added => {
                        let mut exception = train_validity_period::ActiveModelEx {
                            timezone: ActiveValue::Set(timezone.to_owned()),
                            valid_begin: ActiveValue::Set(
                                calendar_date.date.and_hms_opt(0, 0, 0).unwrap()
                            ),
                            valid_end: ActiveValue::Set(
                                calendar_date.date.and_hms_opt(0, 0, 0).unwrap()
                            ),
                            ..Default::default()
                        };
                        exception.populate_single_weekday(calendar_date.date.weekday());
                        validity.push(exception);
                    },
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
) -> Result<Vec<train_cancellation::ActiveModelEx>, GtfsImportError> {
    let timezone = match Tz::from_str(&timezone) {
        Ok(x) => x.name(),
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
                    Exception::Deleted => {
                        let mut validity = train_validity_period::ActiveModelEx {
                            timezone: ActiveValue::Set(timezone.to_owned()),
                            valid_begin: ActiveValue::Set(
                                calendar_date.date.and_hms_opt(0, 0, 0).unwrap()
                            ),
                            valid_end: ActiveValue::Set(
                                calendar_date.date.and_hms_opt(0, 0, 0).unwrap()
                            ),
                            ..Default::default()
                        };
                        validity.populate_single_weekday(calendar_date.date.weekday());
                        cancellations.push(train_cancellation::ActiveModelEx {
                            validity: ActiveHasMany::Append(vec![validity]),
                            source: ActiveValue::Set(Some(TrainSource::ShortTerm)),
                            ..Default::default()
                        });
                    },
                    Exception::Added => (),
                }
            }
        }
    }

    Ok(cancellations)
}

fn calculate_route(
    stop_times: &Vec<StopTime>,
    variable_train: &variable_train::ActiveModelEx,
    timezone: &str,
    stops: &HashMap<String, Arc<Stop>>,
    namespace: &str,
) -> Result<Vec<train_location::ActiveModelEx>, GtfsImportError> {
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
                && stop_time.stop_headsign != current_variable_train.headcode.clone().unwrap())
                || (stop_time.stop_headsign.is_none()
                    && variable_train.headcode.clone().unwrap()
                    != current_variable_train.headcode.clone().unwrap())
            {
                current_variable_train.headcode = ActiveValue::Set(match &stop_time.stop_headsign {
                    Some(x) => Some(x.clone()),
                    None => variable_train.headcode.clone().unwrap(),
                });
                Some(Box::new(current_variable_train.clone()))
            } else {
                None
            }
        };

        let train_location = train_location::ActiveModelEx {
            timing_tz: match Tz::from_str(&timezone) {
                Ok(x) => ActiveValue::Set(Some(x.name().to_owned())),
                Err(x) => {
                    return Err(GtfsImportError {
                        error_type: GtfsErrorType::InvalidTimezone(timezone.to_string(), x),
                        file: "agency".to_string(),
                    })
                }
            },
            index: ActiveValue::Set(i.try_into().unwrap()),
            location_id: ActiveValue::Set(actual_stop_id.clone()),
            namespace: ActiveValue::Set(namespace.to_owned()),
            id_suffix: ActiveValue::Set(Some(stop_time.stop_sequence.to_string())),
            working_arr: ActiveValue::Set(working_arr),
            working_arr_day: ActiveValue::Set(working_arr_day),
            working_dep: ActiveValue::Set(working_dep),
            working_dep_day: ActiveValue::Set(working_dep_day),
            working_pass: ActiveValue::Set(None),
            working_pass_day: ActiveValue::Set(None),
            public_arr: ActiveValue::Set(public_arr),
            public_arr_day: ActiveValue::Set(public_arr_day),
            public_dep: ActiveValue::Set(public_dep),
            public_dep_day: ActiveValue::Set(public_dep_day),
            platform: ActiveValue::Set(stops
                .get(&actual_platform_id)
                .unwrap()
                .platform_code
                .clone()),
            platform_zone: ActiveValue::Set(match actual_zone_id {
                None => None,
                Some(x) => stops.get(&x).unwrap().name.clone(),
            }),
            line: ActiveValue::Set(None),
            path: ActiveValue::Set(None),
            engineering_allowance_s: ActiveValue::Set(None),
            pathing_allowance_s: ActiveValue::Set(None),
            performance_allowance_s: ActiveValue::Set(None),
            set_down_only: ActiveValue::Set(
                stop_time.pickup_type == PickupDropOffType::NotAvailable
                    && stop_time.drop_off_type != PickupDropOffType::NotAvailable
            ),
            pick_up_only: ActiveValue::Set(
                stop_time.pickup_type != PickupDropOffType::NotAvailable
                    && stop_time.drop_off_type == PickupDropOffType::NotAvailable
            ),
            unadvertised_stop: ActiveValue::Set(
                stop_time.pickup_type == PickupDropOffType::NotAvailable
                    && stop_time.drop_off_type == PickupDropOffType::NotAvailable
            ),
            request_pick_up: ActiveValue::Set(
                stop_time.pickup_type == PickupDropOffType::CoordinateWithDriver
            ),
            request_set_down: ActiveValue::Set(stop_time.drop_off_type
                == PickupDropOffType::CoordinateWithDriver
            ),
            request_pick_up_by_telephone: ActiveValue::Set(stop_time.pickup_type
                == PickupDropOffType::ArrangeByPhone
            ),
            request_set_down_by_telephone: ActiveValue::Set(stop_time.drop_off_type
                == PickupDropOffType::ArrangeByPhone
            ),
            normal_passenger_stop: ActiveValue::Set(
                stop_time.pickup_type != PickupDropOffType::NotAvailable
                    && stop_time.drop_off_type != PickupDropOffType::NotAvailable
            ),
            train_begins: ActiveValue::Set(i == 0),
            train_finishes: ActiveValue::Set(i == stop_times.len() - 1),
            times_approximate: ActiveValue::Set(match stop_time.timepoint {
                TimepointType::Approximate => true,
                TimepointType::Exact => false,
            }),
            detach: ActiveValue::Set(false),
            attach: ActiveValue::Set(false),
            other_trains_pass: ActiveValue::Set(false),
            attach_or_detach_assisting_loco: ActiveValue::Set(false),
            x_on_arrival: ActiveValue::Set(false),
            banking_loco: ActiveValue::Set(false),
            crew_change: ActiveValue::Set(false),
            examination: ActiveValue::Set(false),
            gbprtt: ActiveValue::Set(false),
            prevent_column_merge: ActiveValue::Set(false),
            prevent_third_column_merge: ActiveValue::Set(false),
            passenger_count: ActiveValue::Set(false),
            ticket_collection: ActiveValue::Set(false),
            ticket_examination: ActiveValue::Set(false),
            first_class_ticket_examination: ActiveValue::Set(false),
            selective_ticket_examination: ActiveValue::Set(false),
            change_loco: ActiveValue::Set(false),
            operational_stop: ActiveValue::Set(false),
            train_locomotive_on_rear: ActiveValue::Set(false),
            propelling: ActiveValue::Set(false),
            reversing_move: ActiveValue::Set(false),
            run_round: ActiveValue::Set(false),
            staff_stop: ActiveValue::Set(false),
            tops_reporting: ActiveValue::Set(false),
            token_etc: ActiveValue::Set(false),
            watering_stock: ActiveValue::Set(false),
            cross_at_passing_point: ActiveValue::Set(false),
            change_en_route: ActiveHasOne::Set(change_en_route),
            association_nodes: ActiveHasMany::Append(vec![]), // TODO implement becomes/forms_from
            ..Default::default()
        };

        route.push(train_location);
    }

    Ok(route)
}

fn colour_to_u32(colour: RGB8) -> u32 {
    (u32::from(colour.r) << 16) + (u32::from(colour.g) << 8) + u32::from(colour.b)
}

impl GtfsImporter {
    pub fn new() -> GtfsImporter {
        GtfsImporter { base_gtfs: None }
    }

    async fn overlay_worker(
        &mut self,
        gtfs: Gtfs,
        schedule: &schedule::ModelEx,
        transaction: &DatabaseTransaction,
    ) -> Result<(), Error> {
        if gtfs.agencies.len() == 0 {
            return Err(GtfsImportError {
                error_type: GtfsErrorType::NoAgencyDefined,
                file: "agency".to_string(),
            }.into());
        }

        let default_timezone = gtfs.agencies[0].timezone.clone();

        let default_timezone = match Tz::from_str(&default_timezone) {
            Ok(x) => x.name(),
            Err(x) => {
                return Err(GtfsImportError {
                    error_type: GtfsErrorType::InvalidTimezone(default_timezone.to_string(), x),
                    file: "agency".to_string(),
                }.into())
            }
        };

        let namespace = schedule.namespace.clone();

        let mut active_schedule: schedule::ActiveModelEx = schedule.clone().into();

        for feed_info in &gtfs.feed_info {
            active_schedule.their_id = ActiveValue::Set(feed_info.version.clone());
            active_schedule.valid_begin = ActiveValue::Set(feed_info.start_date.map(
                |x| x.and_hms_opt(0, 0, 0).unwrap()
            ));
            active_schedule.valid_end = ActiveValue::Set(feed_info.end_date.map(
                |x| x.and_hms_opt(0, 0, 0).unwrap()
            ));
            active_schedule.timezone = ActiveValue::Set(Some(default_timezone.to_owned()));
        }

        // We can't use the relationships of this to add things, as we can't use `save` due to our
        // explicit setting of PKs.
        println!("[{}] Updating root schedule...", namespace);
        active_schedule.update(transaction).await?;
        println!("[{}] Updated root schedule", namespace);

        let mut location_count: usize = 0;
        println!("[{}] Loading locations...", namespace);
        for (_, stop) in &gtfs.stops {
            match stop.location_type {
                LocationType::StopPoint => {
                    if stop.parent_station.is_none() {
                        let location = load_stop(stop, &default_timezone, &namespace)?;
                        location.insert(transaction).await?;
                        location_count += 1;
                    }
                }
                LocationType::StopArea => {
                    let location = load_stop(stop, &default_timezone, &namespace)?;
                    location.insert(transaction).await?;
                    location_count += 1;
                }
                LocationType::StationEntrance => (), // don't care
                LocationType::GenericNode => (),     // also don't care
                LocationType::BoardingArea => (), // also don't care, will be looked up later if needed
                LocationType::Unknown(x) => {
                    return Err(GtfsImportError {
                        error_type: GtfsErrorType::UnknownLocationType(x),
                        file: "stops".to_string(),
                    }.into())
                }
            }
        }
        println!("[{}] Persisted {} locations", namespace, location_count);

        let mut line_count: usize = 0;
        println!("[{}] Loading lines...", namespace);
        for (route_id, route) in &gtfs.routes {
            let line = line::ActiveModelEx {
                id: ActiveValue::Set(route_id.clone()),
                namespace: ActiveValue::Set(namespace.to_owned()),
                public_id: ActiveValue::Set(route.short_name.clone()),
                name: ActiveValue::Set(route.long_name.clone()),
                description: ActiveValue::Set(route.desc.clone()),
                url: ActiveValue::Set(route.url.clone()),
                foreground_colour: ActiveValue::Set(route.text_color.map(|x| colour_to_u32(x))),
                background_colour: ActiveValue::Set(route.color.map(|x| colour_to_u32(x))),
                ..Default::default()
            };

            line.insert(transaction).await?;
            line_count += 1;
        }

        println!("[{}] Persisted {} lines", namespace, line_count);

        let mut operator_count: usize = 0;
        println!("[{}] Loading operators...", namespace);
        for agency in &gtfs.agencies {
            let operator = train_operator::ActiveModelEx {
                id: ActiveValue::Set(match &agency.id {
                    Some(x) => x.clone(),
                    None => agency.name.clone(),
                }),
                namespace: ActiveValue::Set(namespace.to_owned()),
                public_id: ActiveValue::Set(None),
                description: ActiveValue::Set(Some(agency.name.clone())),
                ..Default::default()
            };

            operator.insert(transaction).await?;
            operator_count += 1;
        }

        println!("[{}] Persisted {} operators", namespace, operator_count);

        let mut train_count: usize = 0;
        println!("[{}] Loading trains...", namespace);
        for (trip_id, trip) in &gtfs.trips {
            let route = match &gtfs.routes.get(&trip.route_id) {
                Some(x) => (*x).clone(),
                None => {
                    return Err(GtfsImportError {
                        error_type: GtfsErrorType::RouteNotPresent(trip.route_id.clone()),
                        file: "trips".to_string(),
                    }.into())
                }
            };

            let agency = match &route.agency_id {
                Some(x) => match &gtfs.agencies.iter().find(|y| y.id == Some(x.clone())) {
                    Some(x) => (*x).clone(),
                    None => {
                        return Err(GtfsImportError {
                            error_type: GtfsErrorType::AgencyNotPresent(x.to_string()),
                            file: "routes".to_string(),
                        }.into())
                    }
                },
                None => gtfs.agencies[0].clone(),
            };

            let variable_train = variable_train::ActiveModelEx {
                namespace: ActiveValue::Set(namespace.to_owned()),
                train_type: ActiveValue::Set(match route.route_type {
                    RouteType::Tramway => TrainType::Tram,
                    RouteType::Subway => TrainType::Metro,
                    RouteType::Rail => TrainType::Passenger,
                    RouteType::Bus => TrainType::Bus,
                    RouteType::Ferry => TrainType::Ship,
                    RouteType::CableCar => TrainType::CableTram,
                    RouteType::Gondola => TrainType::CableCar,
                    RouteType::Funicular => TrainType::Funicular,
                    RouteType::Extended(extended) => match extended {
                        ExtendedRouteType::Railway => TrainType::Passenger,
                        ExtendedRouteType::HighSpeedRail => TrainType::HighSpeedPassenger,
                        ExtendedRouteType::LongDistanceTrains => TrainType::LongDistancePassenger,
                        ExtendedRouteType::InterRegionalRail => TrainType::InterregionalPassenger,
                        ExtendedRouteType::CarTransportRail => TrainType::CarCarryingPassenger,
                        ExtendedRouteType::SleeperRail => TrainType::SleeperPassenger,
                        ExtendedRouteType::RegionalRail => TrainType::RegionalPassenger,
                        ExtendedRouteType::TouristRailway => TrainType::TouristPassenger,
                        ExtendedRouteType::RailShuttleWithinComplex => TrainType::ShuttlePassenger,
                        ExtendedRouteType::SuburbanRailway => TrainType::SuburbanPassenger,
                        ExtendedRouteType::ReplacementRail => TrainType::ReplacementPassenger,
                        ExtendedRouteType::SpecialRail => TrainType::SpecialPassenger,
                        ExtendedRouteType::LorryTransportRail => TrainType::LorryCarryingPassenger,
                        ExtendedRouteType::OtherRail => TrainType::Passenger,
                        ExtendedRouteType::CrossCountryRail => TrainType::CrossCountryPassenger,
                        ExtendedRouteType::VehicleTransportRail => TrainType::CarCarryingPassenger,
                        ExtendedRouteType::RackAndPinionRailway =>
                            TrainType::RackAndPinionPassenger,
                        ExtendedRouteType::AdditionalRail => TrainType::ReliefPassenger,
                        ExtendedRouteType::Coach => TrainType::Coach,
                        ExtendedRouteType::InternationalCoach => TrainType::InternationalCoach,
                        ExtendedRouteType::NationalCoach => TrainType::NationalCoach,
                        ExtendedRouteType::ShuttleCoach => TrainType::ShuttleCoach,
                        ExtendedRouteType::RegionalCoach => TrainType::RegionalCoach,
                        ExtendedRouteType::SpecialCoach => TrainType::SpecialCoach,
                        ExtendedRouteType::SightseeingCoach => TrainType::SightseeingCoach,
                        ExtendedRouteType::TouristCoach => TrainType::TouristCoach,
                        ExtendedRouteType::CommuterCoach => TrainType::CommuterCoach,
                        ExtendedRouteType::OtherCoach => TrainType::Coach,
                        ExtendedRouteType::UrbanRailway => TrainType::UrbanPassenger,
                        ExtendedRouteType::Metro => TrainType::Metro,
                        ExtendedRouteType::Underground => TrainType::Metro,
                        ExtendedRouteType::UrbanRailwayServiceDetail => TrainType::UrbanPassenger,
                        ExtendedRouteType::OtherUrbanRailway => TrainType::UrbanPassenger,
                        ExtendedRouteType::Monorail => TrainType::Monorail,
                        ExtendedRouteType::Bus => TrainType::Bus,
                        // TODO other bus types not yet supported
                        ExtendedRouteType::Tram => TrainType::Tram,
                        // TODO other tram types not yet supported
                        ExtendedRouteType::Air => TrainType::Air,
                        ExtendedRouteType::Ferry => TrainType::Ship,
                        ExtendedRouteType::CableCar => TrainType::CableCar,
                        ExtendedRouteType::Funicular => TrainType::Funicular,
                        ExtendedRouteType::Taxi => TrainType::Taxi,
                        // TODO there's a few more here, panic for now
                        x => {
                            return Err(GtfsImportError {
                                error_type: GtfsErrorType::UnknownExtendedRouteType(x),
                                file: "routes".to_string(),
                            }.into())
                        }
                    },
                    x => {
                        return Err(GtfsImportError {
                            error_type: GtfsErrorType::UnknownRouteType(x),
                            file: "routes".to_string(),
                        }.into())
                    }
                }),
                public_id: ActiveValue::Set(trip.trip_short_name.clone()),
                operator_id: ActiveValue::Set(agency.id.clone()),
                line_id: ActiveValue::Set(Some(route.id.clone())),
                headcode: ActiveValue::Set(trip.trip_headsign.clone()),
                power_type: ActiveValue::Set(None),
                timing_speed_m_per_s: ActiveValue::Set(None),
                brand: ActiveValue::Set(None),
                name: ActiveValue::Set(None),
                uic_code: ActiveValue::Set(None),
                wheelchair_accessible: ActiveValue::Set(match trip.wheelchair_accessible {
                    Availability::InformationNotAvailable => None,
                    Availability::Available => Some(true),
                    Availability::NotAvailable => Some(false),
                    x => {
                        return Err(GtfsImportError {
                            error_type: GtfsErrorType::UnknownWheelchairAccessibility(x),
                            file: "trips".to_string(),
                        }.into())
                    }
                }),
                has_luggage: ActiveValue::Set(true),
                luggage_bicycles: ActiveValue::Set(match trip.bikes_allowed {
                    BikesAllowedType::NoBikeInfo => None,
                    BikesAllowedType::AtLeastOneBike => Some(true),
                    BikesAllowedType::NoBikesAllowed => Some(false),
                    x => {
                        return Err(GtfsImportError {
                            error_type: GtfsErrorType::UnknownBicyclesAllowed(x),
                            file: "trips".to_string(),
                        }.into())
                    }
                }),
                has_operating_characteristics: ActiveValue::Set(false),
                has_catering: ActiveValue::Set(false),
                has_reservations: ActiveValue::Set(false),
                has_toilets: ActiveValue::Set(false),
                has_families: ActiveValue::Set(false),
                has_passenger_communications: ActiveValue::Set(false),
                has_assistance: ActiveValue::Set(false),
                has_passenger_information: ActiveValue::Set(false),
                ..Default::default()
            };

            let train_variant = train_variant::ActiveModelEx {
                validity: ActiveHasMany::Append(calculate_validities(
                    &gtfs.calendar.get(&trip.service_id),
                    &gtfs.calendar_dates.get(&trip.service_id),
                    &default_timezone,
                )?),
                cancellations: ActiveHasMany::Append(calculate_cancellations(
                    &gtfs.calendar_dates.get(&trip.service_id),
                    &default_timezone,
                )?),
                variable_train: ActiveHasOne::Set(Some(Box::new(variable_train.clone()))),
                // no distinction between long and short in GTFS
                source: ActiveValue::Set(Some(TrainSource::LongTerm)),
                runs_as_required: ActiveValue::Set(false),
                performance_monitoring: ActiveValue::Set(None),
                route: ActiveHasMany::Append(calculate_route(
                    &trip.stop_times,
                    &variable_train,
                    &default_timezone,
                    &gtfs.stops,
                    &namespace,
                )?),
                ..Default::default()
            };

            let train = train::ActiveModelEx {
                id: ActiveValue::Set(trip_id.clone()),
                namespace: ActiveValue::Set(namespace.clone()),
                train_variants: ActiveHasMany::Append(vec![train_variant]),
                ..Default::default()
            };

            train.insert(transaction).await?;
            train_count += 1;
        }

        println!("[{}] Persisted {} trains", namespace, train_count);
        self.base_gtfs = Some(gtfs);
        Ok(())
    }
}

#[async_trait]
impl SlowGtfsImporter for GtfsImporter {
    async fn overlay(
        &mut self, gtfs: Gtfs, schedule: &schedule::ModelEx, transaction: &DatabaseTransaction
    ) -> Result<(), Error> {
        let namespace = schedule.namespace.clone();
        self.overlay_worker(gtfs, schedule, transaction).await?;
        println!("[{}] Successfully loaded trains from GTFS", namespace);
        Ok(())
    }
}
