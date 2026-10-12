use crate::error::Error;
use crate::importer::{FastImporter, MainScheduleStatus};
use crate::schedule::{
    Direction, TrainSource, TrainType, line, location, schedule, sublocation, train,
    train_cancellation, train_location, train_validity_period, train_variant, variable_train
};

use async_trait::async_trait;

use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, TimeZone};
use chrono::format::ParseError;
use chrono_tz::Tz;

use gtfs_rt_decode::decode;
use gtfs_rt_decode::gtfs_rt_types::{
    FeedEntity, FeedHeader, FeedMessage, TripDescriptor, TripUpdate,
};
use gtfs_rt_decode::gtfs_rt_types::trip_descriptor::ScheduleRelationship;
use gtfs_rt_decode::gtfs_rt_types::trip_update::stop_time_update::{
    ScheduleRelationship as StopScheduleRelationship
};
use gtfs_rt_decode::gtfs_rt_types::trip_update::stop_time_update::stop_time_properties::{
    DropOffPickupType,
};
use gtfs_rt_decode::gtfs_rt_types::vehicle_descriptor::WheelchairAccessible;

use prost::UnknownEnumValue;
use prost::bytes::Bytes;

use sea_orm::{ColumnTrait, DatabaseTransaction, DbErr, EntityLoaderTrait, QueryFilter};
use sea_orm::entity::{ActiveHasMany, ActiveHasOne, ActiveValue};
use sea_orm::prelude::HasMany;

use std::fmt;
use std::collections::HashMap;
use std::str::FromStr;

#[derive(Clone, Debug)]
pub enum GtfsRtErrorType {
    ChronoParseError(ParseError),
    InvalidDirectionId(u32),
    InvalidDropOffPickupType(UnknownEnumValue),
    InvalidScheduleRelationship(UnknownEnumValue),
    InvalidTripKey(String),
    InvalidWheelchairAccessible(UnknownEnumValue),
    MissingArrival(usize),
    MissingDeparture(usize),
    MissingStopId(usize),
    RouteNotFound(String),
}

impl fmt::Display for GtfsRtErrorType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GtfsRtErrorType::ChronoParseError(x)
                => write!(f, "Failed to parse date and/or time: {}", x),
            GtfsRtErrorType::InvalidDirectionId(x) => write!(f, "Invalid direction_id {}", x),
            GtfsRtErrorType::InvalidDropOffPickupType(x)
                => write!(f, "Invalid DropOffPickupType {}", x),
            GtfsRtErrorType::InvalidScheduleRelationship(x)
                => write!(f, "Invalid ScheduleRelationship {}", x),
            GtfsRtErrorType::InvalidTripKey(x) => write!(
                f,
                "Invalid trip key. If no trip_id is present, all four of route_id, direction_id, \
                start_time, and start_date must be present. Missing {}",
                x
            ),
            GtfsRtErrorType::InvalidWheelchairAccessible(x)
                => write!(f, "Invalid WheelchairAccessible {}", x),
            GtfsRtErrorType::MissingArrival(x)
                => write!(f, "Missing arrival for stop update {}", x),
            GtfsRtErrorType::MissingDeparture(x)
                => write!(f, "Missing departure for stop update {}", x),
            GtfsRtErrorType::MissingStopId(x) => write!(f, "Missing stop_id for stop update {}", x),
            GtfsRtErrorType::RouteNotFound(x) => write!(f, "Route (line) {} not found in DB", x),
        }
    }
}

#[derive(Debug)]
pub struct GtfsRtError {
    error_type: GtfsRtErrorType,
    entity_id: String,
    field_name: String,
}

impl fmt::Display for GtfsRtError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Error reading GTFS-RT field {} for entity {}: {}",
            self.field_name, self.entity_id, self.error_type
        )
    }
}

pub struct GtfsRtImporter {
    treat_cancelled_as_deleted: bool,
    treat_added_as_duplicated: bool,
    extra_location_sources: Vec<String>,
}

async fn get_train_variants_by_trip_id_for_delete(
    trip_id: &str, namespace: &str, transaction: &DatabaseTransaction
) -> Result<Vec<train_variant::ModelEx>, Error> {
    let root_train_variants = train_variant::Entity::load()
        .filter(train_variant::COLUMN.train_id.eq(trip_id))
        .filter(train_variant::COLUMN.namespace.eq(namespace))
        .with(train_validity_period::Entity)
        .with((train_cancellation::Entity, train_validity_period::Entity))
        .all(transaction)
        .await?;

    let mut all_train_variants = root_train_variants.clone();
    let mut prev_train_variants = root_train_variants;
    loop {
        prev_train_variants = train_variant::Entity::load()
            .filter(train_variant::COLUMN.parent_train_variant_id.is_in(
                prev_train_variants.iter().map(|x| Some(x.id))
            ))
            .with(train_validity_period::Entity)
            .with((train_cancellation::Entity, train_validity_period::Entity))
            .all(transaction)
            .await?;
        if prev_train_variants.len() > 0 {
            all_train_variants.append(&mut prev_train_variants.clone());
        } else {
            break;
        }
    }

    Ok(all_train_variants)
}

async fn get_train_variants_by_trip_id_for_replace(
    trip_id: &str, namespace: &str, transaction: &DatabaseTransaction
) -> Result<Vec<train_variant::ModelEx>, Error> {
    let root_train_variants = train_variant::Entity::load()
        .filter(train_variant::COLUMN.train_id.eq(trip_id))
        .filter(train_variant::COLUMN.namespace.eq(namespace))
        .with(train_validity_period::Entity)
        .all(transaction)
        .await?;

    let mut all_train_variants = root_train_variants.clone();
    let mut prev_train_variants = root_train_variants;
    loop {
        prev_train_variants = train_variant::Entity::load()
            .filter(train_variant::COLUMN.parent_train_variant_id.is_in(
                prev_train_variants.iter().map(|x| Some(x.id))
            ))
            .with(train_validity_period::Entity)
            .all(transaction)
            .await?;
        if prev_train_variants.len() > 0 {
            all_train_variants.append(&mut prev_train_variants.clone());
        } else {
            break;
        }
    }

    Ok(all_train_variants)
}

fn get_alternative_trip_key(trip: &TripDescriptor, entity_id: &str)
    -> Result<(String, Direction, NaiveTime, NaiveDate), GtfsRtError> {
    let route_id = match &trip.route_id {
        Some(x) => x,
        None => return Err(GtfsRtError {
            error_type: GtfsRtErrorType::InvalidTripKey("route_id".to_string()),
            entity_id: entity_id.to_string(),
            field_name: "entity.trip_update.trip.route_id".to_string(),
        }),
    };

    let direction = match trip.direction_id {
        Some(0) => Direction::Outbound,
        Some(1) => Direction::Inbound,
        Some(x) => return Err(GtfsRtError {
            error_type: GtfsRtErrorType::InvalidDirectionId(x),
            entity_id: entity_id.to_string(),
            field_name: "entity.trip_update.trip.direction_id".to_string(),
        }),
        None => return Err(GtfsRtError {
            error_type: GtfsRtErrorType::InvalidTripKey("direction_id".to_string()),
            entity_id: entity_id.to_string(),
            field_name: "entity.trip_update.trip.direction_id".to_string(),
        }),
    };

    let start_time = match &trip.start_time {
        Some(start_time) => match NaiveTime::parse_from_str(&start_time, "%H:%M:%S") {
            Ok(x) => x,
            Err(x) => return Err(GtfsRtError {
                error_type: GtfsRtErrorType::ChronoParseError(x),
                entity_id: entity_id.to_string(),
                field_name: "entity.trip_update.trip.start_time".to_string(),
            }),
        },
        None => return Err(GtfsRtError {
            error_type: GtfsRtErrorType::InvalidTripKey("start_time".to_string()),
            entity_id: entity_id.to_string(),
            field_name: "entity.trip_update.trip.start_time".to_string(),
        }),
    };

    let start_date = match &trip.start_date {
        Some(start_date) => match NaiveDate::parse_from_str(&start_date, "%Y%m%d") {
            Ok(x) => x,
            Err(x) => return Err(GtfsRtError {
                error_type: GtfsRtErrorType::ChronoParseError(x),
                entity_id: entity_id.to_string(),
                field_name: "entity.trip_update.trip.start_date".to_string(),
            }),
        },
        None => return Err(GtfsRtError {
            error_type: GtfsRtErrorType::InvalidTripKey("start_date".to_string()),
            entity_id: entity_id.to_string(),
            field_name: "entity.trip_update.trip.start_date".to_string(),
        }),
    };

    Ok((route_id.to_string(), direction, start_time, start_date))
}

async fn get_train_variants_by_alternative_for_delete(
    trip: &TripDescriptor, namespace: &str, entity_id: &str, transaction: &DatabaseTransaction,
) -> Result<Vec<train_variant::ModelEx>, Error> {
    let (route_id, direction, start_time, _)
        = get_alternative_trip_key(trip, entity_id)?;
    let root_variable_trains = variable_train::Entity::load()
        .filter(variable_train::COLUMN.line_id.eq(route_id))
        .filter(variable_train::COLUMN.direction.eq(direction))
        .filter(variable_train::COLUMN.namespace.eq(namespace))
        .filter(variable_train::COLUMN.train_variant_id.is_not_null())
        .with((train_variant::Entity, train_validity_period::Entity))
        .with((train_variant::Entity, train_location::Entity))
        .all(transaction)
        .await?;

    // We don't actually need the variable trains
    let mut root_train_variants: Vec<train_variant::ModelEx>
        = root_variable_trains.into_iter().map(|x| x.train_variant.unwrap()).collect();

    let mut cancellations: HashMap<i64, Vec<train_cancellation::ModelEx>>
        = train_cancellation::Entity::load()
            .filter(train_cancellation::COLUMN.train_variant_id.is_in(
                root_train_variants.iter().map(|x| x.id)
            ))
            .with(train_validity_period::Entity)
            .all(transaction)
            .await?
            .into_iter()
            .fold(HashMap::new(), |mut map, item| {
                map.entry(item.train_variant_id)
                    .or_default()
                    .push(item);
                map
            });

    for variable_train in root_train_variants.iter_mut() {
        variable_train.cancellations = HasMany::Loaded(
            cancellations.entry(variable_train.id).or_insert(vec![]).clone()
        );
    }

    let mut all_train_variants = root_train_variants.clone();
    let mut prev_train_variants = root_train_variants;
    loop {
        prev_train_variants = train_variant::Entity::load()
            .filter(train_variant::COLUMN.parent_train_variant_id.is_in(
                prev_train_variants.iter().map(|x| Some(x.id))
            ))
            .with(train_validity_period::Entity)
            .with(train_location::Entity)
            .with((train_cancellation::Entity, train_validity_period::Entity))
            .all(transaction)
            .await?;
        if prev_train_variants.len() > 0 {
            all_train_variants.append(&mut prev_train_variants.clone());
        } else {
            break;
        }
    }

    for train_variant in all_train_variants.iter_mut() {
        let route = match &mut train_variant.route {
            HasMany::Loaded(x) => x,
            HasMany::Unloaded => return Err(
                DbErr::RecordNotFound("train_variant.route not loaded".to_owned()).into()
            ),
        };
        route.sort_by_key(|train_location| train_location.index);
    }

    // Finding a valid date is always required, but this part is unusual so do it here; filter out
    // trains without the correct start time.
    all_train_variants.retain(|train_variant| {
        let first_location = &train_variant.route[0];
        let dep_time = match first_location.public_dep {
            Some(x) => x,
            None => first_location.working_dep.unwrap(),
        };

        dep_time == start_time
    });

    Ok(all_train_variants)
}

async fn get_train_variants_by_alternative_for_replace(
    trip: &TripDescriptor, namespace: &str, entity_id: &str, transaction: &DatabaseTransaction,
) -> Result<Vec<train_variant::ModelEx>, Error> {
    let (route_id, direction, start_time, _)
        = get_alternative_trip_key(trip, entity_id)?;
    let root_variable_trains = variable_train::Entity::load()
        .filter(variable_train::COLUMN.line_id.eq(route_id))
        .filter(variable_train::COLUMN.direction.eq(direction))
        .filter(variable_train::COLUMN.namespace.eq(namespace))
        .filter(variable_train::COLUMN.train_variant_id.is_not_null())
        .with((train_variant::Entity, train_validity_period::Entity))
        .with((train_variant::Entity, train_location::Entity))
        .all(transaction)
        .await?;

    // We don't actually need the variable trains
    let root_train_variants: Vec<train_variant::ModelEx>
        = root_variable_trains.into_iter().map(|x| x.train_variant.unwrap()).collect();

    let mut all_train_variants = root_train_variants.clone();
    let mut prev_train_variants = root_train_variants;
    loop {
        prev_train_variants = train_variant::Entity::load()
            .filter(train_variant::COLUMN.parent_train_variant_id.is_in(
                prev_train_variants.iter().map(|x| Some(x.id))
            ))
            .with(train_validity_period::Entity)
            .with(train_location::Entity)
            .all(transaction)
            .await?;
        if prev_train_variants.len() > 0 {
            all_train_variants.append(&mut prev_train_variants.clone());
        } else {
            break;
        }
    }

    for train_variant in all_train_variants.iter_mut() {
        let route = match &mut train_variant.route {
            HasMany::Loaded(x) => x,
            HasMany::Unloaded => return Err(
                DbErr::RecordNotFound("train_variant.route not loaded".to_owned()).into()
            ),
        };
        route.sort_by_key(|train_location| train_location.index);
    }

    // Finding a valid date is always required, but this part is unusual so do it here; filter out
    // trains without the correct start time.
    all_train_variants.retain(|train_variant| {
        let first_location = &train_variant.route[0];
        let dep_time = match first_location.public_dep {
            Some(x) => x,
            None => first_location.working_dep.unwrap(),
        };

        dep_time == start_time
    });

    Ok(all_train_variants)
}

async fn train_exists_with_trip_id(
    trip_id: &str, namespace: &str, transaction: &DatabaseTransaction
) -> Result<bool, Error> {
    let train = train::Entity::load()
        .filter_by_id((trip_id.to_string(), namespace.to_string()))
        .one(transaction)
        .await?;

    Ok(train.is_some())
}

fn is_train_variant_applicable(train_variant: &train_variant::ModelEx, date: NaiveDate)
    -> bool {
    for validity in &train_variant.validity {
        if validity.valid_begin.date() > date || validity.valid_end.date() < date {
            continue;
        }

        let days_of_week = train_validity_period::DaysOfWeek::get_from_model(validity);

        if !days_of_week.get_by_weekday(date.weekday()) {
            continue;
        }
        
        return true;
    }

    false
}

async fn train_variant_from_trip(
    trip_update: &TripUpdate,
    namespace: &str,
    timezone: &str,
    line: &Option<line::ModelEx>,
    entity_id: &str,
    is_duplicated: bool,
    extra_location_sources: &Vec<String>,
    transaction: &DatabaseTransaction,
) -> Result<Option<train_variant::ActiveModelEx>, Error> {
    // Unwrap because this is from the database so should be good
    let parsed_timezone = Tz::from_str(&timezone).unwrap();

    let start_date = if is_duplicated {
        panic!("Not yet supported");
    } else {
        match &trip_update.trip.start_date {
            Some(x) => match NaiveDate::parse_from_str(x, "%Y%m%d") {
                Ok(x) => x,
                Err(x) => return Err(GtfsRtError {
                    error_type: GtfsRtErrorType::ChronoParseError(x),
                    entity_id: entity_id.to_string(),
                    field_name: "entity.trip_update.trip.start_date".to_string(),
                }.into()),
            },
            None => {
                let stop_time_update = &trip_update.stop_time_update[0];

                match &stop_time_update.departure {
                    Some(stop_time_event) => {
                        let unix_time = match stop_time_event.scheduled_time {
                            Some(scheduled_time) => scheduled_time,
                            None => match stop_time_event.time {
                                Some(time) => time - i64::from(stop_time_event.delay.unwrap_or(0)),
                                None => return Err(GtfsRtError {
                                    error_type: GtfsRtErrorType::MissingDeparture(0),
                                    entity_id: entity_id.to_string(),
                                    field_name:
                                        "entity.trip_update.stop_time_update.departure.time".to_string(),
                                }.into()),
                            },
                        };

                        let stop_datetime = parsed_timezone.timestamp_opt(unix_time, 0).unwrap();

                        stop_datetime.date_naive()
                    },
                    None => return Err(GtfsRtError {
                        error_type: GtfsRtErrorType::MissingDeparture(0),
                        entity_id: entity_id.to_string(),
                        field_name: "entity.trip_update.stop_time_update.departure".to_string(),
                    }.into()),
                }
            },
        }
    };

    let mut validity = train_validity_period::ActiveModelEx {
        timezone: ActiveValue::Set(timezone.to_string()),
        valid_begin: ActiveValue::Set(start_date.and_hms_opt(0, 0, 0).unwrap()),
        valid_end: ActiveValue::Set(start_date.and_hms_opt(0, 0, 0).unwrap()),
        ..Default::default()
    };

    validity.populate_single_weekday(start_date.weekday());

    let variable_train = variable_train::ActiveModelEx {
        namespace: ActiveValue::Set(namespace.to_string()),
        // you may think we should set the actual allocation to vehicle, but that's the realtime
        // allocation; this field is intended for the booked vehicle type.
        line_id: ActiveValue::Set(trip_update.trip.route_id.clone()),
        operator_id: ActiveValue::Set(line.as_ref().and_then(|x| x.operator_id.clone())),
        public_id: ActiveValue::Set(
            trip_update.trip_properties.as_ref().and_then(|x| x.trip_short_name.clone())
        ),
        headcode: ActiveValue::Set(
            trip_update.trip_properties.as_ref().and_then(|x| x.trip_headsign.clone())
        ),
        // If there's no line specified we can't figure out what type of train it is
        train_type: ActiveValue::Set(
            line.as_ref().and_then(|x| x.train_type.clone()).unwrap_or(TrainType::Unknown)
        ),
        power_type: ActiveValue::Set(None),
        timing_speed_m_per_s: ActiveValue::Set(None),
        brand_id: ActiveValue::Set(None),
        name: ActiveValue::Set(None),
        uic_code: ActiveValue::Set(None),
        wheelchair_accessible: ActiveValue::Set(
            match trip_update.vehicle.as_ref().and_then(|x| x.wheelchair_accessible.clone()) {
                None => None,
                Some(wheelchair_accessible) => {
                    let wheelchair_accessible: WheelchairAccessible
                        = match wheelchair_accessible.try_into() {
                            Ok(x) => x,
                            Err(x) => return Err(GtfsRtError {
                                error_type: GtfsRtErrorType::InvalidWheelchairAccessible(x),
                                entity_id: entity_id.to_string(),
                                field_name:
                                    "entity.trip_update.vehicle.wheelchair_accessible".to_string(),
                            }.into()),
                        };
                    match wheelchair_accessible {
                        WheelchairAccessible::NoValue => None,
                        WheelchairAccessible::Unknown => None,
                        WheelchairAccessible::WheelchairAccessible => Some(true),
                        WheelchairAccessible::WheelchairInaccessible => Some(false),
                    }
                },
            }
        ),
        direction: ActiveValue::Set(trip_update.trip.direction_id.map(|direction| match direction {
            0 => Ok(Direction::Outbound),
            1 => Ok(Direction::Inbound),
            x => Err(GtfsRtError {
                error_type: GtfsRtErrorType::InvalidDirectionId(x),
                entity_id: entity_id.to_string(),
                field_name: "entity.trip_update.trip.direction_id".to_string(),
            }),
        }).transpose()?),
        has_luggage: ActiveValue::Set(false),
        has_operating_characteristics: ActiveValue::Set(false),
        has_catering: ActiveValue::Set(false),
        has_reservations: ActiveValue::Set(false),
        has_toilets: ActiveValue::Set(false),
        has_families: ActiveValue::Set(false),
        has_passenger_communications: ActiveValue::Set(false),
        has_assistance: ActiveValue::Set(false),
        has_passenger_information: ActiveValue::Set(false),
        has_fares: ActiveValue::Set(false),
        has_occupancy: ActiveValue::Set(false),
        has_boarding_information: ActiveValue::Set(false),
        ..Default::default()
    };

    let mut current_variable_train = variable_train.clone();

    let mut route = vec![];
    for (i, stop_time_update) in trip_update.stop_time_update.iter().enumerate() {
        let schedule_relationship: StopScheduleRelationship
            = match stop_time_update.schedule_relationship {
                Some(schedule_relationship) => match schedule_relationship.try_into() {
                    Ok(x) => x,
                    Err(x) => return Err(GtfsRtError {
                        error_type: GtfsRtErrorType::InvalidScheduleRelationship(x),
                        entity_id: entity_id.to_string(),
                        field_name:
                            "entity.trip_update.stop_time_update.schedule_relationship".to_string(),
                    }.into()),
                },
                None => StopScheduleRelationship::Scheduled,
            };

        if schedule_relationship == StopScheduleRelationship::Skipped {
            // TODO maybe we should represent this as a cancelled stop instead
            continue;
        }

        let stop_id = match &stop_time_update.stop_id {
            Some(x) => x,
            None => return Err(GtfsRtError {
                error_type: GtfsRtErrorType::MissingStopId(i),
                entity_id: entity_id.to_string(),
                field_name: "entity.trip_update.stop_time_update.stop_id".to_string(),
            }.into()),
        };

        let drop_off_type = match stop_time_update.stop_time_properties.as_ref().and_then(
            |x| x.drop_off_type.clone()
        ) {
            Some(drop_off_type) => {
                let drop_off_type: DropOffPickupType = match drop_off_type.try_into() {
                    Ok(x) => x,
                    Err(x) => return Err(GtfsRtError {
                        error_type: GtfsRtErrorType::InvalidDropOffPickupType(x),
                        entity_id: entity_id.to_string(),
                        field_name:
                            "entity.trip_update.stop_time_update.stop_time_properties\
                            .drop_off_type".to_string(),
                    }.into()),
                };

                drop_off_type
            },
            None => if i == 0 { DropOffPickupType::None } else { DropOffPickupType::Regular },
        };

        let pickup_type = match stop_time_update.stop_time_properties.as_ref().and_then(
            |x| x.pickup_type.clone()
        ) {
            Some(pickup_type) => {
                let pickup_type: DropOffPickupType = match pickup_type.try_into() {
                    Ok(x) => x,
                    Err(x) => return Err(GtfsRtError {
                        error_type: GtfsRtErrorType::InvalidDropOffPickupType(x),
                        entity_id: entity_id.to_string(),
                        field_name:
                            "entity.trip_update.stop_time_update.stop_time_properties\
                            .pickup_type".to_string(),
                    }.into()),
                };

                pickup_type
            },
            None => if i == trip_update.stop_time_update.len() - 1 {
                DropOffPickupType::None
            } else {
                DropOffPickupType::Regular
            },
        };

        let (arrival_time, arrival_time_day) = match &stop_time_update.arrival {
            Some(stop_time_event) => {
                let unix_time = match stop_time_event.scheduled_time {
                    Some(scheduled_time) => scheduled_time,
                    None => match stop_time_event.time {
                        Some(time) => time - i64::from(stop_time_event.delay.unwrap_or(0)),
                        None => return Err(GtfsRtError {
                            error_type: GtfsRtErrorType::MissingArrival(i),
                            entity_id: entity_id.to_string(),
                            field_name:
                                "entity.trip_update.stop_time_update.arrival.time".to_string(),
                        }.into()),
                    },
                };

                let stop_datetime = parsed_timezone.timestamp_opt(unix_time, 0).unwrap();

                (
                    Some(stop_datetime.time()),
                    Some(
                        u8::try_from((stop_datetime.date_naive() - start_date).num_days()).unwrap()
                    ),
                )
            },
            None => if i == 0 {
                (None, None)
            } else {
                return Err(GtfsRtError {
                    error_type: GtfsRtErrorType::MissingArrival(i),
                    entity_id: entity_id.to_string(),
                    field_name: "entity.trip_update.stop_time_update.arrival".to_string(),
                }.into())
            },
        };

        let (departure_time, departure_time_day) = match &stop_time_update.departure {
            Some(stop_time_event) => {
                let unix_time = match stop_time_event.scheduled_time {
                    Some(scheduled_time) => scheduled_time,
                    None => match stop_time_event.time {
                        Some(time) => time - i64::from(stop_time_event.delay.unwrap_or(0)),
                        None => return Err(GtfsRtError {
                            error_type: GtfsRtErrorType::MissingDeparture(i),
                            entity_id: entity_id.to_string(),
                            field_name:
                                "entity.trip_update.stop_time_update.departure.time".to_string(),
                        }.into()),
                    },
                };

                let stop_datetime = parsed_timezone.timestamp_opt(unix_time, 0).unwrap();

                (
                    Some(stop_datetime.time()),
                    Some(
                        u8::try_from((stop_datetime.date_naive() - start_date).num_days()).unwrap()
                    ),
                )
            },
            None => if i == trip_update.stop_time_update.len() - 1 {
                (None, None)
            } else {
                return Err(GtfsRtError {
                    error_type: GtfsRtErrorType::MissingDeparture(i),
                    entity_id: entity_id.to_string(),
                    field_name: "entity.trip_update.stop_time_update.departure".to_string(),
                }.into())
            },
        };

        let (working_arr, working_arr_day) = match drop_off_type {
            DropOffPickupType::None => (arrival_time, arrival_time_day),
            DropOffPickupType::Regular
            | DropOffPickupType::PhoneAgency
            | DropOffPickupType::CoordinateWithDriver => (None, None),
        };
        let (working_dep, working_dep_day) = match pickup_type {
            DropOffPickupType::None => (departure_time, departure_time_day),
            DropOffPickupType::Regular
            | DropOffPickupType::PhoneAgency
            | DropOffPickupType::CoordinateWithDriver => (None, None),
        };
        let (public_arr, public_arr_day) = match drop_off_type {
            DropOffPickupType::None => (None, None),
            DropOffPickupType::Regular
            | DropOffPickupType::PhoneAgency
            | DropOffPickupType::CoordinateWithDriver => (arrival_time, arrival_time_day),
        };
        let (public_dep, public_dep_day) = match pickup_type {
            DropOffPickupType::None => (None, None),
            DropOffPickupType::Regular
            | DropOffPickupType::PhoneAgency
            | DropOffPickupType::CoordinateWithDriver => (departure_time, departure_time_day),
        };

        let stop_headsign
            = stop_time_update.stop_time_properties.as_ref().and_then(|x| x.stop_headsign.clone());

        let change_en_route = {
            if (stop_headsign.is_some()
                && stop_headsign != current_variable_train.headcode.clone().unwrap())
                || (stop_headsign.is_none()
                    && variable_train.headcode.clone().unwrap()
                    != current_variable_train.headcode.clone().unwrap())
            {
                current_variable_train.headcode = ActiveValue::Set(match stop_headsign {
                    Some(x) => Some(x.clone()),
                    None => variable_train.headcode.clone().unwrap(),
                });
                Some(Box::new(current_variable_train.clone()))
            } else {
                None
            }
        };

        let sublocation = sublocation::Entity::load()
            .filter_by_id((stop_id.clone(), namespace.to_string()))
            .one(transaction)
            .await?;

        let (location_id, platform, platform_zone) = match sublocation {
            Some(sublocation)
                => (sublocation.location_id, sublocation.platform, sublocation.platform_zone),
            None => (stop_id.clone(), None, None),
        };

        let location = location::Entity::load()
            .filter_by_id((location_id.clone(), namespace.to_string()))
            .one(transaction)
            .await?;

        match location {
            Some(_) => (),
            None => {
                // Try to find a location in other namespaces and duplicate it if found
                let locations = location::Entity::load()
                    .filter(location::COLUMN.id.eq(&location_id))
                    .filter(location::COLUMN.namespace.is_in(extra_location_sources))
                    .all(transaction)
                    .await?;
                // Arbitrarily pick the first one
                match locations.into_iter().next() {
                    Some(location) => {
                        // Duplicate the location into our namespace
                        let new_location = location::ActiveModelEx {
                            id: ActiveValue::Set(location_id.clone()),
                            namespace: ActiveValue::Set(namespace.to_string()),
                            name: ActiveValue::Set(location.name.clone()),
                            public_id: ActiveValue::Set(location.public_id.clone()),
                            timezone: ActiveValue::Set(location.timezone.clone()),
                            ..Default::default()
                        };

                        new_location.insert(transaction).await?;
                    },
                    None => {
                        println!("[{}] WARNING Location {} not found", namespace, location_id);
                        return Ok(None);
                    }
                };
            },
        };

        let train_location = train_location::ActiveModelEx {
            timing_tz: ActiveValue::Set(Some(timezone.to_string())),
            index: ActiveValue::Set(i.try_into().unwrap()),
            location_id: ActiveValue::Set(location_id),
            namespace: ActiveValue::Set(namespace.to_string()),
            id_suffix: ActiveValue::Set(stop_time_update.stop_sequence.map(|x| x.to_string())),
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
            platform: ActiveValue::Set(platform),
            platform_zone: ActiveValue::Set(platform_zone),
            line: ActiveValue::Set(None),
            path: ActiveValue::Set(None),
            engineering_allowance_s: ActiveValue::Set(None),
            pathing_allowance_s: ActiveValue::Set(None),
            performance_allowance_s: ActiveValue::Set(None),
            set_down_only: ActiveValue::Set(
                pickup_type == DropOffPickupType::None
                    && drop_off_type != DropOffPickupType::None
            ),
            pick_up_only: ActiveValue::Set(
                pickup_type != DropOffPickupType::None
                    && drop_off_type == DropOffPickupType::None
            ),
            unadvertised_stop: ActiveValue::Set(
                pickup_type == DropOffPickupType::None
                    && drop_off_type == DropOffPickupType::None
            ),
            request_pick_up: ActiveValue::Set(
                pickup_type == DropOffPickupType::CoordinateWithDriver
            ),
            request_set_down: ActiveValue::Set(
                drop_off_type == DropOffPickupType::CoordinateWithDriver
            ),
            request_pick_up_by_telephone: ActiveValue::Set(
                pickup_type == DropOffPickupType::PhoneAgency
            ),
            request_set_down_by_telephone: ActiveValue::Set(
                drop_off_type == DropOffPickupType::PhoneAgency
            ),
            normal_passenger_stop: ActiveValue::Set(
                pickup_type != DropOffPickupType::None
                    && drop_off_type != DropOffPickupType::None
            ),
            train_begins: ActiveValue::Set(i == 0),
            train_finishes: ActiveValue::Set(i == trip_update.stop_time_update.len() - 1),
            times_approximate: ActiveValue::Set(false),
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

    Ok(Some(train_variant::ActiveModelEx {
        namespace: ActiveValue::Set(namespace.to_string()),
        source: ActiveValue::Set(Some(TrainSource::VeryShortTerm)),
        runs_as_required: ActiveValue::Set(false),
        performance_monitoring: ActiveValue::Set(None),
        validity: ActiveHasMany::Append(vec![validity]),
        variable_train: ActiveHasOne::Set(Some(Box::new(variable_train))),
        route: ActiveHasMany::Append(route),
        ..Default::default()
    }))
}

impl GtfsRtImporter {
    pub fn new(
        treat_cancelled_as_deleted: bool,
        treat_added_as_duplicated: bool,
        extra_location_sources: Vec<String>,
    ) -> Self {
        Self {
            treat_cancelled_as_deleted,
            treat_added_as_duplicated,
            extra_location_sources,
        }
    }

    async fn read_header(
    &self,
        feed_header: FeedHeader,
        schedule: &schedule::ModelEx,
    ) -> Result<(MainScheduleStatus, DateTime<Tz>), Error> {
        let parsed_timezone = Tz::from_str(schedule.timezone.as_ref().unwrap()).unwrap();
        let timestamp = parsed_timezone.timestamp_opt(
            feed_header.timestamp.unwrap().try_into().unwrap(), 0
        ).unwrap();
        match feed_header.feed_version {
            Some(version) => if schedule.their_id == Some(version) {
                Ok((MainScheduleStatus::UpToDate, timestamp))
            } else {
                Ok((MainScheduleStatus::NeedsUpdate, timestamp))
            },
                None => Ok((MainScheduleStatus::NoData, timestamp)),
        }
    }

    async fn read_new(
        &self,
        trip_update: TripUpdate,
        timezone: &str,
        namespace: &str,
        entity_id: &str,
        transaction: &DatabaseTransaction
    ) -> Result<usize, Error> {
        // Often the GTFS-RT feed has a wider scope than the feed we ingest for static timings. As
        // such we first want to filter by route ID, if present.
        let line = match &trip_update.trip.route_id {
            Some(route_id) => {
                let line = line::Entity::load()
                    .filter_by_id((route_id.to_string(), namespace.to_string()))
                    .one(transaction)
                    .await?;

                if line.is_none() {
                    return Ok(0);
                }

                line
            },
            // If no route ID, we can't check
            None => None,
        };

        // Now let's find out if a train already exists matching this. Note we assume that an ADDED
        // trip's schedule does not change after creation; if this is not the case we might have to
        // consider making this automatically become an amend...

        let trip_id = match &trip_update.trip.trip_id {
            Some(trip_id) => trip_id.clone(),
            None => {
                let (route_id, direction, start_time, start_date)
                    = get_alternative_trip_key(&trip_update.trip, entity_id)?;

                // We've been given no ID for this train, so we now have to come up with one
                format!(
                    "_{}__{}__{}T{}",
                    route_id,
                    direction,
                    start_date.format("%Y-%m-%d"),
                    start_time.format("%H:%M:%S"),
                )
            },
        };
        let exists = train_exists_with_trip_id(&trip_id, namespace, transaction).await?;

        if exists {
            return Ok(0);
        }

        let train_variant = match train_variant_from_trip(
            &trip_update,
            namespace,
            timezone,
            &line,
            entity_id,
            false,
            &self.extra_location_sources,
            transaction,
        ).await? {
            Some(x) => x,
            None => return Ok(0),
        };

        let train = train::ActiveModelEx {
            id: ActiveValue::Set(trip_id.clone()),
            namespace: ActiveValue::Set(namespace.to_string()),
            train_variants: ActiveHasMany::Append(vec![train_variant]),
            ..Default::default()
        };

        train.insert(transaction).await?;
        println!("[{}] Written train ID {} from entity ID {}", namespace, trip_id, entity_id);
        Ok(1)
    }

    async fn read_duplicated(
        &self,
        _trip_update: TripUpdate,
        _timezone: &str,
        _namespace: &str,
        _timestamp: &DateTime<Tz>,
        _entity_id: &str,
        _transaction: &DatabaseTransaction
    ) -> Result<usize, Error> {
        // TODO this is awkward, let's implement it later if it ever actually comes up
        panic!("read_duplicated not implemented")
    }

    async fn read_replacement(
        &self,
        trip_update: TripUpdate,
        timezone: &str,
        namespace: &str,
        timestamp: &DateTime<Tz>,
        entity_id: &str,
        transaction: &DatabaseTransaction
    ) -> Result<usize, Error> {
        let mut train_variants = match &trip_update.trip.trip_id {
            Some(trip_id) => get_train_variants_by_trip_id_for_replace(
                trip_id, namespace, transaction
            ).await?,
            None => get_train_variants_by_alternative_for_replace(
                &trip_update.trip, namespace, entity_id, transaction
            ).await?,
        };

        // If there's no train to replace, we assume this was meant for a feed we don't consume.
        if train_variants.len() == 0 {
            return Ok(0)
        }

        let start_date = match &trip_update.trip.start_date {
            Some(start_date) => {
                match NaiveDate::parse_from_str(start_date, "%Y%m%d") {
                    Ok(x) => Some(x),
                    Err(x) => return Err(GtfsRtError {
                        error_type: GtfsRtErrorType::ChronoParseError(x),
                        entity_id: entity_id.to_string(),
                        field_name: "entity.trip_update.trip.start_date".to_string(),
                    }.into()),
                }
            },
            None => None,
        };

        // If there's already a replacement train on this day we can assume we have already replaced
        for train_variant in &train_variants {
            if train_variant.parent_train_variant_id.is_some() {
                for validity in &train_variant.validity {
                    match start_date {
                        Some(start_date) => if start_date == validity.valid_begin.date() {
                            return Ok(0);
                        },
                        None => return Ok(0),
                    };
                }
            }
        }

        let line = match &trip_update.trip.route_id {
            Some(route_id) => {
                let line = line::Entity::load()
                    .filter_by_id((route_id.to_string(), namespace.to_string()))
                    .one(transaction)
                    .await?;

                if line.is_none() {
                    return Err(GtfsRtError {
                        error_type: GtfsRtErrorType::RouteNotFound(route_id.to_string()),
                        entity_id: entity_id.to_string(),
                        field_name: "entity.trip_update.trip.route_id".to_string(),
                    }.into());
                }

                line
            },
            None => None,
        };

        let mut new_train_variant = match train_variant_from_trip(
            &trip_update,
            namespace,
            timezone,
            &line,
            entity_id,
            false,
            &self.extra_location_sources,
            transaction,
        ).await? {
            Some(x) => x,
            None => return Ok(0),
        };

        for train_variant in train_variants.iter_mut() {
            match &trip_update.trip.start_date {
                Some(start_date) => {
                    let start_date = match NaiveDate::parse_from_str(start_date, "%Y%m%d") {
                        Ok(x) => x,
                        Err(x) => return Err(GtfsRtError {
                            error_type: GtfsRtErrorType::ChronoParseError(x),
                            entity_id: entity_id.to_string(),
                            field_name: "entity.trip_update.trip.start_date".to_string(),
                        }.into()),
                    };

                    if !is_train_variant_applicable(train_variant, start_date) {
                        continue;
                    }

                    new_train_variant.parent_train_variant_id
                        = ActiveValue::Set(Some(train_variant.id));

                    new_train_variant.clone().insert(transaction).await?;
                    println!(
                        "[{}] Replaced train {} from entity ID {}",
                        namespace,
                        train_variant.train_id.as_ref().unwrap(),
                        entity_id
                    );
                },
                // If no start date, for now use the date in the update. TODO make this more
                // intelligent.
                None => {
                    let start_date = timestamp.date_naive();

                    if !is_train_variant_applicable(train_variant, start_date) {
                        continue;
                    }

                    let mut validity = train_validity_period::ActiveModelEx {
                        timezone: ActiveValue::Set(timezone.to_string()),
                        valid_begin: ActiveValue::Set(start_date.and_hms_opt(0, 0, 0).unwrap()),
                        valid_end: ActiveValue::Set(start_date.and_hms_opt(0, 0, 0).unwrap()),
                        ..Default::default()
                    };

                    validity.populate_single_weekday(start_date.weekday());

                    new_train_variant.parent_train_variant_id
                        = ActiveValue::Set(Some(train_variant.id));

                    new_train_variant.clone().insert(transaction).await?;
                    println!(
                        "[{}] Replaced train {} from entity ID {}",
                        namespace,
                        train_variant.train_id.as_ref().unwrap(),
                        entity_id
                    );
                },
            };
        }

        Ok(0)
    }

    async fn read_deleted(
        &self,
        trip_update: TripUpdate,
        timezone: &str,
        namespace: &str,
        timestamp: &DateTime<Tz>,
        entity_id: &str,
        transaction: &DatabaseTransaction
    ) -> Result<usize, Error> {
        let mut train_variants = match &trip_update.trip.trip_id {
            Some(trip_id)
                => get_train_variants_by_trip_id_for_delete(trip_id, namespace, transaction).await?,
            None => get_train_variants_by_alternative_for_delete(
                &trip_update.trip, namespace, entity_id, transaction
            ).await?,
        };

        for train_variant in train_variants.iter_mut() {
            match &trip_update.trip.start_date {
                Some(start_date) => {
                    let start_date = match NaiveDate::parse_from_str(start_date, "%Y%m%d") {
                        Ok(x) => x,
                        Err(x) => return Err(GtfsRtError {
                            error_type: GtfsRtErrorType::ChronoParseError(x),
                            entity_id: entity_id.to_string(),
                            field_name: "entity.trip_update.trip.start_date".to_string(),
                        }.into()),
                    };

                    if !is_train_variant_applicable(train_variant, start_date) {
                        continue;
                    }

                    for cancellation in &train_variant.cancellations {
                        for validity in &cancellation.validity {
                            let days_of_week
                                = train_validity_period::DaysOfWeek::get_from_model(validity);
                            if validity.valid_begin.date() <= start_date
                                && validity.valid_end.date() >= start_date
                                && days_of_week.get_by_weekday(start_date.weekday()) {
                                // Already cancelled, nothing to do
                                return Ok(0)
                            }
                        }
                    }

                    let mut validity = train_validity_period::ActiveModelEx {
                        timezone: ActiveValue::Set(timezone.to_string()),
                        valid_begin: ActiveValue::Set(start_date.and_hms_opt(0, 0, 0).unwrap()),
                        valid_end: ActiveValue::Set(start_date.and_hms_opt(0, 0, 0).unwrap()),
                        ..Default::default()
                    };

                    validity.populate_single_weekday(start_date.weekday());

                    let cancellation = train_cancellation::ActiveModelEx {
                        train_variant_id: ActiveValue::Set(train_variant.id),
                        validity: ActiveHasMany::Append(vec![validity]),
                        source: ActiveValue::Set(Some(TrainSource::VeryShortTerm)),
                        ..Default::default()
                    };

                    cancellation.insert(transaction).await?;
                    println!("[{}] Applied cancellation ID {}", namespace, entity_id);
                },
                // If no start date, for now use the date in the update. TODO make this more
                // intelligent.
                None => {
                    let start_date = timestamp.date_naive();

                    if !is_train_variant_applicable(train_variant, start_date) {
                        continue;
                    }

                    for cancellation in &train_variant.cancellations {
                        for validity in &cancellation.validity {
                            let days_of_week
                                = train_validity_period::DaysOfWeek::get_from_model(validity);
                            if validity.valid_begin.date() <= start_date
                                && validity.valid_end.date() >= start_date
                                && days_of_week.get_by_weekday(start_date.weekday()) {
                                // Already cancelled, nothing to do
                                return Ok(0)
                            }
                        }
                    }

                    let mut validity = train_validity_period::ActiveModelEx {
                        timezone: ActiveValue::Set(timezone.to_string()),
                        valid_begin: ActiveValue::Set(start_date.and_hms_opt(0, 0, 0).unwrap()),
                        valid_end: ActiveValue::Set(start_date.and_hms_opt(0, 0, 0).unwrap()),
                        ..Default::default()
                    };

                    validity.populate_single_weekday(start_date.weekday());

                    let cancellation = train_cancellation::ActiveModelEx {
                        train_variant_id: ActiveValue::Set(train_variant.id),
                        validity: ActiveHasMany::Append(vec![validity]),
                        source: ActiveValue::Set(Some(TrainSource::VeryShortTerm)),
                        ..Default::default()
                    };

                    cancellation.insert(transaction).await?;
                    println!("[{}] Applied cancellation ID {}", namespace, entity_id);
                },
            };
        }
        Ok(0)
    }

    async fn read_entity(
        &self,
        feed_entity: FeedEntity,
        timezone: &str,
        namespace: &str,
        timestamp: &DateTime<Tz>,
        transaction: &DatabaseTransaction,
    ) -> Result<usize, Error> {
        // TODO we currently only support TripUpdate. Also support, if we find any examples of these
        // being used for trains:
        // * Alert (where we can tie them to somewhere specific) — these can often be put out far
        // in advance so is a good candidate for pre-realtime support
        // * VehiclePosition (for realtime position and realtime allox)
        // * Stop (for dynamically added locations — does not seem that relevant to trains but
        // simple enough to implement)
        // * TripModifications (for short-notice alterations to multiple journeys at once).
        let trip_update = match feed_entity.trip_update {
            Some(trip_update) => trip_update,
            None => return Ok(0),
        };

        let schedule_relationship = match trip_update.trip.schedule_relationship {
            // Assume same as ScheduleRelationship::Scheduled which is currently a no-op
            None => return Ok(0),
            Some(x) => x,
        };

        let schedule_relationship: ScheduleRelationship = match schedule_relationship.try_into() {
            Ok(x) => x,
            Err(x) => return Err(GtfsRtError {
                error_type: GtfsRtErrorType::InvalidScheduleRelationship(x),
                entity_id: feed_entity.id.clone(),
                field_name: "entity.trip_update.trip.schedule_relationship".to_string(),
            }.into()),
        };

        // `Added` is deprecated but we need to handle it for client support
        #[allow(deprecated)]
        match schedule_relationship {
            // TODO relevant for realtime data only
            ScheduleRelationship::Scheduled | ScheduleRelationship::Unscheduled => Ok(0),
            ScheduleRelationship::Added => if self.treat_added_as_duplicated {
                self.read_duplicated(
                    trip_update, timezone, namespace, timestamp, &feed_entity.id, transaction
                ).await
            } else {
                self.read_new(
                    trip_update, timezone, namespace, &feed_entity.id, transaction
                ).await
            },
            ScheduleRelationship::Canceled => if self.treat_cancelled_as_deleted {
                self.read_deleted(
                    trip_update, timezone, namespace, timestamp, &feed_entity.id, transaction
                ).await
            } else {
                // TODO relevant for realtime data only
                Ok(0)
            },
            ScheduleRelationship::Replacement
                => self.read_replacement(
                    trip_update, timezone, namespace, timestamp, &feed_entity.id, transaction
                ).await,
            ScheduleRelationship::Duplicated
                => self.read_duplicated(
                    trip_update, timezone, namespace, timestamp, &feed_entity.id, transaction
                ).await,
            ScheduleRelationship::Deleted
                => self.read_deleted(
                    trip_update, timezone, namespace, timestamp, &feed_entity.id, transaction
                ).await,
            ScheduleRelationship::New
                => self.read_new(
                    trip_update, timezone, namespace, &feed_entity.id, transaction
                ).await,
        }
    }

    async fn read_feed_message(
        &self,
        gtfs_rt: FeedMessage,
        schedule: &schedule::ModelEx,
        transaction: &DatabaseTransaction,
    ) -> Result<MainScheduleStatus, Error> {
        let (main_schedule_status, timestamp) = self.read_header(gtfs_rt.header, schedule).await?;

        match main_schedule_status {
            MainScheduleStatus::UpToDate | MainScheduleStatus::NoData => (),
            MainScheduleStatus::NeedsUpdate => return Ok(main_schedule_status),
        };

        let mut train_count: usize = 0;
        for entity in gtfs_rt.entity.into_iter() {
            train_count += self.read_entity(
                entity,
                schedule.timezone.as_ref().unwrap(),
                &schedule.namespace,
                &timestamp,
                transaction,
            ).await?;
        }
        println!("[{}] Persisted {} trains from GTFS-RT", schedule.namespace, train_count);

        Ok(main_schedule_status)
    }
}

#[async_trait]
impl FastImporter for GtfsRtImporter {
    async fn overlay(
        &self, data: Vec<u8>, schedule: &schedule::ModelEx, transaction: &DatabaseTransaction
    ) -> Result<MainScheduleStatus, Error> {
        let bytes = Bytes::from(data);
        let gtfs_rt = decode::from_bytes(bytes)?;

        let res = self.read_feed_message(gtfs_rt, schedule, transaction).await?;

        Ok(res)
    }
}
