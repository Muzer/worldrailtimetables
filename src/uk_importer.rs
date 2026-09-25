use crate::error::Error;
use crate::importer::{FastImporter, SlowStreamingImporter};
use crate::schedule::{
    AccommodationClass, accommodation_types, association_cancellation, association_node,
    AssociationType, line, location, ReservationField, schedule, train, train_allocation,
    train_cancellation, train_location, train_operator, TrainPower, TrainSource, TrainType,
    train_validity_period, train_variant, variable_train,
};

use async_trait::async_trait;
use chrono::format::ParseError;
use chrono::naive::Days;
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use chrono_tz::Europe::London;
use itertools::Itertools;

use sea_orm::{DatabaseTransaction, EntityLoaderTrait, EntityTrait, QueryFilter};
use sea_orm::entity::{ActiveBelongsTo, ActiveHasMany, ActiveHasOne, ActiveValue};
use sea_orm::prelude::HasMany;

use serde::{Deserialize, Serialize};

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::ops::{Add, Sub};

use tokio::fs;
use tokio::io::AsyncBufReadExt;

#[derive(Clone, Default, Deserialize)]
pub struct CifImporterConfig {
    location_overrides: Option<String>,
}

#[derive(Debug, PartialEq)]
pub enum LastTrain {
    Orphaned((String, NaiveDateTime)),
    DatabaseInsert(train_variant::ActiveModelEx),
    DatabaseSave(train_variant::ActiveModelEx),
}

#[derive(Default)]
pub struct CifImporter {
    last_train: Option<LastTrain>,
    last_train_id: Option<String>,
    unwritten_assocs:
        HashMap<(String, String, Option<String>), Vec<association_node::ActiveModelEx>>,
    change_en_route: Option<Box<variable_train::ActiveModelEx>>,
    cr_location: Option<(String, Option<String>)>,
    orphaned_overlay_trains: HashMap<(String, NaiveDateTime), train_variant::ActiveModelEx>,
    // The following caches are ephemeral; they are not considered canonical and lookups are still
    // performed if a cache miss happens. They are cleared upon completion of an operation.
    cached_operator_ids: HashSet<String>,
    cached_line_ids: HashSet<String>,
    cached_allocation_ids: HashSet<String>,
    // This cache is persistent; it is populated at the start of a process and never cleared.
    cached_train_variant_ids: HashMap<String, HashSet<i64>>,
    config: CifImporterConfig,
}

#[derive(Clone, Debug)]
pub enum CifErrorType {
    InvalidRecordType(String),
    InvalidRecordLength(usize),
    ChronoParseError(ParseError),
    LocationNotFound(String),
    InvalidTransactionType(String),
    InvalidAssociationDateIndicator(String),
    InvalidAssociationType(String),
    InvalidStpIndicator(String),
    InvalidAssociationCategory(String),
    InvalidTrainStatus(String),
    InvalidTrainCategory(String),
    InvalidTrainPower(String),
    InvalidTimingLoad(String),
    InvalidSpeed(String),
    InvalidOperatingCharacteristic(String),
    InvalidClass(String),
    InvalidReservationType(String),
    InvalidCatering(String),
    InvalidBrand(String),
    UnexpectedRecordType(String, String),
    InvalidTrainOperator(String),
    InvalidAtsCode(String),
    InvalidMinuteFraction(String),
    InvalidAllowance(String),
    InvalidActivity(String),
    InvalidWttTimesCombo,
    ChangeEnRouteLocationUnmatched((String, Option<String>), (String, Option<String>)),
    TrainNotFound(String),
    InvalidDaysOfWeek(String),
    NoScheduleSegments,
    NotEnoughLocations,
}

impl fmt::Display for CifErrorType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CifErrorType::InvalidRecordType(x) => write!(f, "Invalid Record Type {}", x),
            CifErrorType::InvalidRecordLength(x) => write!(f, "Invalid Record Length {}", x),
            CifErrorType::ChronoParseError(x) => write!(f, "Failed to parse date and/or time: {}", x),
            CifErrorType::LocationNotFound(x) => write!(f, "Location {} not present in existing schedule", x),
            CifErrorType::InvalidTransactionType(x) => write!(f, "Invalid transaction type {}", x),
            CifErrorType::InvalidAssociationDateIndicator(x) => write!(f, "Invalid association date indicator {}", x),
            CifErrorType::InvalidAssociationType(x) => write!(f, "Invalid association type {}", x),
            CifErrorType::InvalidStpIndicator(x) => write!(f, "Invalid STP indicator {}", x),
            CifErrorType::InvalidAssociationCategory(x) => write!(f, "Invalid association category {}", x),
            CifErrorType::InvalidTrainStatus(x) => write!(f, "Invalid train status {}", x),
            CifErrorType::InvalidTrainCategory(x) => write!(f, "Invalid train category {}", x),
            CifErrorType::InvalidTrainPower(x) => write!(f, "Invalid train power type {}", x),
            CifErrorType::InvalidTimingLoad(x) => write!(f, "Invalid train timing load {}", x),
            CifErrorType::InvalidSpeed(x) => write!(f, "Invalid train speed {}", x),
            CifErrorType::InvalidOperatingCharacteristic(x) => write!(f, "Invalid operating characteristic {}", x),
            CifErrorType::InvalidClass(x) => write!(f, "Invalid accommodation class {}", x),
            CifErrorType::InvalidReservationType(x) => write!(f, "Invalid reservation type {}", x),
            CifErrorType::InvalidCatering(x) => write!(f, "Invalid catering code {}", x),
            CifErrorType::InvalidBrand(x) => write!(f, "Invalid brand code {}", x),
            CifErrorType::UnexpectedRecordType(x, y) => write!(f, "Unexpected record type {} — {}", x, y),
            CifErrorType::InvalidTrainOperator(x) => write!(f, "Invalid train operator {}", x),
            CifErrorType::InvalidAtsCode(x) => write!(f, "Invalid ATS Code {}", x),
            CifErrorType::InvalidMinuteFraction(x) => write!(f, "Invalid minute fraction {}", x),
            CifErrorType::InvalidAllowance(x) => write!(f, "Invalid allowance {}", x),
            CifErrorType::InvalidActivity(x) => write!(f, "Invalid activity code {}", x),
            CifErrorType::InvalidWttTimesCombo => write!(f, "Invalid combination of WTT times; for intermediate, must be arr+dep, or pass only; for origin/destination must be dep/arr only, respectively"),
            CifErrorType::ChangeEnRouteLocationUnmatched((x, y), (a, b)) => write!(f, "Found location {}-{} but expected (from previous CR) {}-{}", x, match y { Some(y) => y, None => " ", }, a, match b { Some(b) => b, None => " ", }),
            CifErrorType::TrainNotFound(x) => write!(f, "Could not find train {}", x),
            CifErrorType::InvalidDaysOfWeek(x) => write!(f, "Invalid days of week string {}", x),
            CifErrorType::NoScheduleSegments => write!(f, "No schedule segments"),
            CifErrorType::NotEnoughLocations => write!(f, "Not enough locations"),
        }
    }
}

#[derive(Debug)]
pub struct CifError {
    error_type: CifErrorType,
    line: u64,
    column: usize,
}

#[derive(Debug)]
pub struct NrJsonError {
    error_type: CifErrorType,
    field_name: String,
}

impl fmt::Display for CifError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Error reading CIF file line {} column {}: {}",
            self.line, self.column, self.error_type
        )
    }
}

impl fmt::Display for NrJsonError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Error reading VSTP JSON field {}: {}",
            self.field_name, self.error_type
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum ModificationType {
    Insert,
    Amend,
    Delete,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum TrainStatus {
    Bus,
    Freight,
    PassengerParcels,
    Ship,
    Trip,
    StpPassengerParcels,
    StpFreight,
    StpTrip,
    StpShip,
    StpBus,
    VstpNone,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Class {
    First,
    Standard,
    Both,
    None,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct OverriddenLocation {
    id: String,
    name: String,
    public_id: Option<String>,
    timezone: String,
}

fn rev_days(
    days: &train_validity_period::DaysOfWeek, day_diff: i8
) -> train_validity_period::DaysOfWeek {
    match day_diff {
        0 => days.clone(),
        -1 => train_validity_period::DaysOfWeek {
            monday: days.tuesday,
            tuesday: days.wednesday,
            wednesday: days.thursday,
            thursday: days.friday,
            friday: days.saturday,
            saturday: days.sunday,
            sunday: days.monday,
        },
        1 => train_validity_period::DaysOfWeek {
            monday: days.sunday,
            tuesday: days.monday,
            wednesday: days.tuesday,
            thursday: days.wednesday,
            friday: days.thursday,
            saturday: days.friday,
            sunday: days.saturday,
        },
        _ => panic!("Only designed for prev or next day (as per NR)"),
    }
}

fn rev_date(date: NaiveDateTime, day_diff: i8) -> NaiveDateTime {
    if day_diff < 0 {
        date.sub(Days::new(u64::try_from(-day_diff).unwrap()))
    } else {
        date.add(Days::new(u64::try_from(day_diff).unwrap()))
    }
}

fn check_date_applicability(
    existing_validity: &train_validity_period::ActiveModelEx,
    new_begin: NaiveDateTime,
    new_end: NaiveDateTime,
    new_days: &train_validity_period::DaysOfWeek,
) -> bool {
    // check for no overlapping days at all
    if train_validity_period::DaysOfWeek::get_from_active_model(existing_validity)
        .into_iter()
        .zip(new_days.into_iter())
        .find(|(existing_day, new_day)| *existing_day && *new_day)
        .is_none()
    {
        false
    } else if new_begin > existing_validity.valid_end.clone().unwrap()
        || new_end < existing_validity.valid_begin.clone().unwrap() {
        false
    } else {
        true
    }
}

async fn write_assocs_to_trains(
    train_variants: Vec<train_variant::ModelEx>,
    location: &str,
    location_suffix: &Option<String>,
    assocs: &Vec<association_node::ActiveModelEx>,
    transaction: &DatabaseTransaction,
) -> Result<(), Error> {
    for train_variant in train_variants {
        let mut train_variant: train_variant::ActiveModelEx = train_variant.into();
        let mut changed = false;
        for train_location in train_variant.route.as_mut_vec().iter_mut() {
            if train_location.location_id.clone().unwrap() == location
                && train_location.id_suffix.clone().unwrap() == *location_suffix {
                for assoc in assocs {
                    if !check_date_applicability(
                        &train_variant.validity[0],
                        assoc.validity[0].valid_begin.clone().unwrap(),
                        assoc.validity[0].valid_end.clone().unwrap(),
                        &train_validity_period::DaysOfWeek::get_from_active_model(
                            &assoc.validity[0]
                        ),
                    ) {
                        continue;
                    }
                    // we now know this is applicable to this train, so add it
                    train_location.association_nodes.push(assoc.clone());
                    changed = true;
                }
            }
        }
        if changed {
            train_variant.save(transaction).await?;
        }
    }
    Ok(())
}

fn amend_assoc(
    assoc: &mut association_node::ActiveModelEx,
    begin: &NaiveDateTime,
    end: &NaiveDateTime,
    days_of_week: &train_validity_period::DaysOfWeek,
    day_diff: Option<i8>,
    for_passengers: Option<bool>,
    association_type: Option<AssociationType>,
    use_rev: bool,
) {
    let (new_begin, new_end, new_days) = match use_rev {
        false => (begin.clone(), end.clone(), days_of_week.clone()),
        true => (
            rev_date(*begin, assoc.day_diff.clone().unwrap()),
            rev_date(*end, assoc.day_diff.clone().unwrap()),
            rev_days(days_of_week, assoc.day_diff.clone().unwrap()),
        ),
    };
    let mut validity = train_validity_period::ActiveModelEx {
        valid_begin: ActiveValue::Set(new_begin.clone()),
        valid_end: ActiveValue::Set(new_end.clone()),
        timezone: ActiveValue::Set(London.name().to_string()),
        ..Default::default()
    };
    validity.populate_days_of_week(&new_days);
    assoc.validity = ActiveHasMany::Replace(vec![validity]);
    match day_diff {
        None => (),
        Some(x) => assoc.day_diff = ActiveValue::Set(x * if use_rev { -1 } else { 1 }),
    }
    match for_passengers {
        None => (),
        Some(x) => assoc.for_passengers = ActiveValue::Set(x),
    }
    match association_type {
        None => (),
        Some(x) => assoc.association_type = ActiveValue::Set(x),
    }
}

async fn get_all_train_variants_for_assocs(
    train_variant_ids: &HashSet<i64>, transaction: &DatabaseTransaction
) -> Result<Vec<train_variant::ModelEx>, Error> {
    // Short circuit the DB logic if the cache is empty
    if train_variant_ids.len() == 0 {
        return Ok(vec![]);
    }
    let all_train_variants = train_variant::Entity::load()
        .filter(train_variant::COLUMN.id.is_in(train_variant_ids.clone()))
        .all(transaction)
        .await?;

    Ok(all_train_variants)
}

async fn get_all_train_variants_for_delete(
    train_variant_ids: &HashSet<i64>, transaction: &DatabaseTransaction
) -> Result<Vec<train_variant::ModelEx>, Error> {
    // Short circuit the DB logic if the cache is empty
    if train_variant_ids.len() == 0 {
        return Ok(vec![]);
    }
    let all_train_variants = train_variant::Entity::load()
        .filter(train_variant::COLUMN.id.is_in(train_variant_ids.clone()))
        .with(train_validity_period::Entity)
        .with((train_cancellation::Entity, train_validity_period::Entity))
        .all(transaction)
        .await?;

    Ok(all_train_variants)
}

async fn get_all_train_variants_for_delete_without_cache(
    train_id: &str, namespace: &str, transaction: &DatabaseTransaction
) -> Result<Vec<train_variant::ModelEx>, Error> {
    let root_train_variants = train_variant::Entity::load()
        .filter(train_variant::COLUMN.train_id.eq(train_id))
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

async fn get_all_train_variants_for_cancel(
    train_variant_ids: &HashSet<i64>, transaction: &DatabaseTransaction
) -> Result<Vec<train_variant::ModelEx>, Error> {
    // Short circuit the DB logic if the cache is empty
    if train_variant_ids.len() == 0 {
        return Ok(vec![]);
    }
    let all_train_variants = train_variant::Entity::load()
        .filter(train_variant::COLUMN.id.is_in(train_variant_ids.clone()))
        .with(train_validity_period::Entity)
        .all(transaction)
        .await?;

    Ok(all_train_variants)
}

async fn get_all_train_variants_for_cancel_without_cache(
    train_id: &str, namespace: &str, transaction: &DatabaseTransaction
) -> Result<Vec<train_variant::ModelEx>, Error> {
    let root_train_variants = train_variant::Entity::load()
        .filter(train_variant::COLUMN.train_id.eq(train_id))
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

async fn get_all_train_variants_for_amend_cancel(
    train_variant_ids: &HashSet<i64>, transaction: &DatabaseTransaction
) -> Result<Vec<train_variant::ModelEx>, Error> {
    // Short circuit the DB logic if the cache is empty
    if train_variant_ids.len() == 0 {
        return Ok(vec![]);
    }
    let all_train_variants = train_variant::Entity::load()
        .filter(train_variant::COLUMN.id.is_in(train_variant_ids.clone()))
        .with((train_cancellation::Entity, train_validity_period::Entity))
        .all(transaction)
        .await?;

    Ok(all_train_variants)
}

async fn get_all_train_variants_for_amend_cancel_without_cache(
    train_id: &str, namespace: &str, transaction: &DatabaseTransaction
) -> Result<Vec<train_variant::ModelEx>, Error> {
    let root_train_variants = train_variant::Entity::load()
        .filter(train_variant::COLUMN.train_id.eq(train_id))
        .filter(train_variant::COLUMN.namespace.eq(namespace))
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

async fn get_all_train_variants_for_amend(
    train_variant_ids: &HashSet<i64>, transaction: &DatabaseTransaction
) -> Result<Vec<train_variant::ModelEx>, Error> {
    // Short circuit the DB logic if the cache is empty
    if train_variant_ids.len() == 0 {
        return Ok(vec![]);
    }
    let all_train_variants = train_variant::Entity::load()
        .filter(train_variant::COLUMN.id.is_in(train_variant_ids.clone()))
        .with(train_validity_period::Entity)
        .all(transaction)
        .await?;

    Ok(all_train_variants)
}

async fn get_all_train_variants_for_amend_without_cache(
    train_id: &str, namespace: &str, transaction: &DatabaseTransaction
) -> Result<Vec<train_variant::ModelEx>, Error> {
    let root_train_variants = train_variant::Entity::load()
        .filter(train_variant::COLUMN.train_id.eq(train_id))
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

async fn get_all_train_variants_for_replace(
    train_variant_ids: &HashSet<i64>, transaction: &DatabaseTransaction
) -> Result<Vec<train_variant::ModelEx>, Error> {
    // Short circuit the DB logic if the cache is empty
    if train_variant_ids.len() == 0 {
        return Ok(vec![]);
    }
    let all_train_variants = train_variant::Entity::load()
        .filter(train_variant::COLUMN.id.is_in(train_variant_ids.clone()))
        .with(train_validity_period::Entity)
        .all(transaction)
        .await?;

    Ok(all_train_variants)
}

async fn get_all_train_variants_for_replace_without_cache(
    train_id: &str, namespace: &str, transaction: &DatabaseTransaction
) -> Result<Vec<train_variant::ModelEx>, Error> {
    let root_train_variants = train_variant::Entity::load()
        .filter(train_variant::COLUMN.train_id.eq(train_id))
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

async fn get_all_train_variants_for_assoc_write(
    train_variant_ids: &HashSet<i64>, transaction: &DatabaseTransaction
) -> Result<Vec<train_variant::ModelEx>, Error> {
    // Short circuit the DB logic if the cache is empty
    if train_variant_ids.len() == 0 {
        return Ok(vec![]);
    }
    let all_train_variants = train_variant::Entity::load()
        .filter(train_variant::COLUMN.id.is_in(train_variant_ids.clone()))
        .with(train_validity_period::Entity)
        .with(train_location::Entity)
        .all(transaction)
        .await?;

    Ok(all_train_variants)
}

async fn get_all_associations_for_train_location(
    train_variant_ids: &HashSet<i64>,
    namespace: &str,
    location: &str,
    location_suffix: &Option<String>,
    transaction: &DatabaseTransaction,
) -> Result<Vec<association_node::ModelEx>, Error> {
    let train_variant_ids: Vec<i64>
        = get_all_train_variants_for_assocs(train_variant_ids, transaction)
        .await?
        .iter()
        .map(|x| x.id)
        .collect();
    let train_location_ids: Vec<i64> = train_location::Entity::load()
        .filter(train_location::COLUMN.train_variant_id.is_in(train_variant_ids))
        .filter(train_location::COLUMN.namespace.eq(namespace))
        .filter(train_location::COLUMN.location_id.eq(location))
        .filter(train_location::COLUMN.id_suffix.eq(location_suffix.clone()))
        .all(transaction)
        .await?
        .into_iter()
        .map(|x| x.id)
        .collect();
    let root_nodes = association_node::Entity::load()
        .filter(association_node::COLUMN.main_train_location_id.is_in(train_location_ids))
        .with(train_validity_period::Entity)
        .with((association_cancellation::Entity, train_validity_period::Entity))
        .all(transaction)
        .await?;
    let mut all_association_nodes = root_nodes.clone();
    let mut prev_association_nodes = root_nodes;
    loop {
        prev_association_nodes = association_node::Entity::load()
            .filter(association_node::COLUMN.parent_association_node_id.is_in(
                prev_association_nodes.iter().map(|x| Some(x.id))
            ))
            .with(train_validity_period::Entity)
            .with((association_cancellation::Entity, train_validity_period::Entity))
            .all(transaction)
            .await?;
        if prev_association_nodes.len() > 0 {
            all_association_nodes.append(&mut prev_association_nodes.clone());
        } else {
            break;
        }
    }

    Ok(all_association_nodes)
}

async fn trains_delete_assoc(
    train_variant_ids: &HashSet<i64>,
    other_train_id: &str,
    namespace: &str,
    begin: &NaiveDateTime,
    location: &str,
    location_suffix: &Option<String>,
    other_train_location_suffix: &Option<String>,
    stp_modification_type: &ModificationType,
    is_stp: bool,
    transaction: &DatabaseTransaction,
) -> Result<(), Error> {
    let assocs = get_all_associations_for_train_location(
        train_variant_ids,
        namespace,
        location,
        location_suffix,
        transaction
    ).await?;
    match stp_modification_type {
        ModificationType::Insert => {
            let assoc_ids_to_delete: Vec<i64> = assocs.into_iter().filter(|assoc|
                assoc.main_train_location_id.is_some()
                && assoc.other_train_id == other_train_id
                && matches!(
                    assoc.association_type,
                    AssociationType::MainDividesToFormOther
                    | AssociationType::MainJoinsToOther
                    | AssociationType::MainBecomesOther
                ) && assoc.validity[0].valid_begin == *begin
                && assoc.other_train_location_id_suffix == *other_train_location_suffix
                && assoc.source.unwrap() ==
                    if is_stp { TrainSource::ShortTerm } else { TrainSource::LongTerm }
            ).map(|x| x.id).collect();
            association_node::Entity::delete_many()
                .filter(association_node::COLUMN.id.is_in(assoc_ids_to_delete))
                .exec(transaction)
                .await?;
        },
        ModificationType::Amend => {
            let assoc_ids_to_delete: Vec<i64> = assocs.into_iter().filter(|assoc|
                assoc.main_train_location_id.is_none()
                && assoc.other_train_id == other_train_id
                && matches!(
                    assoc.association_type,
                    AssociationType::MainDividesToFormOther
                    | AssociationType::MainJoinsToOther
                    | AssociationType::MainBecomesOther
                ) && assoc.validity[0].valid_begin == *begin
                && assoc.other_train_location_id_suffix == *other_train_location_suffix
            ).map(|x| x.id).collect();
            association_node::Entity::delete_many()
                .filter(association_node::COLUMN.id.is_in(assoc_ids_to_delete))
                .exec(transaction)
                .await?;
        },
        ModificationType::Delete => {
            let cancellation_ids_to_delete: Vec<i64> = assocs
                .into_iter()
                .filter(|assoc|
                    assoc.other_train_id == other_train_id
                    && assoc.other_train_location_id_suffix == *other_train_location_suffix
                    && matches!(
                        assoc.association_type,
                        AssociationType::MainDividesToFormOther
                        | AssociationType::MainJoinsToOther
                        | AssociationType::MainBecomesOther
                    )
                ).flat_map(|assoc| assoc.cancellations.clone().into_iter().filter(|cancellation|
                    cancellation.validity[0].valid_begin == *begin
                ).collect::<Vec<association_cancellation::ModelEx>>()).map(|x| x.id)
                .collect();
            association_cancellation::Entity::delete_many()
                .filter(association_cancellation::COLUMN.id.is_in(cancellation_ids_to_delete))
                .exec(transaction)
                .await?;
        },
    };
    Ok(())
}

async fn trains_delete_rev_assoc(
    train_variant_ids: &HashSet<i64>,
    other_train_id: &str,
    namespace: &str,
    begin: &NaiveDateTime,
    location: &str,
    location_suffix: &Option<String>,
    other_train_location_suffix: &Option<String>,
    stp_modification_type: &ModificationType,
    is_stp: bool,
    transaction: &DatabaseTransaction,
) -> Result<(), Error> {
    let assocs = get_all_associations_for_train_location(
        train_variant_ids,
        namespace,
        location,
        location_suffix,
        transaction
    ).await?;
    match stp_modification_type {
        ModificationType::Insert => {
            let assoc_ids_to_delete: Vec<i64> = assocs.into_iter().filter(|assoc|
                assoc.main_train_location_id.is_some()
                && assoc.other_train_id == other_train_id
                && matches!(
                    assoc.association_type,
                    AssociationType::MainDividesFromOther
                    | AssociationType::MainIsJoinedToByOther
                    | AssociationType::MainFormsFromOther
                ) && assoc.validity[0].valid_begin == rev_date(*begin, assoc.day_diff)
                && assoc.other_train_location_id_suffix == *other_train_location_suffix
                && assoc.source.unwrap() ==
                    if is_stp { TrainSource::ShortTerm } else { TrainSource::LongTerm }
            ).map(|x| x.id).collect();
            association_node::Entity::delete_many()
                .filter(association_node::COLUMN.id.is_in(assoc_ids_to_delete))
                .exec(transaction)
                .await?;
        },
        ModificationType::Amend => {
            let assoc_ids_to_delete: Vec<i64> = assocs.into_iter().filter(|assoc|
                assoc.main_train_location_id.is_none()
                && assoc.other_train_id == other_train_id
                && matches!(
                    assoc.association_type,
                    AssociationType::MainDividesFromOther
                    | AssociationType::MainIsJoinedToByOther
                    | AssociationType::MainFormsFromOther
                ) && assoc.validity[0].valid_begin == rev_date(*begin, assoc.day_diff)
                && assoc.other_train_location_id_suffix == *other_train_location_suffix
            ).map(|x| x.id).collect();
            association_node::Entity::delete_many()
                .filter(association_node::COLUMN.id.is_in(assoc_ids_to_delete))
                .exec(transaction)
                .await?;
        },
        ModificationType::Delete => {
            let cancellation_ids_to_delete: Vec<i64> = assocs
                .into_iter()
                .filter(|assoc|
                    assoc.other_train_id == other_train_id
                    && assoc.other_train_location_id_suffix == *other_train_location_suffix
                    && matches!(
                        assoc.association_type,
                        AssociationType::MainDividesFromOther
                        | AssociationType::MainIsJoinedToByOther
                        | AssociationType::MainFormsFromOther
                    )
                ).flat_map(|assoc| assoc.cancellations.clone().into_iter().filter(|cancellation|
                    cancellation.validity[0].valid_begin == rev_date(*begin, assoc.day_diff)
                ).collect::<Vec<association_cancellation::ModelEx>>()).map(|x| x.id)
                .collect();
            association_cancellation::Entity::delete_many()
                .filter(association_cancellation::COLUMN.id.is_in(cancellation_ids_to_delete))
                .exec(transaction)
                .await?;
        },
    };
    Ok(())
}

async fn trains_amend_assoc(
    train_variant_ids: &HashSet<i64>,
    other_train_id: &str,
    namespace: &str,
    begin: &NaiveDateTime,
    end: &NaiveDateTime,
    days_of_week: &train_validity_period::DaysOfWeek,
    location: &str,
    location_suffix: &Option<String>,
    other_train_location_suffix: &Option<String>,
    stp_modification_type: &ModificationType,
    is_stp: bool,
    day_diff: Option<i8>,
    for_passengers: Option<bool>,
    association_type: Option<AssociationType>,
    transaction: &DatabaseTransaction,
) -> Result<(), Error> {
    let assocs = get_all_associations_for_train_location(
        train_variant_ids,
        namespace,
        location,
        location_suffix,
        transaction
    ).await?;
    let assocs_to_amend: Vec<association_node::ModelEx> = assocs.into_iter().filter(|assoc|
        assoc.other_train_id == other_train_id
        && matches!(
            assoc.association_type,
            AssociationType::MainDividesToFormOther
            | AssociationType::MainJoinsToOther
            | AssociationType::MainBecomesOther
        ) && assoc.other_train_location_id_suffix == *other_train_location_suffix
    ).collect();
    for assoc in &assocs_to_amend {
        if *stp_modification_type == ModificationType::Insert
            && assoc.source == Some(
                if is_stp { TrainSource::ShortTerm } else { TrainSource::LongTerm }
            )
            && assoc.validity[0].valid_begin == *begin
            && assoc.main_train_location_id.is_some()
        {
            let mut assoc: association_node::ActiveModelEx = assoc.clone().into();
            amend_assoc(
                &mut assoc,
                begin,
                end,
                days_of_week,
                day_diff,
                for_passengers,
                association_type,
                false,
            );
            assoc.save(transaction).await?;
        }
        else if *stp_modification_type == ModificationType::Amend
            && assoc.validity[0].valid_begin == *begin
            && assoc.main_train_location_id.is_none()
        {
            let mut assoc: association_node::ActiveModelEx = assoc.clone().into();
            amend_assoc(
                &mut assoc,
                begin,
                end,
                days_of_week,
                day_diff,
                for_passengers,
                association_type,
                false,
            );
            assoc.save(transaction).await?;
        }
        else if *stp_modification_type == ModificationType::Delete
        {
            for cancellation in assoc.cancellations.iter() {
                if cancellation.validity[0].valid_begin == *begin {
                    let mut cancellation: association_cancellation::ActiveModelEx
                        = cancellation.clone().into();
                    let mut validity = train_validity_period::ActiveModelEx {
                        valid_begin: ActiveValue::Set(*begin),
                        valid_end: ActiveValue::Set(*end),
                        timezone: ActiveValue::Set(London.name().to_string()),
                        ..Default::default()
                    };
                    validity.populate_days_of_week(days_of_week);

                    cancellation.validity = ActiveHasMany::Replace(vec![validity]);
                    cancellation.save(transaction).await?;
                }
            }
        }
    }
    Ok(())
}

async fn trains_amend_rev_assoc(
    train_variant_ids: &HashSet<i64>,
    other_train_id: &str,
    namespace: &str,
    begin: &NaiveDateTime,
    end: &NaiveDateTime,
    days_of_week: &train_validity_period::DaysOfWeek,
    location: &str,
    location_suffix: &Option<String>,
    other_train_location_suffix: &Option<String>,
    stp_modification_type: &ModificationType,
    is_stp: bool,
    day_diff: Option<i8>,
    for_passengers: Option<bool>,
    association_type: Option<AssociationType>,
    transaction: &DatabaseTransaction,
) -> Result<(), Error> {
    let assocs = get_all_associations_for_train_location(
        train_variant_ids,
        namespace,
        location,
        location_suffix,
        transaction
    ).await?;
    let assocs_to_amend: Vec<association_node::ModelEx> = assocs.into_iter().filter(|assoc|
        assoc.other_train_id == other_train_id
        && matches!(
            assoc.association_type,
            AssociationType::MainDividesFromOther
            | AssociationType::MainIsJoinedToByOther
            | AssociationType::MainFormsFromOther
        ) && assoc.other_train_location_id_suffix == *other_train_location_suffix
    ).collect();
    for assoc in &assocs_to_amend {
        if *stp_modification_type == ModificationType::Insert
            && assoc.source == Some(
                if is_stp { TrainSource::ShortTerm } else { TrainSource::LongTerm }
            )
            && assoc.validity[0].valid_begin == rev_date(*begin, assoc.day_diff)
            && assoc.main_train_location_id.is_some()
        {
            let mut assoc: association_node::ActiveModelEx = assoc.clone().into();
            amend_assoc(
                &mut assoc,
                begin,
                end,
                days_of_week,
                day_diff,
                for_passengers,
                association_type,
                true,
            );
            assoc.save(transaction).await?;
        }
        else if *stp_modification_type == ModificationType::Amend
            && assoc.source == Some(
                if is_stp { TrainSource::ShortTerm } else { TrainSource::LongTerm }
            )
            && assoc.validity[0].valid_begin == rev_date(*begin, assoc.day_diff)
            && assoc.main_train_location_id.is_none()
        {
            let mut assoc: association_node::ActiveModelEx = assoc.clone().into();
            amend_assoc(
                &mut assoc,
                begin,
                end,
                days_of_week,
                day_diff,
                for_passengers,
                association_type,
                true,
            );
            assoc.save(transaction).await?;
        }
        else if *stp_modification_type == ModificationType::Delete
        {
            for cancellation in assoc.cancellations.iter() {
                if cancellation.validity[0].valid_begin == rev_date(*begin, assoc.day_diff) {
                    let mut cancellation: association_cancellation::ActiveModelEx
                        = cancellation.clone().into();
                    let mut validity = train_validity_period::ActiveModelEx {
                        valid_begin: ActiveValue::Set(rev_date(*begin, assoc.day_diff)),
                        valid_end: ActiveValue::Set(rev_date(*end, assoc.day_diff)),
                        timezone: ActiveValue::Set(London.name().to_string()),
                        ..Default::default()
                    };
                    validity.populate_days_of_week(&rev_days(&days_of_week, assoc.day_diff));

                    cancellation.validity = ActiveHasMany::Replace(vec![validity]);
                    cancellation.save(transaction).await?;
                }
            }
        }
    }
    Ok(())
}

async fn trains_cancel_assoc(
    train_variant_ids: &HashSet<i64>,
    other_train_id: &str,
    namespace: &str,
    begin: &NaiveDateTime,
    end: &NaiveDateTime,
    days_of_week: &train_validity_period::DaysOfWeek,
    location: &str,
    location_suffix: &Option<String>,
    other_train_location_suffix: &Option<String>,
    transaction: &DatabaseTransaction,
) -> Result<(), Error> {
    let assocs = get_all_associations_for_train_location(
        train_variant_ids,
        namespace,
        location,
        location_suffix,
        transaction
    ).await?;
    let assocs_to_cancel: Vec<association_node::ModelEx> = assocs.into_iter().filter(|assoc|
        assoc.other_train_id == other_train_id
        && matches!(
            assoc.association_type,
            AssociationType::MainDividesToFormOther
            | AssociationType::MainJoinsToOther
            | AssociationType::MainBecomesOther
        ) && assoc.other_train_location_id_suffix == *other_train_location_suffix
    ).collect();
    for assoc in &assocs_to_cancel {
        if !check_date_applicability(
            &assoc.validity[0].clone().into(), *begin, *end, days_of_week
        ) {
            continue;
        }
        let mut validity = train_validity_period::ActiveModelEx {
            valid_begin: ActiveValue::Set(*begin),
            valid_end: ActiveValue::Set(*end),
            timezone: ActiveValue::Set(London.name().to_string()),
            ..Default::default()
        };
        validity.populate_days_of_week(days_of_week);
        let new_cancel = association_cancellation::ActiveModelEx {
            validity: ActiveHasMany::Append(vec![validity]),
            source: ActiveValue::Set(Some(TrainSource::ShortTerm)),
            ..Default::default()
        };
        let mut assoc: association_node::ActiveModelEx = assoc.clone().into();
        assoc
            .cancellations
            .push(new_cancel);
        assoc.save(transaction).await?;
    }
    Ok(())
}

async fn trains_cancel_rev_assoc(
    train_variant_ids: &HashSet<i64>,
    other_train_id: &str,
    namespace: &str,
    begin: &NaiveDateTime,
    end: &NaiveDateTime,
    days_of_week: &train_validity_period::DaysOfWeek,
    location: &str,
    location_suffix: &Option<String>,
    other_train_location_suffix: &Option<String>,
    transaction: &DatabaseTransaction,
) -> Result<(), Error> {
    let assocs = get_all_associations_for_train_location(
        train_variant_ids,
        namespace,
        location,
        location_suffix,
        transaction
    ).await?;
    let assocs_to_cancel: Vec<association_node::ModelEx> = assocs.into_iter().filter(|assoc|
        assoc.other_train_id == other_train_id
        && matches!(
            assoc.association_type,
            AssociationType::MainDividesToFormOther
            | AssociationType::MainJoinsToOther
            | AssociationType::MainBecomesOther
        ) && assoc.other_train_location_id_suffix == *other_train_location_suffix
    ).collect();
    for assoc in &assocs_to_cancel {
        let (rev_begin, rev_end, rev_days_of_week) =
            (
                rev_date(*begin, assoc.day_diff),
                rev_date(*end, assoc.day_diff),
                rev_days(&days_of_week, assoc.day_diff),
            );
        if !check_date_applicability(
            &assoc.validity[0].clone().into(), rev_begin, rev_end, &rev_days_of_week
        ) {
            continue;
        }
        let mut validity = train_validity_period::ActiveModelEx {
            valid_begin: ActiveValue::Set(rev_begin),
            valid_end: ActiveValue::Set(rev_end),
            timezone: ActiveValue::Set(London.name().to_string()),
            ..Default::default()
        };
        validity.populate_days_of_week(&rev_days_of_week);
        let new_cancel = association_cancellation::ActiveModelEx {
            validity: ActiveHasMany::Append(vec![validity]),
            source: ActiveValue::Set(Some(TrainSource::ShortTerm)),
            ..Default::default()
        };
        let mut assoc: association_node::ActiveModelEx = assoc.clone().into();
        assoc
            .cancellations
            .push(new_cancel);
        assoc.save(transaction).await?;
    }
    Ok(())
}

async fn trains_replace_assoc(
    train_variant_ids: &HashSet<i64>,
    other_train_id: &str,
    namespace: &str,
    location: &str,
    location_suffix: &Option<String>,
    other_train_location_suffix: &Option<String>,
    new_assoc: &association_node::ActiveModelEx,
    transaction: &DatabaseTransaction,
) -> Result<(), Error> {
    let assocs = get_all_associations_for_train_location(
        train_variant_ids,
        namespace,
        location,
        location_suffix,
        transaction
    ).await?;
    let assocs_to_replace: Vec<association_node::ModelEx> = assocs.into_iter().filter(|assoc|
        assoc.other_train_id == other_train_id
        && matches!(
            assoc.association_type,
            AssociationType::MainDividesToFormOther
            | AssociationType::MainJoinsToOther
            | AssociationType::MainBecomesOther
        ) && assoc.other_train_location_id_suffix == *other_train_location_suffix
    ).collect();
    for assoc in &assocs_to_replace {
        if !check_date_applicability(
            &assoc.validity[0].clone().into(),
            new_assoc.validity[0].valid_begin.clone().unwrap(),
            new_assoc.validity[0].valid_end.clone().unwrap(),
            &train_validity_period::DaysOfWeek::get_from_active_model(&new_assoc.validity[0]),
        ) {
            continue;
        }
        let mut assoc: association_node::ActiveModelEx = assoc.clone().into();
        assoc
            .replacements
            .push(new_assoc.clone());
        assoc.save(transaction).await?;
    }

    Ok(())
}

fn produce_cif_error_closure(number: u64, column: usize) -> Box<dyn Fn(CifErrorType) -> CifError> {
    Box::new(move |x| CifError {
        error_type: x.clone(),
        line: number,
        column: column,
    })
}

fn produce_nr_json_error_closure(field_name: String) -> Box<dyn Fn(CifErrorType) -> NrJsonError> {
    Box::new(move |x| NrJsonError {
        error_type: x.clone(),
        field_name: field_name.clone(),
    })
}

fn read_modification_type<F, T>(
    modification_slice: &str,
    error_logic: F,
) -> Result<ModificationType, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    match modification_slice {
        "N" => Ok(ModificationType::Insert),
        "D" => Ok(ModificationType::Delete),
        "R" => Ok(ModificationType::Amend),
        x => Err(error_logic(CifErrorType::InvalidTransactionType(
            x.to_string(),
        ))),
    }
}

fn read_stp_indicator<F, T>(stp_slice: &str, error_logic: F) -> Result<(ModificationType, bool), T>
where
    F: FnOnce(CifErrorType) -> T,
{
    let stp_modification_type = match stp_slice.trim() {
        "" => ModificationType::Insert,
        "P" => ModificationType::Insert,
        "N" => ModificationType::Insert,
        "O" => ModificationType::Amend,
        "C" => ModificationType::Delete,
        x => {
            return Err(error_logic(CifErrorType::InvalidStpIndicator(
                x.to_string(),
            )))
        }
    };
    let is_stp = match stp_slice.trim() {
        " " => false,
        "P" => false,
        "N" => true,
        "O" => true,
        "C" => true,
        x => {
            return Err(error_logic(CifErrorType::InvalidStpIndicator(
                x.to_string(),
            )))
        }
    };

    return Ok((stp_modification_type, is_stp));
}

fn read_date<F, T>(date_slice: &str, error_logic: F) -> Result<NaiveDateTime, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    let parsed_date = NaiveDate::parse_from_str(date_slice, "%y%m%d");
    let parsed_date = match parsed_date {
        Ok(x) => x,
        Err(x) => return Err(error_logic(CifErrorType::ChronoParseError(x))),
    };
    Ok(parsed_date.and_hms_opt(0, 0, 0).unwrap())
}

fn read_backwards_date<F, T>(date_slice: &str, error_logic: F) -> Result<NaiveDateTime, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    let parsed_date = NaiveDate::parse_from_str(date_slice, "%d%m%y");
    let parsed_date = match parsed_date {
        Ok(x) => x,
        Err(x) => return Err(error_logic(CifErrorType::ChronoParseError(x))),
    };
    Ok(parsed_date.and_hms_opt(0, 0, 0).unwrap())
}

fn read_vstp_date<F, T>(date_slice: &str, error_logic: F) -> Result<NaiveDateTime, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    let parsed_date = NaiveDate::parse_from_str(date_slice, "%Y-%m-%d");
    let parsed_date = match parsed_date {
        Ok(x) => x,
        Err(x) => return Err(error_logic(CifErrorType::ChronoParseError(x))),
    };
    Ok(parsed_date.and_hms_opt(0, 0, 0).unwrap())
}

fn read_optional_string(slice: &str) -> Option<String> {
    if slice.chars().fold(true, |acc, x| acc && x == ' ') {
        None
    } else {
        Some(slice.to_string())
    }
}

fn read_days_of_week<F, T>(
    slice: &str, error_logic: F
) -> Result<train_validity_period::DaysOfWeek, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    if slice
        .chars()
        .fold(false, |acc, x| acc || (x != '0' && x != '1'))
    {
        Err(error_logic(CifErrorType::InvalidDaysOfWeek(
            slice.to_string(),
        )))
    } else {
        Ok(train_validity_period::DaysOfWeek {
            monday: &slice[0..1] == "1",
            tuesday: &slice[1..2] == "1",
            wednesday: &slice[2..3] == "1",
            thursday: &slice[3..4] == "1",
            friday: &slice[4..5] == "1",
            saturday: &slice[5..6] == "1",
            sunday: &slice[6..7] == "1",
        })
    }
}

fn read_train_type<F, T>(slice: &str, error_logic: F) -> Result<Option<TrainType>, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    match slice.trim() {
        "OL" => Ok(Some(TrainType::Metro)),
        "OU" => Ok(Some(TrainType::UnadvertisedPassenger)),
        "OO" => Ok(Some(TrainType::OrdinaryPassenger)),
        "00" => Ok(Some(TrainType::OrdinaryPassenger)), // Found in NI
        "OS" => Ok(Some(TrainType::Staff)),
        "OW" => Ok(Some(TrainType::Mixed)),
        "XC" => Ok(Some(TrainType::InternationalPassenger)),
        "XD" => Ok(Some(TrainType::InternationalSleeperPassenger)),
        "XI" => Ok(Some(TrainType::InternationalPassenger)),
        "XR" => Ok(Some(TrainType::CarCarryingPassenger)),
        "XU" => Ok(Some(TrainType::UnadvertisedExpressPassenger)),
        "XX" => Ok(Some(TrainType::ExpressPassenger)),
        "XZ" => Ok(Some(TrainType::SleeperPassenger)),
        "BR" => Ok(Some(TrainType::ReplacementBus)),
        "BS" => Ok(Some(TrainType::ServiceBus)),
        "SS" => Ok(Some(TrainType::Ship)),
        "EE" => Ok(Some(TrainType::EmptyPassenger)),
        "EL" => Ok(Some(TrainType::EmptyMetro)),
        "ES" => Ok(Some(TrainType::EmptyPassengerAndStaff)),
        "JJ" => Ok(Some(TrainType::Post)),
        "PM" => Ok(Some(TrainType::Parcels)),
        "PP" => Ok(Some(TrainType::Parcels)),
        "PV" => Ok(Some(TrainType::EmptyNonPassenger)),
        "DD" => Ok(Some(TrainType::FreightDepartmental)),
        "DH" => Ok(Some(TrainType::FreightCivilEngineer)),
        "DI" => Ok(Some(TrainType::FreightMechanicalElectricalEngineer)),
        "DQ" => Ok(Some(TrainType::FreightStores)),
        "DT" => Ok(Some(TrainType::FreightTest)),
        "DY" => Ok(Some(TrainType::FreightSignalTelecoms)),
        "ZB" => Ok(Some(TrainType::LocomotiveBrakeVan)),
        "ZZ" => Ok(Some(TrainType::Locomotive)),
        "J2" => Ok(Some(TrainType::FreightAutomotiveComponents)),
        "H2" => Ok(Some(TrainType::FreightAutomotiveVehicles)),
        "J6" => Ok(Some(TrainType::FreightWagonloadBuildingMaterials)),
        "J5" => Ok(Some(TrainType::FreightChemicals)),
        "J3" => Ok(Some(TrainType::FreightEdibleProducts)),
        "J9" => Ok(Some(TrainType::FreightIntermodalContracts)),
        "H9" => Ok(Some(TrainType::FreightIntermodalOther)),
        "H8" => Ok(Some(TrainType::FreightInternational)),
        "J8" => Ok(Some(TrainType::FreightMerchandise)),
        "J4" => Ok(Some(TrainType::FreightIndustrialMinerals)),
        "A0" => Ok(Some(TrainType::FreightCoalDistributive)),
        "E0" => Ok(Some(TrainType::FreightCoalElectricity)),
        "B0" => Ok(Some(TrainType::FreightNuclear)),
        "B1" => Ok(Some(TrainType::FreightMetals)),
        "B4" => Ok(Some(TrainType::FreightAggregates)),
        "B5" => Ok(Some(TrainType::FreightWaste)),
        "B6" => Ok(Some(TrainType::FreightTrainloadBuildingMaterials)),
        "B7" => Ok(Some(TrainType::FreightPetroleum)),
        "H0" => Ok(Some(TrainType::FreightInternationalMixed)),
        "H1" => Ok(Some(TrainType::FreightInternationalIntermodal)),
        "H3" => Ok(Some(TrainType::FreightInternationalAutomotive)),
        "H4" => Ok(Some(TrainType::FreightInternationalContract)),
        "H5" => Ok(Some(TrainType::FreightInternationalHaulmark)),
        "H6" => Ok(Some(TrainType::FreightInternationalJointVenture)),
        "" => Ok(None),
        x => Err(error_logic(CifErrorType::InvalidTrainCategory(
            x.to_string(),
        ))),
    }
}

fn read_power_type<F, T>(
    power_type: &str,
    timing_load: &str,
    error_logic: F,
) -> Result<Option<TrainPower>, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    match power_type.trim() {
        "D" => Ok(Some(TrainPower::DieselLocomotive)),
        "DEM" => Ok(Some(TrainPower::DieselElectricMultipleUnit)),
        "DMU" => match timing_load {
            "" => Ok(Some(TrainPower::DieselHydraulicMultipleUnit)),
            x => match &x[0..1] {
                "D" => Ok(Some(TrainPower::DieselMechanicalMultipleUnit)),
                "V" => Ok(Some(TrainPower::DieselElectricMultipleUnit)),
                "7" => Ok(Some(TrainPower::ElectricAndDieselMultipleUnit)),
                "8" => Ok(Some(TrainPower::ElectricAndDieselMultipleUnit)),
                _ => Ok(Some(TrainPower::DieselHydraulicMultipleUnit)),
            },
        },
        "E" => Ok(Some(TrainPower::ElectricLocomotive)),
        "ED" => Ok(Some(TrainPower::ElectricAndDieselLocomotive)),
        "EML" => Ok(Some(TrainPower::ElectricMultipleUnitWithLocomotive)),
        "EMU" => Ok(Some(TrainPower::ElectricMultipleUnit)),
        "HST" => Ok(Some(TrainPower::DieselElectricMultipleUnit)),
        "" => match timing_load.trim() {
            "DMU" => Ok(Some(TrainPower::DieselHydraulicMultipleUnit)), // NI
            _ => Ok(None),
        },
        x => Err(error_logic(CifErrorType::InvalidTrainPower(x.to_string()))),
    }
}

fn read_speed<F, T>(slice: &str, error_logic: F) -> Result<Option<f64>, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    let speed_mph = match slice {
        "   " => None,
        x => match x.trim().parse::<u16>() {
            Ok(speed) => Some(speed),
            Err(_) => return Err(error_logic(CifErrorType::InvalidSpeed(slice.to_string()))),
        },
    };

    match speed_mph {
        Some(x) => Ok(Some(f64::from(x) * (1609.344 / (60. * 60.)))),
        None => Ok(None),
    }
}

fn read_operating_characteristics<F, T>(
    slice: &str,
    error_logic: F,
) -> Result<(variable_train::OperatingCharacteristics, bool), T>
where
    F: FnOnce(CifErrorType) -> T,
{
    let mut operating_characteristics = variable_train::OperatingCharacteristics {
        vacuum_braked: false,
        one_hundred_mph: false,
        driver_only_passenger: false,
        br_mark_four_coaches: false,
        guard_required: false,
        one_hundred_and_ten_mph: false,
        push_pull: false,
        air_conditioned_with_pa: false,
        steam_heat: false,
        runs_to_locations_as_required: false,
        sb1c_gauge: false,
    };
    let mut runs_as_required = false;

    for chr in slice.chars() {
        match chr {
            'B' => operating_characteristics.vacuum_braked = true,
            'C' => operating_characteristics.one_hundred_mph = true,
            'D' => operating_characteristics.driver_only_passenger = true,
            'E' => operating_characteristics.br_mark_four_coaches = true,
            'G' => operating_characteristics.guard_required = true,
            'M' => operating_characteristics.one_hundred_and_ten_mph = true,
            'P' => operating_characteristics.push_pull = true,
            'Q' => runs_as_required = true,
            'R' => operating_characteristics.air_conditioned_with_pa = true,
            'S' => operating_characteristics.steam_heat = true,
            'Y' => operating_characteristics.runs_to_locations_as_required = true,
            'Z' => operating_characteristics.sb1c_gauge = true,
            ' ' => (),
            x => {
                return Err(error_logic(CifErrorType::InvalidOperatingCharacteristic(
                    x.to_string(),
                )))
            }
        }
    }

    Ok((operating_characteristics, runs_as_required))
}

fn read_timing_load<F, T>(
    power_type: &str,
    timing_load: &str,
    br_mark_four_coaches: bool,
    error_logic: F,
) -> Result<Option<String>, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    Ok(match power_type.trim() {
        "D" => match timing_load.trim() {
            "" => None,
            "DDK" => Some("Diesel locomotive hauling De Dietrich stock".to_string()), // NI
            x => {
                if br_mark_four_coaches {
                    Some(format!(
                        "Diesel locomotive hauling {} tons of BR Mark 4 Coaches",
                        x
                    ))
                } else {
                    Some(format!("Diesel locomotive hauling {} tons", x))
                }
            }
        },
        "DEM" | "DMU" => match timing_load.trim() {
            "69" => Some("Class 172/0, 172/1, or 172/2 'Turbostar' DMU".to_string()),
            "A" => Some("Class 14x 2-axle 'Pacer' DMU".to_string()),
            "E" => Some("Class 158, 168, 170, 172, or 175 'Express' DMU".to_string()),
            "N" => Some("Class 165/0 'Network Turbo' DMU".to_string()),
            "S" => Some("Class 150, 153, 155, or 156 'Sprinter' DMU".to_string()),
            "T" => Some("Class 165/1 or 166 'Network Turbo' DMU".to_string()),
            "V" => Some("Class 220 or 221 'Voyager' DMU".to_string()),
            "X" => Some("Class 159 'South Western Turbo' DMU".to_string()),
            "D1" => Some("Vacuum-braked DMU with power car and trailer".to_string()),
            "D2" => Some("Vacuum-braked DMU with two power cars and trailer".to_string()),
            "D3" => Some("Vacuum-braked DMU with two power cars".to_string()),
            "195" => Some("Class 195 'Civity' DMU".to_string()),
            "196" => Some("Class 196 'Civity' DMU".to_string()),
            "197" => Some("Class 197 'Civity' DMU".to_string()),
            "231" => Some("Class 231 'FLIRT' DEMU".to_string()),
            "755" => Some("Class 755 'FLIRT' bi-mode running on diesel".to_string()),
            "777" => Some("Class 777/1 'METRO' bi-mode running on battery".to_string()),
            "800" => Some("Class 800 'Azuma' bi-mode running on diesel".to_string()),
            "802" => {
                Some("Class 800/802 'IET/Nova 1/Paragon' bi-mode running on diesel".to_string())
            }
            "805" => Some("Class 805/807 'Evero' bi-mode running on diesel".to_string()),
            "810" => Some("Class 810 'Aurora' bi-mode running on diesel".to_string()),
            "1400" => Some("Diesel locomotive hauling 1400 tons".to_string()), // lol
            "CAF" => Some("Class 3000/4000 'C3K/C4K' CAF DMU".to_string()),    // NI
            "DMU" => Some("Class 3000/4000 'C3K/C4K' CAF DMU".to_string()),    // NI
            "" => None,
            x => return Err(error_logic(CifErrorType::InvalidTimingLoad(x.to_string()))),
        },
        "E" => match timing_load.trim() {
            "325" => Some("Class 325 Parcels EMU".to_string()),
            "92" => Some("Class 92 locomotive".to_string()),
            "" => None,
            x => {
                if br_mark_four_coaches {
                    Some(format!(
                        "Electric locomotive hauling {} tons of BR Mark 4 Coaches",
                        x
                    ))
                } else {
                    Some(format!("Electric locomotive hauling {} tons", x))
                }
            }
        },
        "ED" => match timing_load.trim() {
            "" => None,
            x => {
                if br_mark_four_coaches {
                    Some(format!(
                        "Electric and diesel locomotive hauling {} tons of BR Mark 4 Coaches",
                        x
                    ))
                } else {
                    Some(format!("Electric and diesel locomotive hauling {} tons", x))
                }
            }
        },
        "EML" | "EMU" => match timing_load.trim() {
            "AT" => Some("EMU with accelerated timings".to_string()),
            "E" => Some("Class 458 EMU".to_string()),
            "0" => Some("Class 380 EMU".to_string()),
            "506" => Some("Class 350/1 EMU".to_string()),
            "" => None,
            x => Some(format!("Class {} EMU", x)),
        },
        "HST" => Some("High Speed Train (IC125)".to_string()),
        "" => None,
        x => return Err(error_logic(CifErrorType::InvalidTrainPower(x.to_string()))),
    })
}

fn classes_to_bools(class: Class) -> (bool, bool) {
    let first = match class {
        Class::Both => true,
        Class::First => true,
        Class::Standard => false,
        Class::None => false,
    };
    let standard = match class {
        Class::Both => true,
        Class::First => false,
        Class::Standard => true,
        Class::None => false,
    };

    (first, standard)
}

fn read_seating_class<F, T>(
    slice: &str,
    train_type: TrainType,
    error_logic: F,
) -> Result<(bool, bool), T>
where
    F: FnOnce(CifErrorType) -> T,
{
    let seating_class = match slice.trim() {
        "" => match train_type {
            TrainType::Bus
            | TrainType::ServiceBus
            | TrainType::ReplacementBus
            | TrainType::OrdinaryPassenger
            | TrainType::ExpressPassenger
            | TrainType::InternationalPassenger
            | TrainType::SleeperPassenger
            | TrainType::InternationalSleeperPassenger
            | TrainType::CarCarryingPassenger
            | TrainType::UnadvertisedPassenger
            | TrainType::UnadvertisedExpressPassenger
            | TrainType::Staff
            | TrainType::EmptyPassengerAndStaff
            | TrainType::Mixed
            | TrainType::Metro
            | TrainType::PassengerParcels
            | TrainType::Ship => Class::Both,
            _ => Class::None,
        },
        "B" => Class::Both,
        "F" => Class::First,
        "S" => Class::Standard,
        x => return Err(error_logic(CifErrorType::InvalidClass(x.to_string()))),
    };

    Ok(classes_to_bools(seating_class))
}

fn read_sleeper_class<F, T>(slice: &str, error_logic: F) -> Result<(bool, bool), T>
where
    F: FnOnce(CifErrorType) -> T,
{
    let seating_class = match slice.trim() {
        "" => Class::None,
        "B" => Class::Both,
        "F" => Class::First,
        "S" => Class::Standard,
        x => return Err(error_logic(CifErrorType::InvalidClass(x.to_string()))),
    };

    Ok(classes_to_bools(seating_class))
}

fn read_catering<F, T>(slice: &str, error_logic: F) -> Result<(variable_train::Catering, bool), T>
where
    F: FnOnce(CifErrorType) -> T,
{
    let mut catering = variable_train::Catering {
        at_seat_meal: false,
        bar: false,
        bistro: false,
        breakfast_in_car: false,
        buffet: false,
        coffee_shop: false,
        self_service: false,
        trolley: false,
        vending_machine_food: false,
        vending_machine_drink: false,
        mini_bar: false,
        restaurant: false,
        first_class_restaurant: false,
        first_class_meal: false,
        other: false,
        food_available: None,
        hot_food_available: None,
        drink_available: None,
        snacks_available: None,
    };
    let mut wheelchair_reservations = false;

    for chr in slice.chars() {
        match chr {
            'C' => catering.buffet = true,
            'F' => catering.first_class_restaurant = true,
            'H' => catering.hot_food_available = Some(true),
            'M' => catering.first_class_meal = true,
            'P' => wheelchair_reservations = true,
            'R' => catering.restaurant = true,
            'T' => catering.trolley = true,
            ' ' => (),
            x => return Err(error_logic(CifErrorType::InvalidCatering(x.to_string()))),
        }
    }

    Ok((catering, wheelchair_reservations))
}

fn read_reservations<F, T>(
    slice: &str,
    wheelchair_reservations: bool,
    first_seating: bool,
    standard_seating: bool,
    first_sleepers: bool,
    standard_sleepers: bool,
    train_type: TrainType,
    error_logic: F,
) -> Result<variable_train::Reservations, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    match slice.trim() {
        "A" => Ok(variable_train::Reservations {
            seats: if first_seating || standard_seating {
                ReservationField::Mandatory
            } else {
                ReservationField::NotApplicable
            },
            groups: ReservationField::Unknown,
            first_class: ReservationField::NotApplicable,
            second_class: ReservationField::NotApplicable,
            not_every_class: ReservationField::NotApplicable,
            bicycles: ReservationField::Mandatory,
            sleepers: if first_sleepers || standard_sleepers {
                ReservationField::Mandatory
            } else {
                ReservationField::NotApplicable
            },
            vehicles: if train_type == TrainType::CarCarryingPassenger {
                ReservationField::Mandatory
            } else {
                ReservationField::NotApplicable
            },
            wheelchairs: ReservationField::Mandatory,
            supplement_charged: None,
        }),
        "E" => Ok(variable_train::Reservations {
            seats: if first_seating || standard_seating {
                ReservationField::NotMandatory
            } else {
                ReservationField::NotApplicable
            },
            groups: ReservationField::Unknown,
            first_class: ReservationField::NotApplicable,
            second_class: ReservationField::NotApplicable,
            not_every_class: ReservationField::NotApplicable,
            bicycles: ReservationField::Mandatory,
            sleepers: if first_sleepers || standard_sleepers {
                ReservationField::NotMandatory
            } else {
                ReservationField::NotApplicable
            },
            vehicles: if train_type == TrainType::CarCarryingPassenger {
                ReservationField::Mandatory
            } else {
                ReservationField::NotApplicable
            },
            wheelchairs: if wheelchair_reservations {
                ReservationField::Possible
            } else {
                ReservationField::NotMandatory
            },
            supplement_charged: None,
        }),
        "R" => Ok(variable_train::Reservations {
            seats: if first_seating || standard_seating {
                ReservationField::Recommended
            } else {
                ReservationField::NotApplicable
            },
            groups: ReservationField::Unknown,
            first_class: ReservationField::NotApplicable,
            second_class: ReservationField::NotApplicable,
            not_every_class: ReservationField::NotApplicable,
            bicycles: ReservationField::NotMandatory,
            sleepers: if first_sleepers || standard_sleepers {
                ReservationField::Recommended
            } else {
                ReservationField::NotApplicable
            },
            vehicles: if train_type == TrainType::CarCarryingPassenger {
                ReservationField::Mandatory
            } else {
                ReservationField::NotApplicable
            },
            wheelchairs: ReservationField::Recommended,
            supplement_charged: None,
        }),
        "S" => Ok(variable_train::Reservations {
            seats: if first_seating || standard_seating {
                ReservationField::Possible
            } else {
                ReservationField::NotApplicable
            },
            groups: ReservationField::Unknown,
            first_class: ReservationField::NotApplicable,
            second_class: ReservationField::NotApplicable,
            not_every_class: ReservationField::NotApplicable,
            bicycles: ReservationField::NotMandatory,
            sleepers: if first_sleepers || standard_sleepers {
                ReservationField::Possible
            } else {
                ReservationField::NotApplicable
            },
            vehicles: if train_type == TrainType::CarCarryingPassenger {
                ReservationField::Mandatory
            } else {
                ReservationField::NotApplicable
            },
            wheelchairs: ReservationField::Possible,
            supplement_charged: None,
        }),
        "" => Ok(variable_train::Reservations {
            seats: if first_seating || standard_seating {
                ReservationField::Impossible
            } else {
                ReservationField::NotApplicable
            },
            groups: ReservationField::Unknown,
            first_class: ReservationField::NotApplicable,
            second_class: ReservationField::NotApplicable,
            not_every_class: ReservationField::NotApplicable,
            bicycles: ReservationField::NotMandatory,
            sleepers: if first_sleepers || standard_sleepers {
                ReservationField::Impossible
            } else {
                ReservationField::NotApplicable
            },
            vehicles: if train_type == TrainType::CarCarryingPassenger {
                ReservationField::Mandatory
            } else {
                ReservationField::NotApplicable
            },
            wheelchairs: if wheelchair_reservations {
                ReservationField::Possible
            } else {
                if first_seating || standard_seating || first_sleepers || standard_sleepers {
                    ReservationField::Impossible
                } else {
                    ReservationField::NotApplicable
                }
            },
            supplement_charged: None,
        }),
        x => Err(error_logic(CifErrorType::InvalidReservationType(
            x.to_string(),
        ))),
    }
}

fn read_brand<F, T>(slice: &str, error_logic: F) -> Result<Option<String>, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    let mut brand = None;
    // contrary to the spec, NIR uses two letters to mean "Enterprise"
    if slice.trim() == "EP" {
        return Ok(Some("Enterprise".to_string()));
    }
    for chr in slice.chars() {
        match chr {
            'E' => brand = Some("Eurostar".to_string()),
            'U' => brand = Some("Alphaline".to_string()),
            ' ' => (),
            x => return Err(error_logic(CifErrorType::InvalidBrand(x.to_string()))),
        }
    }

    Ok(brand)
}

fn amend_train(
    train_variant: &mut train_variant::ActiveModelEx,
    new_train_variant: train_variant::ActiveModelEx
) {
    train_variant.validity = new_train_variant.validity;
    train_variant.runs_as_required = new_train_variant.runs_as_required;
    train_variant.performance_monitoring = new_train_variant.performance_monitoring;
    train_variant.route = new_train_variant.route;
    train_variant.variable_train = new_train_variant.variable_train;
    train_variant.source = new_train_variant.source;
}

fn read_mandatory_wtt_time<F, T>(slice: &str, error_logic: F) -> Result<NaiveTime, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    let wtt = NaiveTime::parse_from_str(&slice[0..4], "%H%M");
    let wtt = match wtt {
        Ok(x) => x,
        Err(x) => return Err(error_logic(CifErrorType::ChronoParseError(x))),
    };
    Ok(wtt
        + match &slice[4..5] {
            "H" => Duration::seconds(30),
            " " => Duration::seconds(0),
            x => {
                return Err(error_logic(CifErrorType::InvalidMinuteFraction(
                    x.to_string(),
                )))
            }
        })
}

fn read_optional_wtt_time<F, T>(slice: &str, error_logic: F) -> Result<Option<NaiveTime>, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    Ok(match slice {
        "     " => None,
        x => Some(read_mandatory_wtt_time(x, error_logic)?),
    })
}

fn read_vstp_time<F, T>(slice: &Option<String>, error_logic: F) -> Result<Option<NaiveTime>, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    Ok(match slice {
        Some(x) => match x.trim() {
            "" => None,
            x => Some(match NaiveTime::parse_from_str(x, "%H%M%S") {
                Ok(x) => x,
                Err(x) => return Err(error_logic(CifErrorType::ChronoParseError(x))),
            }),
        },
        None => None,
    })
}

fn read_public_time<F, T>(slice: &str, error_logic: F) -> Result<Option<NaiveTime>, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    // contrary to the spec NI Railways use blanks not zeroes
    if slice == "    " {
        return Ok(None);
    }
    let pub_dep = NaiveTime::parse_from_str(slice, "%H%M");
    let pub_dep = match pub_dep {
        Ok(x) => x,
        Err(x) => return Err(error_logic(CifErrorType::ChronoParseError(x))),
    };
    // amazingly, public departure times of midnight are impossible in Britain!
    Ok(if pub_dep == NaiveTime::from_hms_opt(0, 0, 0).unwrap() {
        None
    } else {
        Some(pub_dep)
    })
}

fn read_allowance<F, T>(slice: &str, error_logic: F) -> Result<u32, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    let (eng_minutes, eng_seconds) = match (&slice[0..1], &slice[1..2], &slice[0..2]) {
        (_, _, "  ") => (Ok(0), 0),
        (_, _, " H") => (Ok(0), 30),
        (x, " ", _) => (x.parse::<u32>(), 0),
        (x, "H", _) => (x.parse::<u32>(), 30),
        (_, _, x) => (x.parse::<u32>(), 0),
    };
    let eng_minutes = match eng_minutes {
        Ok(x) => x,
        Err(_) => {
            return Err(error_logic(CifErrorType::InvalidAllowance(
                slice.to_string(),
            )))
        }
    };
    Ok(eng_minutes * 60 + eng_seconds)
}

fn read_activities<F, T>(slice: &str, error_logic: F) -> Result<train_location::Activities, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    let mut activities = train_location::Activities {
        detach: false,
        attach: false,
        other_trains_pass: false,
        attach_or_detach_assisting_loco: false,
        x_on_arrival: false,
        banking_loco: false,
        crew_change: false,
        set_down_only: false,
        examination: false,
        gbprtt: false,
        prevent_column_merge: false,
        prevent_third_column_merge: false,
        passenger_count: false,
        ticket_collection: false,
        ticket_examination: false,
        first_class_ticket_examination: false,
        selective_ticket_examination: false,
        change_loco: false,
        unadvertised_stop: false,
        operational_stop: false,
        train_locomotive_on_rear: false,
        propelling: false,
        request_pick_up: false,
        request_set_down: false,
        reversing_move: false,
        run_round: false,
        staff_stop: false,
        normal_passenger_stop: false,
        train_begins: false,
        train_finishes: false,
        tops_reporting: false,
        token_etc: false,
        pick_up_only: false,
        watering_stock: false,
        cross_at_passing_point: false,
        request_pick_up_by_telephone: false,
        request_set_down_by_telephone: false,
        times_approximate: false,
    };

    for activity in slice
        .chars()
        .chunks(2)
        .into_iter()
        .map(|chunk| chunk.collect::<String>())
    {
        match activity.trim() {
            "A" => activities.other_trains_pass = true,
            "AE" => activities.attach_or_detach_assisting_loco = true,
            "AX" => activities.x_on_arrival = true,
            "BL" => activities.banking_loco = true,
            "C" => activities.crew_change = true,
            "D" => activities.set_down_only = true,
            "-D" => activities.detach = true,
            "E" => activities.examination = true,
            "G" => activities.gbprtt = true,
            "H" => activities.prevent_column_merge = true,
            "HH" => activities.prevent_third_column_merge = true,
            "K" => activities.passenger_count = true,
            "KC" => activities.ticket_collection = true,
            "KE" => activities.ticket_examination = true,
            "KF" => activities.first_class_ticket_examination = true,
            "KS" => activities.selective_ticket_examination = true,
            "L" => activities.change_loco = true,
            "N" => activities.unadvertised_stop = true,
            "O" => activities.operational_stop = true, // Typo in NIR CIF data
            "OP" => activities.operational_stop = true,
            "OR" => activities.train_locomotive_on_rear = true,
            "PN" => activities.unadvertised_stop = true, // Typo in NIR CIF data
            "PR" => activities.propelling = true,
            "R" => {
                activities.request_pick_up = true;
                activities.request_set_down = true;
            }
            "RM" => activities.reversing_move = true,
            "RR" => activities.run_round = true,
            "S" => activities.staff_stop = true,
            "T" => activities.normal_passenger_stop = true,
            "-T" => (activities.detach, activities.attach) = (true, true),
            "TB" => activities.train_begins = true,
            "TF" => activities.train_finishes = true,
            "TS" => activities.tops_reporting = true,
            "TW" => activities.token_etc = true,
            "U" => activities.pick_up_only = true,
            "-U" => activities.attach = true,
            "W" => activities.watering_stock = true,
            "X" => activities.cross_at_passing_point = true,
            // found in VSTP, this is its meaning in paper WTTs
            "*" => activities.other_trains_pass = true,
            "" => (),
            x => return Err(error_logic(CifErrorType::InvalidActivity(x.to_string()))),
        };
    }

    Ok(activities)
}

fn read_train_status<F, T>(slice: &str, error_logic: F) -> Result<TrainStatus, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    Ok(match slice.trim() {
        "B" => TrainStatus::Bus,
        "F" => TrainStatus::Freight,
        "D" => TrainStatus::Freight, // Appears in a seeming typo in NIR CIF
        "P" => TrainStatus::PassengerParcels,
        "E" => TrainStatus::PassengerParcels, // Appears in a seeming typo in NIR CIF
        "S" => TrainStatus::Ship,
        "T" => TrainStatus::Trip,
        "1" => TrainStatus::StpPassengerParcels,
        "2" => TrainStatus::StpFreight,
        "3" => TrainStatus::StpTrip,
        "4" => TrainStatus::StpShip,
        "5" => TrainStatus::StpBus,
        "" => TrainStatus::VstpNone, // found in VSTP
        x => return Err(error_logic(CifErrorType::InvalidTrainStatus(x.to_string()))),
    })
}

fn read_train_operator<F, T>(slice: &str, error_logic: F) -> Result<Option<String>, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    Ok(match slice {
        "EU" => Some("Virtual European Path".to_string()),
        "AR" => Some("Alliance Rail".to_string()),
        "NT" => Some("Northern".to_string()),
        "AW" => Some("Transport for Wales".to_string()),
        "CC" => Some("c2c".to_string()),
        "CS" => Some("Caledonian Sleeper".to_string()),
        "CH" => Some("Chiltern Railways".to_string()),
        "XC" => Some("CrossCountry".to_string()),
        "EM" => Some("East Midlands Railway".to_string()),
        "ES" => Some("Eurostar".to_string()),
        "FC" => Some("First Capital Connect".to_string()),
        "HT" => Some("Hull Trains".to_string()),
        "GX" => Some("Gatwick Express".to_string()),
        "GN" => Some("Great Northern".to_string()),
        "TL" => Some("Thameslink".to_string()),
        "GC" => Some("Grand Central".to_string()),
        "GW" => Some("Great Western Railway".to_string()),
        "LE" => Some("Greater Anglia".to_string()),
        "HC" => Some("Heathrow Connect".to_string()),
        "HX" => Some("Heathrow Express".to_string()),
        "IL" => Some("Island Line".to_string()),
        "LS" => Some("Locomotive Services".to_string()),
        "LM" => Some("West Midlands Trains".to_string()),
        "LO" => Some("London Overground".to_string()),
        "LT" => Some("London Underground".to_string()),
        "ME" => Some("Merseyrail".to_string()),
        "LR" => Some("Network Rail".to_string()),
        "TW" => Some("Tyne & Wear Metro".to_string()),
        "NY" => Some("North Yorkshire Moors Railway".to_string()),
        "SR" => Some("ScotRail".to_string()),
        "SW" => Some("South Western Railway".to_string()),
        "SJ" => Some("South Yorkshire Supertram".to_string()),
        "SE" => Some("Southeastern".to_string()),
        "SN" => Some("Southern".to_string()),
        "SP" => Some("Swanage Railway".to_string()),
        "XR" => Some("Elizabeth line".to_string()),
        "TP" => Some("TransPennine Express".to_string()),
        "VT" => Some("Avanti West Coast".to_string()),
        "GR" => Some("LNER".to_string()),
        "WR" => Some("West Coast Railway Company".to_string()),
        "WS" => Some("Wrexham and Shropshire".to_string()),
        "TY" => Some("Vintage Trains".to_string()),
        "LD" => Some("Lumo".to_string()),
        "SO" => Some("SLC Operations".to_string()),
        "LF" => Some("Grand Union Trains".to_string()),
        "MV" => Some("Varamis Rail".to_string()),
        "PT" => Some("Europorte 2".to_string()),
        "YG" => Some("Hanson & Hall".to_string()),
        "FS" => Some("Fishbone Solutions".to_string()),
        "PX" => Some("Europhoenix".to_string()),
        "NI" => Some("Translink NI Railways".to_string()),
        "ZZ" => None,
        "#|" => None,
        x => {
            return Err(error_logic(CifErrorType::InvalidTrainOperator(
                x.to_string(),
            )))
        }
    })
}

fn read_ats_code<F, T>(slice: &str, error_logic: F) -> Result<bool, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    match slice {
        "Y" => Ok(true),
        "N" => Ok(false),
        x => Err(error_logic(CifErrorType::InvalidAtsCode(x.to_string()))),
    }
}

fn get_working_time(location: &train_location::ActiveModelEx) -> (NaiveTime, u8) {
    // no error checking needed as any issue here should be a panic; trains are
    // checked for validity as they are written
    match location.working_dep.clone().unwrap() {
        Some(x) => (x, location.working_dep_day.clone().unwrap().unwrap()),
        None => (
            location.working_pass.clone().unwrap().unwrap(),
            location.working_pass_day.clone().unwrap().unwrap(),
        ),
    }
}

fn calculate_day(
    time: &Option<NaiveTime>,
    last_wtt_time: &NaiveTime,
    last_wtt_day: u8,
) -> Option<u8> {
    match time {
        Some(x) => {
            if x < last_wtt_time {
                Some(last_wtt_day + 1)
            } else {
                Some(last_wtt_day)
            }
        }
        None => None,
    }
}

impl CifImporter {
    pub fn new(config: CifImporterConfig) -> CifImporter {
        CifImporter {
            config,
            ..Default::default()
        }
    }

    async fn populate_train_variant_cache(
        &mut self, namespace: &str, transaction: &DatabaseTransaction
    ) -> Result<(), Error> {
        self.cached_train_variant_ids.clear();
        let mut trains = train::Entity::load()
            .filter(train::COLUMN.namespace.eq(namespace))
            .all(transaction)
            .await?;

        let ids: Vec<Vec<Option<String>>>
            = trains
            .iter()
            .map(|x| Some(x.id.clone()))
            .collect::<Vec<Option<String>>>()
            .chunks(10000)
            .map(|x| x.to_vec())
            .collect();

        {
            let mut train_map: HashMap<String, &mut train::ModelEx>
                = trains.iter_mut().map(|x| (x.id.clone(), x)).collect();
            for chunk in &ids {
                let chunk_variants = train_variant::Entity::load()
                    .filter(train_variant::COLUMN.train_id.is_in(chunk.to_vec()))
                    .all(transaction)
                    .await?;

                for train_variant in chunk_variants.into_iter() {
                    let train: &mut train::ModelEx
                        = *train_map.get_mut(train_variant.train_id.as_ref().unwrap()).unwrap();
                    let variants: Option<Vec<train_variant::ModelEx>>
                        = train.train_variants.clone().into();
                    let mut variants = variants.unwrap_or_default();
                    variants.push(train_variant);
                    train.train_variants = HasMany::Loaded(variants);
                }
            }
        }

        self.cached_train_variant_ids = trains
            .iter()
            .fold::<HashMap<String, HashSet<i64>>, _>(HashMap::new(), |mut map, item| {
                map.entry(item.id.clone())
                    .or_default()
                    .extend(item.train_variants.iter().map(|x| x.id));
                map
            });

        let root_train_variants: Vec<train_variant::ModelEx>
            = trains.into_iter().map(|x| x.train_variants).flatten().collect();

        let mut train_id_by_train_variant_id: HashMap<i64, String> = root_train_variants
            .iter()
            .map(|x| (x.id, x.train_id.clone().unwrap()))
            .collect();

        let mut prev_train_variants = root_train_variants;
        loop {
            let ids: Vec<Vec<Option<i64>>>
                = prev_train_variants
                .iter()
                .map(|x| Some(x.id))
                .collect::<Vec<Option<i64>>>()
                .chunks(10000)
                .map(|x| x.to_vec())
                .collect();
            prev_train_variants = vec![];
            for chunk in &ids {
                prev_train_variants.append(&mut train_variant::Entity::load()
                    .filter(train_variant::COLUMN.parent_train_variant_id.is_in(chunk.to_vec()))
                    .all(transaction)
                    .await?);
            }
            if prev_train_variants.len() > 0 {
                for train_variant in &prev_train_variants {
                    let train_id
                        = train_id_by_train_variant_id
                        [&train_variant.parent_train_variant_id.unwrap()].clone();
                    train_id_by_train_variant_id.insert(train_variant.id, train_id.clone());
                    self.cached_train_variant_ids
                        .entry(train_id.clone()).or_default().insert(train_variant.id);
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    fn get_train_variant_ids(
        &self,
        train_id: &str,
    ) -> HashSet<i64> {
        self.cached_train_variant_ids.get(train_id).unwrap_or(&HashSet::new()).clone()
    }

    fn delete_unwritten_assocs(
        &mut self,
        main_train_id: &str,
        location: &str,
        location_suffix: &Option<String>,
        other_train_id: &str,
        begin: &NaiveDateTime,
        other_train_location_suffix: &Option<String>,
        stp_modification_type: &ModificationType,
        is_stp: bool,
        use_rev: bool,
    ) {
        let old_assoc = self.unwritten_assocs.remove(&(
            main_train_id.to_string(),
            location.to_string(),
            location_suffix.clone(),
        ));
        let mut old_assoc = match old_assoc {
            None => vec![],
            Some(x) => x,
        };

        if *stp_modification_type == ModificationType::Insert {
            old_assoc.retain(|assoc|
                assoc.other_train_id == ActiveValue::Set(other_train_id.to_string())
                && assoc.validity[0].valid_begin
                    == ActiveValue::Set(
                        if use_rev { rev_date(*begin, assoc.day_diff.clone().unwrap()) }
                        else { *begin }
                    )
                && assoc.other_train_location_id_suffix
                    == ActiveValue::Set(other_train_location_suffix.clone())
                && assoc.source
                    == ActiveValue::Set(Some(
                        if is_stp { TrainSource::ShortTerm } else { TrainSource::LongTerm }
                    ))
                && ((
                    !use_rev && matches!(
                        assoc.association_type,
                        ActiveValue::Set(AssociationType::MainDividesToFormOther)
                        | ActiveValue::Set(AssociationType::MainJoinsToOther)
                        | ActiveValue::Set(AssociationType::MainBecomesOther)
                    )
                ) || (
                    use_rev && matches!(
                        assoc.association_type,
                        ActiveValue::Set(AssociationType::MainDividesFromOther)
                        | ActiveValue::Set(AssociationType::MainIsJoinedToByOther)
                        | ActiveValue::Set(AssociationType::MainFormsFromOther)
                    )
                ))
            );
        } else {
            for ref mut assoc in old_assoc.iter_mut() {
                if assoc.other_train_id != ActiveValue::Set(other_train_id.to_string())
                    || assoc.other_train_location_id_suffix != ActiveValue::Set(
                        other_train_location_suffix.clone()
                    )
                    || ((
                        !use_rev && !matches!(
                            assoc.association_type,
                            ActiveValue::Set(AssociationType::MainDividesToFormOther)
                            | ActiveValue::Set(AssociationType::MainJoinsToOther)
                            | ActiveValue::Set(AssociationType::MainBecomesOther)
                        )
                    ) || (
                        use_rev && !matches!(
                            assoc.association_type,
                            ActiveValue::Set(AssociationType::MainDividesFromOther)
                            | ActiveValue::Set(AssociationType::MainIsJoinedToByOther)
                            | ActiveValue::Set(AssociationType::MainFormsFromOther)
                        )
                    ))
                {
                    continue;
                }
                if *stp_modification_type == ModificationType::Amend {
                    assoc.replacements.as_mut_vec().retain(|assoc|
                        assoc.validity.as_slice()[0].valid_begin
                            != if use_rev {
                                ActiveValue::Set(rev_date(*begin, assoc.day_diff.clone().unwrap()))
                            } else {
                                ActiveValue::Set(*begin)
                            }
                    );
                } else if *stp_modification_type == ModificationType::Delete {
                    assoc.cancellations.as_mut_vec().retain(|cancellation| {
                        cancellation.validity.as_slice()[0].valid_begin
                            != if use_rev {
                                ActiveValue::Set(rev_date(*begin, assoc.day_diff.clone().unwrap()))
                            } else {
                                ActiveValue::Set(*begin)
                            }
                    });
                }
            }
        }

        self.unwritten_assocs.insert(
            (
                main_train_id.to_string(),
                location.to_string(),
                location_suffix.clone(),
            ),
            old_assoc,
        );
    }

    fn cancel_unwritten_assocs(
        &mut self,
        main_train_id: &str,
        location: &str,
        location_suffix: &Option<String>,
        other_train_id: &str,
        begin: &NaiveDateTime,
        end: &NaiveDateTime,
        days_of_week: &train_validity_period::DaysOfWeek,
        other_train_location_suffix: &Option<String>,
        use_rev: bool,
    ) {
        let old_assoc = self.unwritten_assocs.remove(&(
            main_train_id.to_string(),
            location.to_string(),
            location_suffix.clone(),
        ));
        let mut old_assoc = match old_assoc {
            None => vec![],
            Some(x) => x,
        };

        for ref mut assoc in old_assoc.iter_mut() {
            if assoc.other_train_id != ActiveValue::Set(other_train_id.to_string())
                || ((
                    !use_rev && !matches!(
                        assoc.association_type,
                        ActiveValue::Set(AssociationType::MainDividesToFormOther)
                        | ActiveValue::Set(AssociationType::MainJoinsToOther)
                        | ActiveValue::Set(AssociationType::MainBecomesOther)
                    )
                ) || (
                    use_rev && !matches!(
                        assoc.association_type,
                        ActiveValue::Set(AssociationType::MainDividesFromOther)
                        | ActiveValue::Set(AssociationType::MainIsJoinedToByOther)
                        | ActiveValue::Set(AssociationType::MainFormsFromOther)
                    )
                )) || assoc.other_train_location_id_suffix
                    != ActiveValue::Set(other_train_location_suffix.clone()) {
                continue;
            };
            let (begin, end, days_of_week) = if use_rev {
                (
                    rev_date(*begin, assoc.day_diff.clone().unwrap()),
                    rev_date(*end, assoc.day_diff.clone().unwrap()),
                    rev_days(&days_of_week, assoc.day_diff.clone().unwrap()),
                )
            } else {
                (begin.clone(), end.clone(), days_of_week.clone())
            };
            if !check_date_applicability(&assoc.validity[0], begin, end, &days_of_week) {
                continue;
            }
            let mut validity = train_validity_period::ActiveModelEx {
                valid_begin: ActiveValue::Set(begin),
                valid_end: ActiveValue::Set(end),
                timezone: ActiveValue::Set(London.name().to_string()),
                ..Default::default()
            };
            validity.populate_days_of_week(&days_of_week);
            let new_cancel = association_cancellation::ActiveModelEx {
                validity: ActiveHasMany::Append(vec![validity]),
                source: ActiveValue::Set(Some(TrainSource::ShortTerm)),
                ..Default::default()
            };
            assoc
                .cancellations
                .as_mut_vec()
                .push(new_cancel);
        }

        self.unwritten_assocs.insert(
            (
                main_train_id.to_string(),
                location.to_string(),
                location_suffix.clone(),
            ),
            old_assoc,
        );
    }

    fn amend_unwritten_assocs(
        &mut self,
        main_train_id: &str,
        location: &str,
        location_suffix: &Option<String>,
        other_train_id: &str,
        begin: &NaiveDateTime,
        end: &NaiveDateTime,
        days_of_week: &train_validity_period::DaysOfWeek,
        other_train_location_suffix: &Option<String>,
        stp_modification_type: &ModificationType,
        is_stp: bool,
        day_diff: Option<i8>,
        for_passengers: Option<bool>,
        category: Option<AssociationType>,
        use_rev: bool,
    ) {
        let old_assoc = self.unwritten_assocs.remove(&(
            main_train_id.to_string(),
            location.to_string(),
            location_suffix.clone(),
        ));
        let mut old_assoc = match old_assoc {
            None => vec![],
            Some(x) => x,
        };

        for ref mut assoc in old_assoc.iter_mut() {
            if ActiveValue::Set(other_train_id.to_string()) != assoc.other_train_id
                || ActiveValue::Set(other_train_location_suffix.clone())
                    != assoc.other_train_location_id_suffix {
                continue;
            }
            if *stp_modification_type == ModificationType::Insert
                && assoc.source
                    == ActiveValue::Set(Some(
                        if is_stp { TrainSource::ShortTerm } else { TrainSource::LongTerm }
                    ))
                && assoc.validity[0].valid_begin == ActiveValue::Set(
                    if use_rev { rev_date(*begin, assoc.day_diff.clone().unwrap()) } else { *begin }
                )
                && assoc.main_train_location_id.clone().unwrap().is_some()
            {
                amend_assoc(
                    assoc,
                    begin,
                    end,
                    days_of_week,
                    day_diff,
                    for_passengers,
                    category,
                    use_rev,
                );
            }
            else if *stp_modification_type == ModificationType::Amend
                && assoc.source
                    == ActiveValue::Set(Some(
                        if is_stp { TrainSource::ShortTerm } else { TrainSource::LongTerm }
                    ))
                && assoc.validity[0].valid_begin == ActiveValue::Set(
                    if use_rev { rev_date(*begin, assoc.day_diff.clone().unwrap()) } else { *begin }
                )
                && assoc.main_train_location_id.clone().unwrap().is_none()
            {
                amend_assoc(
                    assoc,
                    begin,
                    end,
                    days_of_week,
                    day_diff,
                    for_passengers,
                    category,
                    use_rev,
                );
            }
            else if *stp_modification_type == ModificationType::Delete
                && assoc.source
                    == ActiveValue::Set(Some(
                        if is_stp { TrainSource::ShortTerm } else { TrainSource::LongTerm }
                    ))
            {
                for ref mut cancellation in assoc.cancellations.as_mut_vec().iter_mut() {
                    if cancellation.validity[0].valid_begin
                        == ActiveValue::Set(
                            if use_rev { rev_date(*begin, assoc.day_diff.clone().unwrap()) }
                            else { *begin }
                        ) {
                        let mut validity = train_validity_period::ActiveModelEx {
                            valid_begin: ActiveValue::Set(
                                if use_rev { rev_date(*begin, assoc.day_diff.clone().unwrap()) }
                                else { *begin }
                            ),
                            valid_end: ActiveValue::Set(
                                if use_rev { rev_date(*end, assoc.day_diff.clone().unwrap()) }
                                else { *end }
                            ),
                            timezone: ActiveValue::Set(London.name().to_string()),
                            ..Default::default()
                        };
                        let days_of_week = if use_rev {
                            rev_days(&days_of_week, assoc.day_diff.clone().unwrap())
                        }
                        else { days_of_week.clone() };
                        validity.populate_days_of_week(&days_of_week);
                        cancellation.validity = ActiveHasMany::Append(vec![validity]);
                    }
                }
            }
        }

        self.unwritten_assocs.insert(
            (
                main_train_id.to_string(),
                location.to_string(),
                location_suffix.clone(),
            ),
            old_assoc,
        );
    }

    fn replace_unwritten_assocs(
        &mut self,
        main_train_id: &str,
        location: &str,
        location_suffix: &Option<String>,
        other_train_id: &str,
        other_train_location_suffix: &Option<String>,
        new_assoc: &association_node::ActiveModelEx,
    ) {
        let old_assoc = self.unwritten_assocs.remove(&(
            main_train_id.to_string(),
            location.to_string(),
            location_suffix.clone(),
        ));
        let mut old_assoc = match old_assoc {
            None => vec![],
            Some(x) => x,
        };

        for ref mut assoc in old_assoc.iter_mut() {
            if ActiveValue::Set(other_train_id.to_string()) == assoc.other_train_id
                && ActiveValue::Set(other_train_location_suffix.clone())
                    == assoc.other_train_location_id_suffix
            {
                // check for no overlapping days at all
                if !check_date_applicability(
                    &assoc.validity[0],
                    new_assoc.validity[0].valid_begin.clone().unwrap(),
                    new_assoc.validity[0].valid_end.clone().unwrap(),
                    &train_validity_period::DaysOfWeek::get_from_active_model(
                        &new_assoc.validity[0]
                    )
                ) {
                    continue;
                }
                assoc.replacements.push(new_assoc.clone());
            }
        }

        self.unwritten_assocs.insert(
            (
                main_train_id.to_string(),
                location.to_string(),
                location_suffix.clone(),
            ),
            old_assoc,
        );
    }

    fn get_last_train<'a>(
        &'a mut self,
        number: u64,
        record_type: &str,
    ) -> Result<&'a mut train_variant::ActiveModelEx, Error> {
        Ok(match &mut self.last_train {
            Some(LastTrain::DatabaseInsert(x)) => x,
            Some(LastTrain::DatabaseSave(x)) => x,
            Some(LastTrain::Orphaned(key)) => self.orphaned_overlay_trains.get_mut(key).unwrap(),
            None => {
                return Err(CifError {
                    error_type: CifErrorType::UnexpectedRecordType(
                        record_type.to_string(),
                        "No preceding BS".to_string(),
                    ),
                    line: number,
                    column: 0,
                }.into())
            }
        })
    }

    fn validate_change_en_route_location(
        &self,
        location_id: &str,
        location_suffix: &Option<String>,
        number: u64,
        column: usize,
    ) -> Result<(), CifError> {
        Ok(match self.change_en_route {
            Some(_) => {
                if (location_id.to_string(), location_suffix.clone())
                    != *self.cr_location.as_ref().unwrap()
                {
                    return Err(CifError {
                        error_type: CifErrorType::ChangeEnRouteLocationUnmatched(
                            (location_id.to_string(), location_suffix.clone()),
                            self.cr_location.clone().unwrap(),
                        ),
                        line: number,
                        column: column,
                    });
                }
            }
            None => (),
        })
    }

    async fn read_association(
        &mut self,
        line: &str,
        namespace: &str,
        number: u64,
        transaction: &DatabaseTransaction,
    ) -> Result<(), Error> {
        let modification_type =
            read_modification_type(&line[2..3], produce_cif_error_closure(number, 2))?;
        let (stp_modification_type, is_stp) =
            read_stp_indicator(&line[79..80], produce_cif_error_closure(number, 79))?;

        let main_train_id = &line[3..9];
        let other_train_id = &line[9..15];
        let begin = read_date(&line[15..21], produce_cif_error_closure(number, 15))?;
        let location = &line[37..44].trim();
        let location_suffix = read_optional_string(&line[44..45]);
        let other_train_location_suffix = read_optional_string(&line[45..46]);

        // At this stage we have all the data we need for a simple delete, so handle this here
        //
        // Note these are NOT the same as STP cancels and indeed handled completely differently
        if modification_type == ModificationType::Delete {
            // first find any committed associations and delete
            trains_delete_assoc(
                &self.get_train_variant_ids(main_train_id),
                &other_train_id,
                namespace,
                &begin,
                &location,
                &location_suffix,
                &other_train_location_suffix,
                &stp_modification_type,
                is_stp,
                transaction,
            ).await?;
            trains_delete_rev_assoc(
                &self.get_train_variant_ids(other_train_id),
                &main_train_id,
                namespace,
                &begin,
                &location,
                &other_train_location_suffix,
                &location_suffix,
                &stp_modification_type,
                is_stp,
                transaction,
            ).await?;

            // now delete from unwritten associations
            self.delete_unwritten_assocs(
                main_train_id,
                location,
                &location_suffix,
                other_train_id,
                &begin,
                &other_train_location_suffix,
                &stp_modification_type,
                is_stp,
                false,
            );
            self.delete_unwritten_assocs(
                other_train_id,
                location,
                &other_train_location_suffix,
                main_train_id,
                &begin,
                &location_suffix,
                &stp_modification_type,
                is_stp,
                true,
            );

            return Ok(());
        }

        let end = read_date(&line[21..27], produce_cif_error_closure(number, 21))?;
        let days_of_week = read_days_of_week(&line[27..34], produce_cif_error_closure(number, 27))?;

        // Now we handle STP cancellations; these are where long-running
        // associations are deleted as a one-off
        if stp_modification_type == ModificationType::Delete
            && modification_type == ModificationType::Insert
        {
            // cancel written ones
            trains_cancel_assoc(
                &self.get_train_variant_ids(main_train_id),
                &other_train_id,
                namespace,
                &begin,
                &end,
                &days_of_week,
                &location,
                &location_suffix,
                &other_train_location_suffix,
                transaction,
            ).await?;
            trains_cancel_rev_assoc(
                &self.get_train_variant_ids(other_train_id),
                &main_train_id,
                namespace,
                &begin,
                &end,
                &days_of_week,
                &location,
                &other_train_location_suffix,
                &location_suffix,
                transaction,
            ).await?;

            // now cancel from unwritten associations
            self.cancel_unwritten_assocs(
                main_train_id,
                location,
                &location_suffix,
                other_train_id,
                &begin,
                &end,
                &days_of_week,
                &other_train_location_suffix,
                false,
            );
            self.cancel_unwritten_assocs(
                other_train_id,
                location,
                &other_train_location_suffix,
                main_train_id,
                &begin,
                &end,
                &days_of_week,
                &location_suffix,
                true,
            );

            return Ok(());
        }

        let day_diff = match &line[36..37] {
            "S" => Some(0),
            "N" => Some(1),
            "P" => Some(-1),
            " " => None,
            x => {
                return Err(CifError {
                    error_type: CifErrorType::InvalidAssociationDateIndicator(x.to_string()),
                    line: number,
                    column: 36,
                }.into())
            }
        };
        let for_passengers = match &line[47..48] {
            "P" => Some(true),
            "O" => Some(false),
            " " => None,
            x => {
                return Err(CifError {
                    error_type: CifErrorType::InvalidAssociationType(x.to_string()),
                    line: number,
                    column: 47,
                }.into())
            }
        };

        let category = match &line[34..36] {
            "JJ" => Some(AssociationType::MainJoinsToOther),
            "VV" => Some(AssociationType::MainDividesFromOther),
            "NP" => Some(AssociationType::MainBecomesOther),
            "  " => None,
            x => {
                return Err(CifError {
                    error_type: CifErrorType::InvalidAssociationCategory(x.to_string()),
                    line: number,
                    column: 34,
                }.into())
            }
        };

        let rev_category = match category {
            Some(AssociationType::MainJoinsToOther) => Some(AssociationType::MainIsJoinedToByOther),
            Some(AssociationType::MainDividesFromOther)
                => Some(AssociationType::MainDividesFromOther),
            Some(AssociationType::MainBecomesOther) => Some(AssociationType::MainFormsFromOther),
            None => None,
            _ => panic!("Invalid association category"),
        };

        if modification_type == ModificationType::Amend {
            // first find any committed associations and modify
            trains_amend_assoc(
                &self.get_train_variant_ids(main_train_id),
                &other_train_id,
                namespace,
                &begin,
                &end,
                &days_of_week,
                &location,
                &location_suffix,
                &other_train_location_suffix,
                &stp_modification_type,
                is_stp,
                day_diff,
                for_passengers,
                category,
                transaction,
            ).await?;
            trains_amend_rev_assoc(
                &self.get_train_variant_ids(other_train_id),
                &main_train_id,
                namespace,
                &begin,
                &end,
                &days_of_week,
                &location,
                &other_train_location_suffix,
                &location_suffix,
                &stp_modification_type,
                is_stp,
                day_diff,
                for_passengers,
                rev_category,
                transaction,
            ).await?;

            // now amend unwritten associations
            self.amend_unwritten_assocs(
                main_train_id,
                location,
                &location_suffix,
                other_train_id,
                &begin,
                &end,
                &days_of_week,
                &other_train_location_suffix,
                &stp_modification_type,
                is_stp,
                day_diff,
                for_passengers,
                category,
                false,
            );
            self.amend_unwritten_assocs(
                other_train_id,
                location,
                &other_train_location_suffix,
                main_train_id,
                &begin,
                &end,
                &days_of_week,
                &location_suffix,
                &stp_modification_type,
                is_stp,
                day_diff,
                for_passengers,
                rev_category,
                true,
            );

            return Ok(());
        }

        let day_diff = match day_diff {
            Some(x) => x,
            None => {
                return Err(CifError {
                    error_type: CifErrorType::InvalidAssociationDateIndicator(" ".to_string()),
                    line: number,
                    column: 36,
                }.into())
            }
        };

        let for_passengers = match for_passengers {
            Some(x) => x,
            None => {
                return Err(CifError {
                    error_type: CifErrorType::InvalidAssociationDateIndicator(" ".to_string()),
                    line: number,
                    column: 47,
                }.into())
            }
        };

        let category = match category {
            Some(x) => x,
            None => {
                return Err(CifError {
                    error_type: CifErrorType::InvalidAssociationDateIndicator("  ".to_string()),
                    line: number,
                    column: 34,
                }.into())
            }
        };
        let rev_category = rev_category.unwrap();

        let rev_begin = rev_date(begin, day_diff);
        let rev_end = rev_date(end, day_diff);
        let rev_days_of_week = rev_days(&days_of_week, day_diff);

        // all of the below will use AssociationNodes, so construct them here
        let mut validity = train_validity_period::ActiveModelEx {
            valid_begin: ActiveValue::Set(begin),
            valid_end: ActiveValue::Set(end),
            timezone: ActiveValue::Set(London.name().to_string()),
            ..Default::default()
        };
        validity.populate_days_of_week(&days_of_week);
        let new_assoc = association_node::ActiveModelEx {
            other_train_id: ActiveValue::Set(other_train_id.to_string()),
            namespace: ActiveValue::Set(namespace.to_string()),
            other_train_location_id_suffix: ActiveValue::Set(other_train_location_suffix.clone()),
            validity: ActiveHasMany::Append(vec![validity]),
            cancellations: ActiveHasMany::Append(vec![]),
            replacements: ActiveHasMany::Append(vec![]),
            day_diff: ActiveValue::Set(day_diff),
            for_passengers: ActiveValue::Set(for_passengers),
            source: ActiveValue::Set(Some(if is_stp {
                TrainSource::ShortTerm
            } else {
                TrainSource::LongTerm
            })),
            association_type: ActiveValue::Set(category),
            ..Default::default()
        };

        let mut validity = train_validity_period::ActiveModelEx {
            valid_begin: ActiveValue::Set(rev_begin),
            valid_end: ActiveValue::Set(rev_end),
            timezone: ActiveValue::Set(London.name().to_string()),
            ..Default::default()
        };
        validity.populate_days_of_week(&rev_days_of_week);
        let new_rev_assoc = association_node::ActiveModelEx {
            other_train_id: ActiveValue::Set(main_train_id.to_string()),
            namespace: ActiveValue::Set(namespace.to_string()),
            other_train_location_id_suffix: ActiveValue::Set(location_suffix.clone()),
            validity: ActiveHasMany::Append(vec![validity]),
            cancellations: ActiveHasMany::Append(vec![]),
            replacements: ActiveHasMany::Append(vec![]),
            day_diff: ActiveValue::Set(-day_diff),
            for_passengers: ActiveValue::Set(for_passengers),
            source: ActiveValue::Set(Some(if is_stp {
                TrainSource::ShortTerm
            } else {
                TrainSource::LongTerm
            })),
            association_type: ActiveValue::Set(rev_category),
            ..Default::default()
        };

        if modification_type == ModificationType::Insert
            && stp_modification_type == ModificationType::Insert
        {
            // As trains might not all have appeared yet, we temporarily add to unwritten_assocs
            self.unwritten_assocs
                .entry((
                    main_train_id.to_string(),
                    location.to_string(),
                    location_suffix,
                ))
                .or_insert(vec![])
                .push(new_assoc);
            self.unwritten_assocs
                .entry((
                    other_train_id.to_string(),
                    location.to_string(),
                    other_train_location_suffix,
                ))
                .or_insert(vec![])
                .push(new_rev_assoc);

            return Ok(());
        }

        if stp_modification_type == ModificationType::Amend {
            // first find any committed associations and modify
            trains_replace_assoc(
                &self.get_train_variant_ids(main_train_id),
                &other_train_id,
                namespace,
                &location,
                &location_suffix,
                &other_train_location_suffix,
                &new_assoc,
                transaction,
            ).await?;
            trains_replace_assoc(
                &self.get_train_variant_ids(other_train_id),
                &main_train_id,
                namespace,
                &location,
                &other_train_location_suffix,
                &location_suffix,
                &new_rev_assoc,
                transaction,
            ).await?;

            self.replace_unwritten_assocs(
                &main_train_id,
                &location,
                &location_suffix,
                &other_train_id,
                &other_train_location_suffix,
                &new_assoc,
            );
            self.replace_unwritten_assocs(
                &other_train_id,
                &location,
                &other_train_location_suffix,
                &main_train_id,
                &location_suffix,
                &new_rev_assoc,
            );

            return Ok(());
        }

        Ok(())
    }

    async fn read_basic_schedule(
        &mut self,
        line: &str,
        namespace: &str,
        number: u64,
        transaction: &DatabaseTransaction,
    ) -> Result<(usize, usize, usize), Error> {
        let modification_type =
            read_modification_type(&line[2..3], produce_cif_error_closure(number, 2))?;
        let (stp_modification_type, is_stp) =
            read_stp_indicator(&line[79..80], produce_cif_error_closure(number, 79))?;

        let main_train_id = &line[3..9];
        let begin = read_date(&line[9..15], produce_cif_error_closure(number, 9))?;

        // At this stage we have all the data we need for a simple delete, so handle this here
        //
        // Note these are NOT the same as STP cancels and indeed handled completely differently
        if modification_type == ModificationType::Delete {
            let train_variants = get_all_train_variants_for_delete(
                &self.get_train_variant_ids(main_train_id),
                transaction
            ).await?;
            match stp_modification_type {
                ModificationType::Insert => {
                    let train_variant_ids_to_delete: HashSet<i64> = train_variants
                        .into_iter()
                        .filter(|train_variant|
                            train_variant.train_id.is_some()
                            && train_variant.validity[0].valid_begin == begin
                            && train_variant.source.unwrap() ==
                                if is_stp { TrainSource::ShortTerm } else { TrainSource::LongTerm }
                        )
                        .map(|x| x.id)
                        .collect();
                    train_variant::Entity::delete_many()
                        .filter(train_variant::COLUMN.id.is_in(train_variant_ids_to_delete.clone()))
                        .exec(transaction)
                        .await?;
                    let cached_variant_ids
                        = self
                        .cached_train_variant_ids
                        .entry(main_train_id.to_string())
                        .or_default();
                    *cached_variant_ids = &*cached_variant_ids - &train_variant_ids_to_delete;
                },
                ModificationType::Amend => {
                    let train_variant_ids_to_delete: HashSet<i64> = train_variants
                        .into_iter()
                        .filter(|train_variant|
                            train_variant.train_id.is_none()
                            && train_variant.validity[0].valid_begin == begin
                        )
                        .map(|x| x.id)
                        .collect();
                    train_variant::Entity::delete_many()
                        .filter(train_variant::COLUMN.id.is_in(train_variant_ids_to_delete.clone()))
                        .exec(transaction)
                        .await?;
                    let cached_variant_ids
                        = self
                        .cached_train_variant_ids
                        .entry(main_train_id.to_string())
                        .or_default();
                    *cached_variant_ids = &*cached_variant_ids - &train_variant_ids_to_delete;
                },
                ModificationType::Delete => {
                    let cancellation_ids_to_delete: Vec<i64> = train_variants
                        .into_iter()
                        .flat_map(|train_variant|
                            train_variant.cancellations.clone().into_iter().filter(|cancellation|
                                cancellation.validity[0].valid_begin == begin
                            ).collect::<Vec<train_cancellation::ModelEx>>()).map(|x| x.id)
                        .collect();
                    train_cancellation::Entity::delete_many()
                        .filter(train_cancellation::COLUMN.id.is_in(cancellation_ids_to_delete))
                        .exec(transaction)
                        .await?;
                },
            };
            return Ok((0, 0, 0));
        }

        let end = read_date(&line[15..21], produce_cif_error_closure(number, 15))?;
        let days_of_week = read_days_of_week(&line[21..28], produce_cif_error_closure(number, 27))?;

        // Now we handle STP cancellations; these are where long-running
        // trains are marked as not running as a one-off
        if stp_modification_type == ModificationType::Delete
            && modification_type == ModificationType::Insert
        {
            let train_variants = get_all_train_variants_for_cancel(
                &self.get_train_variant_ids(main_train_id),
                transaction
            ).await?;
            for train_variant in &train_variants {
                if !check_date_applicability(
                    &train_variant.validity[0].clone().into(), begin, end, &days_of_week
                ) {
                    continue;
                }
                let mut validity = train_validity_period::ActiveModelEx {
                    valid_begin: ActiveValue::Set(begin),
                    valid_end: ActiveValue::Set(end),
                    timezone: ActiveValue::Set(London.name().to_string()),
                    ..Default::default()
                };
                validity.populate_days_of_week(&days_of_week);
                let new_cancel = train_cancellation::ActiveModelEx {
                    validity: ActiveHasMany::Append(vec![validity]),
                    source: ActiveValue::Set(Some(TrainSource::ShortTerm)),
                    ..Default::default()
                };
                let mut train_variant: train_variant::ActiveModelEx = train_variant.clone().into();
                train_variant
                    .cancellations
                    .push(new_cancel);
                train_variant.save(transaction).await?;
            }

            return Ok((0, 0, 0));
        }

        if modification_type == ModificationType::Amend
            && stp_modification_type == ModificationType::Delete
        {
            let train_variants = get_all_train_variants_for_amend_cancel(
                &self.get_train_variant_ids(main_train_id),
                transaction
            ).await?;
            for train_variant in &train_variants {
                for cancellation in train_variant.cancellations.iter() {
                    if cancellation.validity[0].valid_begin == begin {
                        let mut validity = train_validity_period::ActiveModelEx {
                            valid_begin: ActiveValue::Set(begin),
                            valid_end: ActiveValue::Set(end),
                            timezone: ActiveValue::Set(London.name().to_string()),
                            ..Default::default()
                        };
                        validity.populate_days_of_week(&days_of_week);

                        let mut cancellation: train_cancellation::ActiveModelEx
                            = cancellation.clone().into();
                        cancellation.validity = ActiveHasMany::Replace(vec![validity]);
                        cancellation.save(transaction).await?;
                    }
                }
            }
            return Ok((0, 0, 0));
        }

        let train_status = read_train_status(&line[29..30], produce_cif_error_closure(number, 29))?;

        let train_type =
            match read_train_type(&line[30..32], produce_cif_error_closure(number, 30))? {
                Some(x) => x,
                None => match train_status {
                    TrainStatus::Bus => TrainType::Bus,
                    TrainStatus::Freight => TrainType::Freight,
                    TrainStatus::PassengerParcels => TrainType::PassengerParcels,
                    TrainStatus::Ship => TrainType::Ship,
                    TrainStatus::Trip => TrainType::Trip,
                    TrainStatus::StpPassengerParcels => TrainType::PassengerParcels,
                    TrainStatus::StpFreight => TrainType::Freight,
                    TrainStatus::StpTrip => TrainType::Trip,
                    TrainStatus::StpShip => TrainType::Ship,
                    TrainStatus::StpBus => TrainType::Bus,
                    TrainStatus::VstpNone => {
                        return Err(CifError {
                            error_type: CifErrorType::InvalidTrainStatus(format!(
                                "{:#?}",
                                train_status
                            )),
                            line: number,
                            column: 29,
                        }.into())
                    }
                },
            };

        let public_id = &line[32..36];
        let headcode = read_optional_string(&line[36..40]);
        let service_group = &line[41..49];

        let power_type = read_power_type(
            &line[50..53],
            &line[53..57],
            produce_cif_error_closure(number, 50),
        )?;
        let speed_m_per_s = read_speed(&line[57..60], produce_cif_error_closure(number, 57))?;

        let (operating_characteristics, runs_as_required) =
            read_operating_characteristics(&line[60..66], produce_cif_error_closure(number, 60))?;

        let timing_load_str = read_timing_load(
            &line[50..53],
            &line[53..57],
            operating_characteristics.br_mark_four_coaches,
            produce_cif_error_closure(number, 50),
        )?;
        let timing_load_id =
            line[50..57].to_string()
            + if operating_characteristics.br_mark_four_coaches { "1" }
            else { "0" };

        let (first_seating, standard_seating) = read_seating_class(
            &line[66..67],
            train_type,
            produce_cif_error_closure(number, 66),
        )?;
        let (first_sleepers, standard_sleepers) =
            read_sleeper_class(&line[67..68], produce_cif_error_closure(number, 67))?;

        let (catering, wheelchair_reservations) =
            read_catering(&line[70..74], produce_cif_error_closure(number, 70))?;

        let reservations = read_reservations(
            &line[68..69],
            wheelchair_reservations,
            first_seating,
            standard_seating,
            first_sleepers,
            standard_sleepers,
            train_type,
            produce_cif_error_closure(number, 68),
        )?;

        let brand = read_brand(&line[74..78], produce_cif_error_closure(number, 74))?;

        // From this point on we will need to ensure that the line and allocation are in the
        // database, so fill them in here
        let mut allocations_written = 0;
        match &timing_load_str {
            Some(timing_load_str) => {
                if !self.cached_allocation_ids.contains(&timing_load_id) {
                    let timing_load = train_allocation::Entity::load()
                        .filter(train_allocation::COLUMN.id.eq(&timing_load_id))
                        .filter(train_allocation::COLUMN.namespace.eq(namespace))
                        .one(transaction)
                        .await?;
                    if timing_load.is_none() {
                        let timing_load = train_allocation::ActiveModelEx {
                            id: ActiveValue::Set(timing_load_id.clone()),
                            namespace: ActiveValue::Set(namespace.to_string()),
                            description: ActiveValue::Set(timing_load_str.clone()),
                            vehicles: ActiveHasMany::Append(vec![]),
                            ..Default::default()
                        };

                        timing_load.insert(transaction).await?;

                        allocations_written += 1;
                    }
                    self.cached_allocation_ids.insert(timing_load_id.clone());
                }
            }
            None => (),
        };

        let mut lines_written = 0;
        if !self.cached_line_ids.contains(service_group) {
            let line = line::Entity::load()
                .filter(line::COLUMN.id.eq(service_group))
                .filter(line::COLUMN.namespace.eq(namespace))
                .one(transaction)
                .await?;
            if line.is_none() {
                let line = line::ActiveModelEx {
                    id: ActiveValue::Set(service_group.to_string()),
                    namespace: ActiveValue::Set(namespace.to_string()),
                    public_id: ActiveValue::Set(None),
                    name: ActiveValue::Set(None),
                    description: ActiveValue::Set(None),
                    url: ActiveValue::Set(None),
                    background_colour: ActiveValue::Set(None),
                    foreground_colour: ActiveValue::Set(None),
                    ..Default::default()
                };

                line.insert(transaction).await?;

                lines_written += 1;
            }
            self.cached_line_ids.insert(service_group.to_string());
        }

        // all of the below will use this so construct it now
        let mut validity = train_validity_period::ActiveModelEx {
            valid_begin: ActiveValue::Set(begin),
            valid_end: ActiveValue::Set(end),
            timezone: ActiveValue::Set(London.name().to_string()),
            ..Default::default()
        };
        validity.populate_days_of_week(&days_of_week);
        let mut variable_train = variable_train::ActiveModelEx {
            namespace: ActiveValue::Set(namespace.to_string()),
            train_type: ActiveValue::Set(train_type),
            public_id: ActiveValue::Set(Some(public_id.to_string())),
            headcode: ActiveValue::Set(headcode),
            power_type: ActiveValue::Set(power_type),
            timing_allocation_id: ActiveValue::Set(match timing_load_str {
                None => None,
                Some(_) => Some(timing_load_id),
            }),
            actual_allocation_id: ActiveValue::Set(None),
            timing_speed_m_per_s: ActiveValue::Set(speed_m_per_s),
            accommodation: ActiveHasMany::Append(vec![
                accommodation_types::ActiveModelEx {
                    class: ActiveValue::Set(AccommodationClass::First),
                    standing: ActiveValue::Set(None),
                    seating: ActiveValue::Set(Some(first_seating)),
                    reclining_seating: ActiveValue::Set(None),
                    special_seating: ActiveValue::Set(None),
                    sleeper: ActiveValue::Set(Some(first_sleepers)),
                    single_sleeper: ActiveValue::Set(None),
                    double_sleeper: ActiveValue::Set(None),
                    special_sleeper: ActiveValue::Set(None),
                    couchette: ActiveValue::Set(None),
                    single_couchette: ActiveValue::Set(None),
                    double_couchette: ActiveValue::Set(None),
                    baby: ActiveValue::Set(None),
                    family: ActiveValue::Set(None),
                    recreation: ActiveValue::Set(None),
                    panoramic: ActiveValue::Set(None),
                    pullman: ActiveValue::Set(None),
                    pushchair: ActiveValue::Set(None),
                    wheelchair: ActiveValue::Set(None),
                    has_female_only: ActiveValue::Set(None),
                    has_male_only: ActiveValue::Set(None),
                    has_same_sex_only: ActiveValue::Set(None),
                    ..Default::default()
                },
                accommodation_types::ActiveModelEx {
                    class: ActiveValue::Set(AccommodationClass::Second),
                    standing: ActiveValue::Set(None),
                    seating: ActiveValue::Set(Some(standard_seating)),
                    reclining_seating: ActiveValue::Set(None),
                    special_seating: ActiveValue::Set(None),
                    sleeper: ActiveValue::Set(Some(standard_sleepers)),
                    single_sleeper: ActiveValue::Set(None),
                    double_sleeper: ActiveValue::Set(None),
                    special_sleeper: ActiveValue::Set(None),
                    couchette: ActiveValue::Set(None),
                    single_couchette: ActiveValue::Set(None),
                    double_couchette: ActiveValue::Set(None),
                    baby: ActiveValue::Set(None),
                    family: ActiveValue::Set(None),
                    recreation: ActiveValue::Set(None),
                    panoramic: ActiveValue::Set(None),
                    pullman: ActiveValue::Set(None),
                    pushchair: ActiveValue::Set(None),
                    wheelchair: ActiveValue::Set(None),
                    has_female_only: ActiveValue::Set(None),
                    has_male_only: ActiveValue::Set(None),
                    has_same_sex_only: ActiveValue::Set(None),
                    ..Default::default()
                },
            ]),
            brand: ActiveValue::Set(brand),
            name: ActiveValue::Set(None),
            line_id: ActiveValue::Set(Some(service_group.to_string())),
            uic_code: ActiveValue::Set(None),
            operator_id: ActiveValue::Set(None),
            wheelchair_accessible: ActiveValue::Set(None),
            has_toilets: ActiveValue::Set(false),
            has_luggage: ActiveValue::Set(false),
            has_families: ActiveValue::Set(false),
            has_passenger_communications: ActiveValue::Set(false),
            has_assistance: ActiveValue::Set(false),
            has_passenger_information: ActiveValue::Set(false),
            ..Default::default()
        };
        variable_train.populate_reservations(&reservations);
        variable_train.populate_catering(&catering);
        variable_train.populate_operating_characteristics(&operating_characteristics);
        let mut new_train_variant = train_variant::ActiveModelEx {
            // TODO do we need to set ID?
            namespace: ActiveValue::Set(namespace.to_string()),
            validity: ActiveHasMany::Append(vec![validity]),
            cancellations: ActiveHasMany::Append(vec![]),
            replacements: ActiveHasMany::Append(vec![]),
            variable_train: ActiveHasOne::Set(Some(Box::new(variable_train))),
            source: ActiveValue::Set(Some(if is_stp {
                TrainSource::ShortTerm
            } else {
                TrainSource::LongTerm
            })),
            runs_as_required: ActiveValue::Set(runs_as_required),
            performance_monitoring: ActiveValue::Set(None),
            route: ActiveHasMany::Append(vec![]),
            ..Default::default()
        };

        if modification_type == ModificationType::Amend {
            // We are finding an existing train and completely replacing it in the DB
            let train_variants = get_all_train_variants_for_amend(
                &self.get_train_variant_ids(main_train_id),
                transaction
            ).await?;
            // This is a rare case of actually needing to use `Replace` despite the performance cost
            // as we need to delete the previous contents
            new_train_variant.route = ActiveHasMany::Replace(vec![]);
            new_train_variant.validity
                = ActiveHasMany::Replace(new_train_variant.validity.into_vec());
            for train_variant in &train_variants {
                if stp_modification_type == ModificationType::Insert
                    && train_variant.source == Some(
                        if is_stp { TrainSource::ShortTerm } else { TrainSource::LongTerm }
                    )
                    && train_variant.validity[0].valid_begin == begin
                    && train_variant.train_id.is_some()
                {
                    let mut train_variant: train_variant::ActiveModelEx
                        = train_variant.clone().into();
                    amend_train(&mut train_variant, new_train_variant.clone());
                    self.last_train = Some(LastTrain::DatabaseSave(train_variant));
                    self.last_train_id = Some(main_train_id.to_string());
                }
                else if stp_modification_type == ModificationType::Amend
                    && train_variant.validity[0].valid_begin == begin
                    && train_variant.train_id.is_none()
                {
                    let mut train_variant: train_variant::ActiveModelEx
                        = train_variant.clone().into();
                    amend_train(&mut train_variant, new_train_variant.clone());
                    self.last_train = Some(LastTrain::DatabaseSave(train_variant));
                    self.last_train_id = Some(main_train_id.to_string());
                }
            }
            return Ok((0, lines_written, allocations_written));
        }

        if modification_type == ModificationType::Insert
            && stp_modification_type == ModificationType::Insert
        {
            // we can write a (partial) train now, and continue updating it later.
            if !self.cached_train_variant_ids.contains_key(main_train_id) {
                // If there's no cached key, that means there is also no parent train, so construct
                // it now.
                let train = train::ActiveModelEx {
                    id: ActiveValue::Set(main_train_id.to_string()),
                    namespace: ActiveValue::Set(namespace.to_string()),
                    ..Default::default()
                };

                new_train_variant.train = ActiveBelongsTo::Set(Some(Box::new(train)));
                self.last_train = Some(LastTrain::DatabaseInsert(new_train_variant));
                self.last_train_id = Some(main_train_id.to_string());

                return Ok((1, lines_written, allocations_written));
            }

            new_train_variant.train_id = ActiveValue::Set(Some(main_train_id.to_string()));
            self.last_train = Some(LastTrain::DatabaseInsert(new_train_variant));
            self.last_train_id = Some(main_train_id.to_string());

            // A train variant is not a train, to be pedantic
            return Ok((0, lines_written, allocations_written));
        }

        if stp_modification_type == ModificationType::Amend {
            // we can write a (partial) train now, and continue updating it later.
            let train_variants = get_all_train_variants_for_replace(
                &self.get_train_variant_ids(main_train_id),
                transaction
            ).await?;

            let mut replaced = false;
            for train_variant in &train_variants {
                // We replace main trains
                if train_variant.train_id.is_none() {
                    continue;
                }
                if !check_date_applicability(
                    &train_variant.validity[0].clone().into(), begin, end, &days_of_week
                ) {
                    continue;
                }
                let mut new_train_variant = new_train_variant.clone();
                new_train_variant.parent_train_variant_id
                    = ActiveValue::Set(Some(train_variant.id));

                replaced = true;

                self.last_train = Some(LastTrain::DatabaseInsert(new_train_variant));
                self.last_train_id = Some(main_train_id.to_string());
            }

            if !replaced {
                self.orphaned_overlay_trains
                    .insert((main_train_id.to_string(), begin), new_train_variant);
                self.last_train = Some(LastTrain::Orphaned((main_train_id.to_string(), begin)));
                self.last_train_id = Some(main_train_id.to_string());
            }

            // A train variant is not a train, to be pedantic
            return Ok((0, lines_written, allocations_written));
        }

        panic!("Unreachable");
    }

    async fn read_extended_schedule(
        &mut self,
        line: &str,
        namespace: &str,
        number: u64,
        transaction: &DatabaseTransaction
    ) -> Result<usize, Error> {
        // at this stage we can only be in an insert or amend statement, for STP other than CAN. So
        // we find the train we are inserting or amending.

        let uic_code = read_optional_string(&line[6..11]);

        let atoc_code = &line[11..13];

        let train_operator_desc =
            read_train_operator(atoc_code, produce_cif_error_closure(number, 11))?;

        let performance_monitoring =
            read_ats_code(&line[13..14], produce_cif_error_closure(number, 13))?;

        let mut operators_written = 0;
        if !self.cached_operator_ids.contains(atoc_code) {
            let operator = train_operator::Entity::load()
                .filter(train_operator::COLUMN.id.eq(atoc_code))
                .filter(train_operator::COLUMN.namespace.eq(namespace))
                .one(transaction)
                .await?;
            if operator.is_none() {
                let operator = train_operator::ActiveModelEx {
                    id: ActiveValue::Set(atoc_code.to_string()),
                    namespace: ActiveValue::Set(namespace.to_string()),
                    public_id: ActiveValue::Set(None),
                    description: ActiveValue::Set(train_operator_desc),
                    ..Default::default()
                };

                operator.insert(transaction).await?;

                operators_written += 1;
            }
        }

        self.cached_operator_ids.insert(atoc_code.to_string());

        let last_train = self.get_last_train(number, "BX")?;

        last_train.variable_train.as_mut().unwrap().uic_code = ActiveValue::Set(uic_code);
        last_train.variable_train.as_mut().unwrap().operator_id
            = ActiveValue::Set(Some(atoc_code.to_string()));
        last_train.performance_monitoring = ActiveValue::Set(Some(performance_monitoring));

        Ok(operators_written)
    }

    async fn read_location_origin(
        &mut self,
        line: &str,
        namespace: &str,
        number: u64,
        _transaction: &DatabaseTransaction
    ) -> Result<(), Error> {
        // at this stage we can only be in an insert or amend statement, for STP other than CAN. So
        // we find the train we are inserting or amending.

        let location_id = &line[2..9].trim();
        let location_suffix = read_optional_string(&line[9..10]);

        let wtt_dep =
            read_mandatory_wtt_time(&line[10..15], produce_cif_error_closure(number, 10))?;
        let pub_dep = read_public_time(&line[15..19], produce_cif_error_closure(number, 15))?;

        let platform = read_optional_string(&line[19..22].trim());
        let line_code = read_optional_string(&line[22..25].trim());

        let eng_allowance = read_allowance(&line[25..27], produce_cif_error_closure(number, 25))?;
        let path_allowance = read_allowance(&line[27..29], produce_cif_error_closure(number, 27))?;

        let activities = read_activities(&line[29..41], produce_cif_error_closure(number, 29))?;

        let perf_allowance = read_allowance(&line[41..43], produce_cif_error_closure(number, 41))?;

        let mut new_location = train_location::ActiveModelEx {
            index: ActiveValue::Set(0),
            timing_tz: ActiveValue::Set(None),
            location_id: ActiveValue::Set(location_id.to_string()),
            namespace: ActiveValue::Set(namespace.to_string()),
            id_suffix: ActiveValue::Set(location_suffix),
            working_arr: ActiveValue::Set(None),
            working_arr_day: ActiveValue::Set(None),
            working_dep: ActiveValue::Set(Some(wtt_dep)),
            working_dep_day: ActiveValue::Set(Some(0)),
            working_pass: ActiveValue::Set(None),
            working_pass_day: ActiveValue::Set(None),
            public_arr: ActiveValue::Set(None),
            public_arr_day: ActiveValue::Set(None),
            public_dep: ActiveValue::Set(pub_dep),
            public_dep_day: ActiveValue::Set(Some(0)),
            platform: ActiveValue::Set(platform),
            platform_zone: ActiveValue::Set(None),
            line: ActiveValue::Set(line_code),
            path: ActiveValue::Set(None),
            engineering_allowance_s: ActiveValue::Set(Some(eng_allowance)),
            pathing_allowance_s: ActiveValue::Set(Some(path_allowance)),
            performance_allowance_s: ActiveValue::Set(Some(perf_allowance)),
            association_nodes: ActiveHasMany::Append(vec![]),
            ..Default::default()
        };
        new_location.populate_activities(&activities);

        {
            let last_train = self.get_last_train(number, "LI")?;

            if !last_train.route.as_mut_vec().is_empty() {
                return Err(CifError {
                    error_type: CifErrorType::UnexpectedRecordType(
                        "LO".to_string(),
                        "Train route not empty".to_string(),
                    ),
                    line: number,
                    column: 0,
                }.into());
            }

            last_train.route.as_mut_vec().push(new_location);
        }

        Ok(())
    }

    async fn read_location_intermediate(
        &mut self,
        line: &str,
        namespace: &str,
        number: u64,
        _transaction: &DatabaseTransaction
    ) -> Result<(), Error> {
        // at this stage we can only be in an insert or amend statement, for STP other than CAN. So
        // we find the train we are inserting or amending.

        let location_id = &line[2..9].trim();
        let location_suffix = read_optional_string(&line[9..10]);

        self.validate_change_en_route_location(location_id, &location_suffix, number, 2)?;

        let wtt_arr = read_optional_wtt_time(&line[10..15], produce_cif_error_closure(number, 10))?;
        let wtt_dep = read_optional_wtt_time(&line[15..20], produce_cif_error_closure(number, 15))?;
        let wtt_pass =
            read_optional_wtt_time(&line[20..25], produce_cif_error_closure(number, 20))?;

        match (wtt_arr, wtt_dep, wtt_pass) {
            (None, None, Some(_)) => (),
            (Some(_), Some(_), None) => (),
            (_, _, _) => {
                return Err(CifError {
                    error_type: CifErrorType::InvalidWttTimesCombo,
                    line: number,
                    column: 10,
                }.into())
            }
        };

        let pub_arr = read_public_time(&line[25..29], produce_cif_error_closure(number, 25))?;
        let pub_dep = read_public_time(&line[29..33], produce_cif_error_closure(number, 29))?;

        let platform = read_optional_string(&line[33..36].trim());
        let line_code = read_optional_string(&line[36..39].trim());
        let path_code = read_optional_string(&line[39..42].trim());

        let activities = read_activities(&line[42..54], produce_cif_error_closure(number, 42))?;

        let eng_allowance = read_allowance(&line[54..56], produce_cif_error_closure(number, 54))?;
        let path_allowance = read_allowance(&line[56..58], produce_cif_error_closure(number, 56))?;
        let perf_allowance = read_allowance(&line[58..60], produce_cif_error_closure(number, 58))?;

        // For efficiency extract into ActiveHasOne::NotSet if it's None to avoid an extra load
        let change_en_route = match self.change_en_route.take() {
            Some(x) => ActiveHasOne::Set(Some(x)),
            None => ActiveHasOne::NotSet,
        };

        self.cr_location = None;

        {
            let last_train = self.get_last_train(number, "LI")?;

            if last_train.route.as_mut_vec().is_empty() {
                return Err(CifError {
                    error_type: CifErrorType::UnexpectedRecordType(
                        "LI".to_string(),
                        "Train route is empty".to_string(),
                    ),
                    line: number,
                    column: 0,
                }.into());
            }

            let (last_wtt_time, last_wtt_day)
                = get_working_time(last_train.route.as_mut_vec().last().unwrap());

            let wtt_arr_day = calculate_day(&wtt_arr, &last_wtt_time, last_wtt_day);
            let wtt_dep_day = calculate_day(&wtt_dep, &last_wtt_time, last_wtt_day);
            let wtt_pass_day = calculate_day(&wtt_pass, &last_wtt_time, last_wtt_day);

            // TODO maybe should change this to calculate based on last public time?
            let pub_arr_day = calculate_day(&pub_arr, &last_wtt_time, last_wtt_day);
            let pub_dep_day = calculate_day(&pub_dep, &last_wtt_time, last_wtt_day);

            let mut new_location = train_location::ActiveModelEx {
                timing_tz: ActiveValue::Set(None),
                location_id: ActiveValue::Set(location_id.to_string()),
                namespace: ActiveValue::Set(namespace.to_string()),
                id_suffix: ActiveValue::Set(location_suffix),
                index: ActiveValue::Set(last_train.route.as_mut_vec().len().try_into().unwrap()),
                working_arr: ActiveValue::Set(wtt_arr),
                working_arr_day: ActiveValue::Set(wtt_arr_day),
                working_dep: ActiveValue::Set(wtt_dep),
                working_dep_day: ActiveValue::Set(wtt_dep_day),
                working_pass: ActiveValue::Set(wtt_pass),
                working_pass_day: ActiveValue::Set(wtt_pass_day),
                public_arr: ActiveValue::Set(pub_arr),
                public_arr_day: ActiveValue::Set(pub_arr_day),
                public_dep: ActiveValue::Set(pub_dep),
                public_dep_day: ActiveValue::Set(pub_dep_day),
                platform: ActiveValue::Set(platform),
                platform_zone: ActiveValue::Set(None),
                line: ActiveValue::Set(line_code),
                path: ActiveValue::Set(path_code),
                engineering_allowance_s: ActiveValue::Set(Some(eng_allowance)),
                pathing_allowance_s: ActiveValue::Set(Some(path_allowance)),
                performance_allowance_s: ActiveValue::Set(Some(perf_allowance)),
                change_en_route: change_en_route,
                association_nodes: ActiveHasMany::Append(vec![]),
                ..Default::default()
            };
            new_location.populate_activities(&activities);

            last_train.route.as_mut_vec().push(new_location);
        }

        Ok(())
    }

    async fn read_location_terminating(
        &mut self,
        line: &str,
        namespace: &str,
        number: u64,
        transaction: &DatabaseTransaction
    ) -> Result<(), Error> {
        // at this stage we can only be in an insert or amend statement, for STP other than CAN. So
        // we find the train we are inserting or amending.

        let location_id = &line[2..9].trim();
        let location_suffix = read_optional_string(&line[9..10]);

        self.validate_change_en_route_location(location_id, &location_suffix, number, 2)?;

        let wtt_arr =
            read_mandatory_wtt_time(&line[10..15], produce_cif_error_closure(number, 10))?;
        let pub_arr = read_public_time(&line[15..19], produce_cif_error_closure(number, 15))?;

        let platform = read_optional_string(&line[19..22].trim());
        let path_code = read_optional_string(&line[22..25].trim());

        let activities = read_activities(&line[25..37], produce_cif_error_closure(number, 25))?;

        self.cr_location = None;
        let change_en_route = match self.change_en_route.take() {
            Some(x) => ActiveHasOne::Set(Some(x)),
            None => ActiveHasOne::NotSet,
        };

        {
            let last_train = self.get_last_train(number, "LT")?;

            if last_train.route.as_mut_vec().is_empty() {
                return Err(CifError {
                    error_type: CifErrorType::UnexpectedRecordType(
                        "LT".to_string(),
                        "Train route is empty".to_string(),
                    ),
                    line: number,
                    column: 0,
                }.into());
            }

            let (last_wtt_time, last_wtt_day)
                = get_working_time(last_train.route.as_mut_vec().last().unwrap());

            let wtt_arr_day = calculate_day(&Some(wtt_arr), &last_wtt_time, last_wtt_day).unwrap();
            let pub_arr_day = calculate_day(&pub_arr, &last_wtt_time, last_wtt_day);

            let mut new_location = train_location::ActiveModelEx {
                timing_tz: ActiveValue::Set(None),
                location_id: ActiveValue::Set(location_id.to_string()),
                namespace: ActiveValue::Set(namespace.to_string()),
                index: ActiveValue::Set(last_train.route.as_mut_vec().len().try_into().unwrap()),
                id_suffix: ActiveValue::Set(location_suffix),
                working_arr: ActiveValue::Set(Some(wtt_arr)),
                working_arr_day: ActiveValue::Set(Some(wtt_arr_day)),
                working_dep: ActiveValue::Set(None),
                working_dep_day: ActiveValue::Set(None),
                working_pass: ActiveValue::Set(None),
                working_pass_day: ActiveValue::Set(None),
                public_arr: ActiveValue::Set(pub_arr),
                public_arr_day: ActiveValue::Set(pub_arr_day),
                public_dep: ActiveValue::Set(None),
                public_dep_day: ActiveValue::Set(None),
                platform: ActiveValue::Set(platform),
                platform_zone: ActiveValue::Set(None),
                line: ActiveValue::Set(None),
                path: ActiveValue::Set(path_code),
                engineering_allowance_s: ActiveValue::Set(None),
                pathing_allowance_s: ActiveValue::Set(None),
                performance_allowance_s: ActiveValue::Set(None),
                change_en_route,
                association_nodes: ActiveHasMany::Append(vec![]),
                ..Default::default()
            };
            new_location.populate_activities(&activities);

            last_train.route.as_mut_vec().push(new_location);
        }

        // we can now persist and unset the last_train as this should be the last message received
        // for any given train
        match &self.last_train {
            Some(LastTrain::DatabaseInsert(last_train)) => {
                // We originally set up the train to have the right relationships so a simple insert
                // should be possible here. However, there are also performance issues with SeaORM,
                // especially since NR trains are likely to have more locations than most other
                // providers due to being WTTs. So we extract out the locations to insert manually.
                let mut last_train = last_train.clone();
                let mut route = last_train.route.into_vec();
                last_train.route = ActiveHasMany::NotSet;
                let inserted = last_train.clone().insert(transaction).await?;
                let mut changes_en_route: HashMap<i64, Option<variable_train::ActiveModelEx>>
                    = HashMap::new();
                for train_location in route.iter_mut() {
                    train_location.train_variant_id = ActiveValue::Set(inserted.id);
                    changes_en_route.insert(
                        train_location.index.clone().unwrap(),
                        train_location.change_en_route.clone().into_option(),
                    );
                    train_location.change_en_route = ActiveHasOne::NotSet;
                }
                let inserted_ids =
                    train_location::Entity
                    ::insert_many(
                        route.into_iter().map(|x| Into::<train_location::ActiveModel>::into(x))
                    )
                    .exec_with_returning_keys(transaction)
                    .await?;
                for (index, mut change_en_route) in changes_en_route {
                    // We could try to be fancy here, but these could be fairly complex models in
                    // theory, and they're likely rare enough we can just push them manually
                    match &mut change_en_route {
                        Some(change_en_route) => {
                            change_en_route.train_location_id
                                = ActiveValue::Set(
                                    Some(inserted_ids[usize::try_from(index).unwrap()])
                                );
                            change_en_route.clone().insert(transaction).await?;
                        },
                        None => (),
                    };
                }
                self
                    .cached_train_variant_ids
                    .entry(self.last_train_id.clone().unwrap())
                    .or_default()
                    .insert(inserted.id);
            },
            Some(LastTrain::DatabaseSave(last_train)) => {
                // These are rare (amends only), so don't waste time optimising (and this would also
                // be rather tricky here). Also we don't need to add to the cache because we know
                // the trains already exist.
                last_train.clone().save(transaction).await?;
            },
            _ => (),
        };
        self.last_train = None;
        self.last_train_id = None;

        Ok(())
    }

    async fn read_change_en_route(
        &mut self,
        line: &str,
        namespace: &str,
        number: u64,
        transaction: &DatabaseTransaction,
    ) -> Result<(usize, usize), Error> {
        // at this stage we can only be in an insert or amend statement, for STP other than CAN. So
        // we find the train we are inserting or amending.

        let (train_type, operator_id) = {
            let last_train = self.get_last_train(number, "CR")?;

            if last_train.route.as_mut_vec().is_empty() {
                return Err(CifError {
                    error_type: CifErrorType::UnexpectedRecordType(
                        "CR".to_string(),
                        "Train route is empty".to_string(),
                    ),
                    line: number,
                    column: 0,
                }.into());
            }

            let train_type =
                match read_train_type(&line[10..12], produce_cif_error_closure(number, 10))? {
                    Some(x) => x,
                    // should only really happen for ships
                    None =>
                        last_train.variable_train.as_ref().unwrap().train_type.clone().unwrap(),
                };

            (
                train_type,
                last_train.variable_train.as_ref().unwrap().operator_id.clone().unwrap(),
            )
        };

        let location_id = &line[2..9].trim();
        let location_suffix = read_optional_string(&line[9..10]);

        self.cr_location = Some((location_id.to_string(), location_suffix));

        let public_id = &line[12..16];
        let headcode = read_optional_string(&line[16..20]);
        let service_group = &line[21..29];

        let power_type = read_power_type(
            &line[30..33],
            &line[33..37],
            produce_cif_error_closure(number, 30),
        )?;

        let speed_m_per_s = read_speed(&line[37..40], produce_cif_error_closure(number, 37))?;

        let (operating_characteristics, _runs_as_required) =
            read_operating_characteristics(&line[40..46], produce_cif_error_closure(number, 40))?;

        let timing_load_str = read_timing_load(
            &line[30..33],
            &line[33..37],
            operating_characteristics.br_mark_four_coaches,
            produce_cif_error_closure(number, 30),
        )?;
        let timing_load_id =
            line[30..37].to_string()
            + if operating_characteristics.br_mark_four_coaches { "1" }
            else { "0" };

        let (first_seating, standard_seating) = read_seating_class(
            &line[46..47],
            train_type,
            produce_cif_error_closure(number, 46),
        )?;
        let (first_sleepers, standard_sleepers) =
            read_sleeper_class(&line[47..48], produce_cif_error_closure(number, 47))?;

        let (catering, wheelchair_reservations) =
            read_catering(&line[50..54], produce_cif_error_closure(number, 50))?;

        let reservations = read_reservations(
            &line[48..49],
            wheelchair_reservations,
            first_seating,
            standard_seating,
            first_sleepers,
            standard_sleepers,
            train_type,
            produce_cif_error_closure(number, 48),
        )?;

        let brand = read_brand(&line[54..58], produce_cif_error_closure(number, 54))?;

        let uic_code = read_optional_string(&line[62..67]);

        // From this point on we will need to ensure that the line and allocation are in the
        // database, so fill them in here
        let mut allocations_written = 0;
        match &timing_load_str {
            Some(timing_load_str) => {
                if !self.cached_allocation_ids.contains(&timing_load_id) {
                    let timing_load = train_allocation::Entity::load()
                        .filter(train_allocation::COLUMN.id.eq(&timing_load_id))
                        .filter(train_allocation::COLUMN.namespace.eq(namespace))
                        .one(transaction)
                        .await?;
                    if timing_load.is_none() {
                        let timing_load = train_allocation::ActiveModelEx {
                            id: ActiveValue::Set(timing_load_id.clone()),
                            namespace: ActiveValue::Set(namespace.to_string()),
                            description: ActiveValue::Set(timing_load_str.clone()),
                            vehicles: ActiveHasMany::Append(vec![]),
                            ..Default::default()
                        };

                        timing_load.insert(transaction).await?;

                        allocations_written += 1;
                    }
                    self.cached_allocation_ids.insert(timing_load_id.clone());
                }
            }
            None => (),
        };

        let mut lines_written = 0;
        if !self.cached_line_ids.contains(service_group) {
            let line = line::Entity::load()
                .filter(line::COLUMN.id.eq(service_group))
                .filter(line::COLUMN.namespace.eq(namespace))
                .one(transaction)
                .await?;
            if line.is_none() {
                let line = line::ActiveModelEx {
                    id: ActiveValue::Set(service_group.to_string()),
                    namespace: ActiveValue::Set(namespace.to_string()),
                    public_id: ActiveValue::Set(None),
                    name: ActiveValue::Set(None),
                    description: ActiveValue::Set(None),
                    url: ActiveValue::Set(None),
                    background_colour: ActiveValue::Set(None),
                    foreground_colour: ActiveValue::Set(None),
                    ..Default::default()
                };

                line.insert(transaction).await?;

                lines_written += 1;
            }
            self.cached_line_ids.insert(service_group.to_string());
        }

        let mut change_en_route = variable_train::ActiveModelEx {
            namespace: ActiveValue::Set(namespace.to_string()),
            train_type: ActiveValue::Set(train_type),
            public_id: ActiveValue::Set(Some(public_id.to_string())),
            headcode: ActiveValue::Set(headcode),
            power_type: ActiveValue::Set(power_type),
            timing_allocation_id: ActiveValue::Set(match timing_load_str {
                None => None,
                Some(_) => Some(timing_load_id),
            }),
            actual_allocation_id: ActiveValue::Set(None),
            timing_speed_m_per_s: ActiveValue::Set(speed_m_per_s),
            accommodation: ActiveHasMany::Append(vec![
                accommodation_types::ActiveModelEx {
                    class: ActiveValue::Set(AccommodationClass::First),
                    standing: ActiveValue::Set(None),
                    seating: ActiveValue::Set(Some(first_seating)),
                    reclining_seating: ActiveValue::Set(None),
                    special_seating: ActiveValue::Set(None),
                    sleeper: ActiveValue::Set(Some(first_sleepers)),
                    single_sleeper: ActiveValue::Set(None),
                    double_sleeper: ActiveValue::Set(None),
                    special_sleeper: ActiveValue::Set(None),
                    couchette: ActiveValue::Set(None),
                    single_couchette: ActiveValue::Set(None),
                    double_couchette: ActiveValue::Set(None),
                    baby: ActiveValue::Set(None),
                    family: ActiveValue::Set(None),
                    recreation: ActiveValue::Set(None),
                    panoramic: ActiveValue::Set(None),
                    pullman: ActiveValue::Set(None),
                    pushchair: ActiveValue::Set(None),
                    wheelchair: ActiveValue::Set(None),
                    has_female_only: ActiveValue::Set(None),
                    has_male_only: ActiveValue::Set(None),
                    has_same_sex_only: ActiveValue::Set(None),
                    ..Default::default()
                },
                accommodation_types::ActiveModelEx {
                    class: ActiveValue::Set(AccommodationClass::Second),
                    standing: ActiveValue::Set(None),
                    seating: ActiveValue::Set(Some(standard_seating)),
                    reclining_seating: ActiveValue::Set(None),
                    special_seating: ActiveValue::Set(None),
                    sleeper: ActiveValue::Set(Some(standard_sleepers)),
                    single_sleeper: ActiveValue::Set(None),
                    double_sleeper: ActiveValue::Set(None),
                    special_sleeper: ActiveValue::Set(None),
                    couchette: ActiveValue::Set(None),
                    single_couchette: ActiveValue::Set(None),
                    double_couchette: ActiveValue::Set(None),
                    baby: ActiveValue::Set(None),
                    family: ActiveValue::Set(None),
                    recreation: ActiveValue::Set(None),
                    panoramic: ActiveValue::Set(None),
                    pullman: ActiveValue::Set(None),
                    pushchair: ActiveValue::Set(None),
                    wheelchair: ActiveValue::Set(None),
                    has_female_only: ActiveValue::Set(None),
                    has_male_only: ActiveValue::Set(None),
                    has_same_sex_only: ActiveValue::Set(None),
                    ..Default::default()
                },
            ]),
            brand: ActiveValue::Set(brand),
            name: ActiveValue::Set(None),
            line_id: ActiveValue::Set(Some(service_group.to_string())),
            uic_code: ActiveValue::Set(uic_code),
            operator_id: ActiveValue::Set(operator_id),
            wheelchair_accessible: ActiveValue::Set(None),
            has_toilets: ActiveValue::Set(false),
            has_luggage: ActiveValue::Set(false),
            has_families: ActiveValue::Set(false),
            has_passenger_communications: ActiveValue::Set(false),
            has_assistance: ActiveValue::Set(false),
            has_passenger_information: ActiveValue::Set(false),
            ..Default::default()
        };
        change_en_route.populate_reservations(&reservations);
        change_en_route.populate_catering(&catering);
        change_en_route.populate_operating_characteristics(&operating_characteristics);
        self.change_en_route = Some(Box::new(change_en_route));

        Ok((lines_written, allocations_written))
    }

    async fn read_tiploc(
        &self,
        line: &str,
        namespace: &str,
        number: u64,
        modification_type: ModificationType,
        transaction: &DatabaseTransaction,
    ) -> Result<usize, Error> {
        let tiploc = &line[2..9].trim();
        let name = &line[18..44].trim();
        let opt_crs = read_optional_string(&line[53..56]);

        match modification_type {
            ModificationType::Insert => {
                let location = location::ActiveModelEx {
                    id: ActiveValue::Set(tiploc.to_string()),
                    namespace: ActiveValue::Set(namespace.to_string()),
                    name: ActiveValue::Set(name.to_string()),
                    public_id: ActiveValue::Set(opt_crs.clone()),
                    // Assume London, this will be overridden later
                    timezone: ActiveValue::Set(London.name().to_string()),
                    ..Default::default()
                };
                location.insert(transaction).await?;
                Ok(1)
            },
            ModificationType::Amend => {
                let location = location::Entity::load()
                    .filter_by_id((tiploc.to_string(), namespace.to_owned()))
                    .one(transaction)
                    .await?;
                let mut location: location::ActiveModelEx = match location {
                    None => {
                        return Err(CifError {
                            error_type: CifErrorType::LocationNotFound(tiploc.to_string()),
                            line: number,
                            column: 2,
                        }.into())
                    }
                    Some(x) => x.into(),
                };
                location.id = ActiveValue::Set(tiploc.to_string());
                location.name = ActiveValue::Set(name.to_string());
                location.public_id = ActiveValue::Set(opt_crs.clone());
                location.update(transaction).await?;
                Ok(0)
            },
            ModificationType::Delete => {
                location::Entity::delete_by_id((tiploc.to_string(), namespace.to_owned()))
                    .exec(transaction)
                    .await?;  // it's OK if the TIPLOC isn't found
                Ok(0)
            },
        }
    }

    async fn read_header(
        &self,
        line: &str,
        schedule: &schedule::ModelEx,
        number: u64,
        transaction: &DatabaseTransaction,
    ) -> Result<(), Error> {
        let namespace = schedule.namespace.clone();
        let mut schedule: schedule::ActiveModelEx = schedule.clone().into();

        schedule.their_id = ActiveValue::Set(Some(line[2..22].to_string()));
        let parsed_datetime = NaiveDateTime::parse_from_str(&line[22..32], "%d%m%y%H%M");
        let parsed_datetime = match parsed_datetime {
            Ok(x) => x,
            Err(x) => {
                return Err(CifError {
                    error_type: CifErrorType::ChronoParseError(x),
                    line: number,
                    column: 22,
                }.into())
            }
        };
        // CIF does not support timezones, assume these times are London time
        schedule.timezone = ActiveValue::Set(Some(London.name().to_string()));
        schedule.last_updated = ActiveValue::Set(Some(parsed_datetime));
        schedule.valid_begin = ActiveValue::Set(Some(read_backwards_date(
            &line[48..54],
            produce_cif_error_closure(number, 48),
        )?));
        schedule.valid_end = ActiveValue::Set(Some(read_backwards_date(
            &line[54..60],
            produce_cif_error_closure(number, 48),
        )?));

        println!("[{}] Updating root schedule...", namespace);
        schedule.update(transaction).await?;
        println!("[{}] Updated root schedule", namespace);

        Ok(())
    }

    async fn finalise(
        &mut self,
        _line: &str,
        namespace: &str,
        number: u64,
        transaction: &DatabaseTransaction,
    ) -> Result<usize, Error> {
        println!(
            "[{}] Writing {} unwritten associations...", namespace, self.unwritten_assocs.len()
        );
        for ((train_id, location, location_suffix), assocs) in &self.unwritten_assocs {
            if assocs.len() == 0 {
                // Can happen when assocs are deleted
                continue;
            }
            let train_variants = get_all_train_variants_for_assoc_write(
                &self.get_train_variant_ids(train_id),
                transaction
            ).await?;
            if train_variants.len() == 0 {
                return Err(CifError {
                    error_type: CifErrorType::TrainNotFound(train_id.clone()),
                    line: number,
                    column: 0,
                }.into())
            }

            write_assocs_to_trains(
                train_variants, &location, &location_suffix, &assocs, transaction
            ).await?;
        }
        println!("[{}] Written unwritten associations", namespace);
        self.unwritten_assocs.clear();

        let mut trains_written = 0;

        println!(
            "[{}] Writing {} orphaned overlay train variants...",
            namespace,
            self.orphaned_overlay_trains.len(),
        );
        for ((train_id, _begin), new_train_variant) in &self.orphaned_overlay_trains {
            let train_variants = get_all_train_variants_for_replace(
                &self.get_train_variant_ids(train_id),
                transaction
            ).await?;
            if train_variants.len() == 0 {
                let mut new_train_variant = new_train_variant.clone();
                // This orphaned overlay was probably intended to be an N instead.
                let inserted = if !self.cached_train_variant_ids.contains_key(train_id) {
                    // There's no train at all, write a new one
                    let train = train::ActiveModelEx {
                        id: ActiveValue::Set(train_id.to_string()),
                        namespace: ActiveValue::Set(namespace.to_string()),
                        ..Default::default()
                    };

                    new_train_variant.train = ActiveBelongsTo::Set(Some(Box::new(train)));

                    trains_written += 1;
                    new_train_variant.insert(transaction).await?
                } else {
                    // There's an empty train present — likely the previous train got deleted or
                    // cancelled.
                    new_train_variant.train_id = ActiveValue::Set(Some(train_id.to_string()));
                    new_train_variant.insert(transaction).await?
                };

                self
                    .cached_train_variant_ids
                    .entry(train_id.to_string())
                    .or_default()
                    .insert(inserted.id);

                continue;
            }

            for train_variant in train_variants {
                let mut train_variant: train_variant::ActiveModelEx = train_variant.clone().into();
                if !check_date_applicability(
                    &train_variant.validity[0],
                    new_train_variant.validity[0].valid_begin.clone().unwrap(),
                    new_train_variant.validity[0].valid_end.clone().unwrap(),
                    &train_validity_period::DaysOfWeek::get_from_active_model(
                        &new_train_variant.validity[0]
                    ),
                ) {
                    continue;
                }
                train_variant
                    .replacements
                    .push(new_train_variant.clone());
                train_variant.save(transaction).await?;
            }
        }
        println!("[{}] Written orphaned overlay train variants", namespace);
        self.orphaned_overlay_trains.clear();

        Ok(trains_written)
    }

    async fn override_locations(
        &self, namespace: &str, transaction: &DatabaseTransaction
    ) -> Result<(), Error> {
        let mut location_overrides = vec![];
        match &self.config.location_overrides {
            None => (),
            Some(filename) => match fs::read_to_string(filename).await {
                Ok(contents) => {
                    location_overrides
                        = serde_json::from_str::<Vec<OverriddenLocation>>(&contents)?;
                }
                Err(x) => {
                    println!("WARNING: Failed to load location overrides: {}", x);
                }
            },
        }
        println!("[{}] Overriding locations", namespace);
        for location in location_overrides {
            let old_location = location::Entity::load()
                .filter(location::COLUMN.id.eq(location.id.clone()))
                .filter(location::COLUMN.namespace.eq(namespace))
                .one(transaction)
                .await?;
            let location: location::ActiveModelEx = location::ActiveModelEx {
                id: ActiveValue::Set(location.id),
                namespace: ActiveValue::Set(namespace.to_string()),
                name: ActiveValue::Set(location.name),
                public_id: ActiveValue::Set(location.public_id),
                timezone: ActiveValue::Set(location.timezone),
                ..Default::default()
            };
            match old_location {
                Some(_) => location.update(transaction).await?,
                None => location.insert(transaction).await?,
            };
        }

        Ok(())
    }

    async fn read_record(
        &mut self,
        line: String,
        schedule: &schedule::ModelEx,
        number: u64,
        transaction: &DatabaseTransaction,
    ) -> Result<(usize, usize, usize, usize, usize), Error> {
        if line.is_empty() {
            return Ok((0, 0, 0, 0, 0));
        }
        if line.len() != 80 {
            return Err(CifError {
                error_type: CifErrorType::InvalidRecordLength(line.len()),
                line: number,
                column: 0,
            }.into());
        }

        let namespace = &schedule.namespace;

        match &line[..2] {
            "HD" => {
                self.read_header(&line, schedule, number, transaction).await?;
                Ok((0, 0, 0, 0, 0))
            },
            "TI" => {
                Ok((self
                    .read_tiploc(&line, namespace, number, ModificationType::Insert, transaction)
                    .await?,
                    0, 0, 0, 0))
            },
            "TA" => {
                Ok((self
                    .read_tiploc(&line, namespace, number, ModificationType::Amend, transaction)
                    .await?,
                    0, 0, 0, 0))
            },
            "TD" => {
                Ok((self
                    .read_tiploc(&line, namespace, number, ModificationType::Delete, transaction)
                    .await?,
                    0, 0, 0, 0))
            },
            "AA" => {
                self.read_association(&line, namespace, number, transaction).await?;
                Ok((0, 0, 0, 0, 0))
            },
            "BS" => {
                let (train_count, line_count, allocation_count)
                    = self.read_basic_schedule(&line, namespace, number, transaction).await?;
                Ok((0, line_count, allocation_count, 0, train_count))
            },
            "BX" => Ok((
                0,
                0,
                0,
                self.read_extended_schedule(&line, namespace, number, transaction).await?,
                0
            )),
            "LO" => {
                self.read_location_origin(&line, namespace, number, transaction).await?;
                Ok((0, 0, 0, 0, 0))
            },
            "LI" => {
                self.read_location_intermediate(&line, namespace, number, transaction).await?;
                Ok((0, 0, 0, 0, 0))
            },
            "LT" => {
                self.read_location_terminating(&line, namespace, number, transaction).await?;
                Ok((0, 0, 0, 0, 0))
            },
            "CR" => {
                let (line_count, allocation_count)
                    = self.read_change_en_route(&line, namespace, number, transaction).await?;
                Ok((0, line_count, allocation_count, 0, 0))
            },
            "ZZ" => Ok((0, 0, 0, 0, self.finalise(&line, namespace, number, transaction).await?)),
            x => Err(CifError {
                error_type: CifErrorType::InvalidRecordType(x.to_string()),
                line: number,
                column: 0,
            }.into()),
        }
    }
}

#[async_trait]
impl SlowStreamingImporter for CifImporter {
    async fn overlay(
        &mut self,
        reader: impl AsyncBufReadExt + Unpin + Send,
        schedule: &schedule::ModelEx,
        transaction: &DatabaseTransaction,
    ) -> Result<(), Error> {
        let mut lines = reader.lines();

        let mut i: u64 = 0;

        let namespace = schedule.namespace.clone();

        let mut location_count = 0;
        let mut line_count = 0;
        let mut allocation_count = 0;
        let mut operator_count = 0;
        let mut train_count = 0;

        println!("[{}] Populating cache...", namespace);
        self.populate_train_variant_cache(&namespace, transaction).await?;
        println!("[{}] Populated cache...", namespace);

        println!("[{}] Loading records...", namespace);

        while let Some(line) = lines.next_line().await? {
            i += 1;
            let (new_locations, new_lines, new_allocations, new_operators, new_trains)
                = self.read_record(line, schedule, i, transaction).await?;
            location_count += new_locations;
            line_count += new_lines;
            allocation_count += new_allocations;
            operator_count += new_operators;
            train_count += new_trains;
            if i % 1000 == 0 {
                println!(
                    "[{}] After {} lines, \
                    persisted {} locations, {} lines, {} operators, {} allocations, {} trains",
                    namespace,
                    i,
                    location_count,
                    line_count,
                    operator_count,
                    allocation_count,
                    train_count,
                );
            }
        }

        self.cached_allocation_ids.clear();
        self.cached_line_ids.clear();
        self.cached_operator_ids.clear();

        println!(
            "[{}] Persisted {} locations, {} lines, {} operators, {} allocations, {} trains",
            namespace,
            location_count,
            line_count,
            operator_count,
            allocation_count,
            train_count,
        );

        self.override_locations(&namespace, transaction).await?;

        println!(
            "[{}] Successfully loaded trains from {} lines of CIF",
            namespace,
            i
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
struct NrJsonSender {
    organisation: String,
    application: String,
    component: String,
    #[serde(rename = "userID")]
    user_id: Option<String>,
    #[serde(rename = "sessionID")]
    session_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
struct NrJsonTiploc {
    tiploc_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
struct NrJsonLocation {
    tiploc: NrJsonTiploc,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
struct NrJsonScheduleLocation {
    scheduled_arrival_time: Option<String>,
    scheduled_departure_time: Option<String>,
    scheduled_pass_time: Option<String>,
    public_arrival_time: Option<String>,
    public_departure_time: Option<String>,
    #[serde(rename = "CIF_platform")]
    cif_platform: Option<String>,
    #[serde(rename = "CIF_line")]
    cif_line: Option<String>,
    #[serde(rename = "CIF_path")]
    cif_path: Option<String>,
    #[serde(rename = "CIF_activity")]
    cif_activity: Option<String>,
    #[serde(rename = "CIF_engineering_allowance")]
    cif_engineering_allowance: Option<String>,
    #[serde(rename = "CIF_pathing_allowance")]
    cif_pathing_allowance: Option<String>,
    #[serde(rename = "CIF_performance_allowance")]
    cif_performance_allowance: Option<String>,
    location: NrJsonLocation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
struct NrJsonScheduleSegment {
    signalling_id: String,
    uic_code: Option<String>,
    atoc_code: Option<String>,
    #[serde(rename = "CIF_train_category")]
    cif_train_category: String,
    #[serde(rename = "CIF_headcode")]
    cif_headcode: Option<String>,
    #[serde(rename = "CIF_course_indicator")]
    cif_course_indicator: Option<String>,
    #[serde(rename = "CIF_train_service_code")]
    cif_train_service_code: Option<String>,
    #[serde(rename = "CIF_business_sector")]
    cif_business_sector: Option<String>,
    #[serde(rename = "CIF_power_type")]
    cif_power_type: Option<String>,
    #[serde(rename = "CIF_timing_load")]
    cif_timing_load: Option<String>,
    #[serde(rename = "CIF_speed")]
    cif_speed: Option<String>,
    #[serde(rename = "CIF_operating_characteristics")]
    cif_operating_characteristics: Option<String>,
    #[serde(rename = "CIF_train_class")]
    cif_train_class: Option<String>,
    #[serde(rename = "CIF_sleepers")]
    cif_sleepers: Option<String>,
    #[serde(rename = "CIF_reservations")]
    cif_reservations: Option<String>,
    #[serde(rename = "CIF_connection_indicator")]
    cif_connection_indicator: Option<String>,
    #[serde(rename = "CIF_catering_code")]
    cif_catering_code: Option<String>,
    #[serde(rename = "CIF_service_branding")]
    cif_service_branding: Option<String>,
    #[serde(rename = "CIF_traction_class")]
    cif_traction_class: Option<String>,
    schedule_location: Vec<NrJsonScheduleLocation>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
struct NrJsonSchedule {
    schedule_id: Option<String>,
    transaction_type: String,
    schedule_start_date: String,
    schedule_end_date: String,
    schedule_days_runs: String,
    applicable_timetable: Option<String>,
    #[serde(rename = "CIF_bank_holiday_running")]
    cif_bank_holiday_running: Option<String>,
    #[serde(rename = "CIF_train_uid")]
    cif_train_uid: String,
    train_status: String,
    #[serde(rename = "CIF_stp_indicator")]
    cif_stp_indicator: String,
    schedule_segment: Option<Vec<NrJsonScheduleSegment>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
struct NrJsonVstpCifMsgV1 {
    #[serde(rename = "schemaLocation")]
    schema_location: Option<String>,
    classification: String,
    timestamp: String,
    owner: String,
    #[serde(rename = "originMsgId")]
    origin_msg_id: String,
    #[serde(rename = "Sender")]
    sender: NrJsonSender,
    schedule: NrJsonSchedule,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NrJsonVstp {
    #[serde(rename = "VSTPCIFMsgV1")]
    vstp_cif_msg_v1: NrJsonVstpCifMsgV1,
}

pub struct NrJsonImporter {
}

impl NrJsonImporter {
    pub async fn new() -> Result<NrJsonImporter, Error> {
        Ok(NrJsonImporter {})
    }

    async fn read_vstp_route(
        &self,
        schedule_segments: &Vec<NrJsonScheduleSegment>,
        train_status: &TrainStatus,
        namespace: &str,
        transaction: &DatabaseTransaction,
    ) -> Result<Vec<train_location::ActiveModelEx>, Error> {
        let mut route = vec![];
        for (i, segment) in schedule_segments.iter().enumerate() {
            if segment.schedule_location.len() == 0 {
                return Err(NrJsonError {
                    error_type: CifErrorType::NotEnoughLocations,
                    field_name: "schedule_location".to_string(),
                }.into());
            }
            for (j, location) in segment.schedule_location.iter().enumerate() {
                // don't populate a change en route on the first segment as this
                // will be populated quite happily in the main train's variable_train field.
                let change_en_route = if i == 0 || j != 0 {
                    ActiveHasOne::NotSet
                } else {
                    ActiveHasOne::Set(Some(Box::new(self.read_vstp_variable_train(
                        segment,
                        train_status,
                        namespace,
                        transaction,
                    ).await?)))
                };

                let is_origin = if i == 0 && j == 0 { true } else { false };

                let is_destination = if i == schedule_segments.len() - 1
                    && j == segment.schedule_location.len() - 1
                {
                    true
                } else {
                    false
                };

                if is_origin && is_destination {
                    return Err(NrJsonError {
                        error_type: CifErrorType::NotEnoughLocations,
                        field_name: "schedule_location".to_string(),
                    }.into());
                }

                let (last_wtt_time, last_wtt_day) = match is_origin {
                    true => (None, None),
                    false => match get_working_time(route.last().unwrap()) {
                        (x, y) => (Some(x), Some(y)),
                    },
                };

                let location_id = &location.location.tiploc.tiploc_id;
                let location_suffix = None; // doesn't appear to be in VSTP

                let wtt_arr = read_vstp_time(
                    &location.scheduled_arrival_time,
                    produce_nr_json_error_closure("scheduled_arrival_time".to_string()),
                )?;
                let wtt_arr_day = match (&last_wtt_time, &wtt_arr) {
                    (Some(x), y) => calculate_day(y, x, last_wtt_day.unwrap()),
                    (None, Some(_)) => Some(0),
                    _ => None,
                };

                let wtt_dep = read_vstp_time(
                    &location.scheduled_departure_time,
                    produce_nr_json_error_closure("scheduled_departure_time".to_string()),
                )?;
                let wtt_dep_day = match (&last_wtt_time, &wtt_dep) {
                    (Some(x), y) => calculate_day(y, x, last_wtt_day.unwrap()),
                    (None, Some(_)) => Some(0),
                    _ => None,
                };

                let wtt_pass = read_vstp_time(
                    &location.scheduled_pass_time,
                    produce_nr_json_error_closure("scheduled_pass_time".to_string()),
                )?;
                let wtt_pass_day = match (&last_wtt_time, &wtt_pass) {
                    (Some(x), y) => calculate_day(y, x, last_wtt_day.unwrap()),
                    (None, Some(_)) => Some(0),
                    _ => None,
                };

                match (wtt_arr, wtt_dep, wtt_pass, is_origin, is_destination) {
                    (None, None, Some(_), false, false) => (),
                    (Some(_), Some(_), None, false, false) => (),
                    (Some(_), None, None, false, true) => (),
                    (None, Some(_), None, true, false) => (),
                    (_, _, _, _, _) => {
                        return Err(NrJsonError {
                            error_type: CifErrorType::InvalidWttTimesCombo,
                            field_name: "scheduled_*_time".to_string(),
                        }.into())
                    }
                };

                let pub_arr = read_vstp_time(
                    &location.public_arrival_time,
                    produce_nr_json_error_closure("public_arrival_time".to_string()),
                )?;
                // TODO maybe should change this to calculate based on last public time?
                let pub_arr_day = match (&last_wtt_time, &pub_arr) {
                    (Some(x), y) => calculate_day(y, x, last_wtt_day.unwrap()),
                    (None, Some(_)) => Some(0),
                    _ => None,
                };

                let pub_dep = read_vstp_time(
                    &location.public_departure_time,
                    produce_nr_json_error_closure("public_departure_time".to_string()),
                )?;
                let pub_dep_day = match (&last_wtt_time, &pub_dep) {
                    (Some(x), y) => calculate_day(y, x, last_wtt_day.unwrap()),
                    (None, Some(_)) => Some(0),
                    _ => None,
                };

                let platform = match &location.cif_platform {
                    Some(x) => read_optional_string(&x.trim()),
                    None => None,
                };
                let line_code = match &location.cif_line {
                    Some(x) => read_optional_string(&x.trim()),
                    None => None,
                };
                let path_code = match &location.cif_path {
                    Some(x) => read_optional_string(&x.trim()),
                    None => None,
                };

                let activities = match &location.cif_activity {
                    Some(x) => Some(read_activities(
                        format!("{: <12}", x).as_str(),
                        produce_nr_json_error_closure("CIF_activity".to_string()),
                    )?),
                    None => None,
                };

                let eng_allowance = match &location.cif_engineering_allowance {
                    Some(x) => Some(read_allowance(
                        format!("{: <2}", x).as_str(),
                        produce_nr_json_error_closure("CIF_engineering_allowance".to_string()),
                    )?),
                    None => None,
                };
                let path_allowance = match &location.cif_pathing_allowance {
                    Some(x) => Some(read_allowance(
                        format!("{: <2}", x).as_str(),
                        produce_nr_json_error_closure("CIF_pathing_allowance".to_string()),
                    )?),
                    None => None,
                };
                let perf_allowance = match &location.cif_performance_allowance {
                    Some(x) => Some(read_allowance(
                        format!("{: <2}", x).as_str(),
                        produce_nr_json_error_closure("CIF_performance_allowance".to_string()),
                    )?),
                    None => None,
                };

                let mut new_location = train_location::ActiveModelEx {
                    namespace: ActiveValue::Set(namespace.to_string()),
                    index: ActiveValue::Set(j.try_into().unwrap()),
                    timing_tz: ActiveValue::Set(None),
                    location_id: ActiveValue::Set(location_id.to_string()),
                    id_suffix: ActiveValue::Set(location_suffix),
                    working_arr: ActiveValue::Set(wtt_arr),
                    working_arr_day: ActiveValue::Set(wtt_arr_day),
                    working_dep: ActiveValue::Set(wtt_dep),
                    working_dep_day: ActiveValue::Set(wtt_dep_day),
                    working_pass: ActiveValue::Set(wtt_pass),
                    working_pass_day: ActiveValue::Set(wtt_pass_day),
                    public_arr: ActiveValue::Set(pub_arr),
                    public_arr_day: ActiveValue::Set(pub_arr_day),
                    public_dep: ActiveValue::Set(pub_dep),
                    public_dep_day: ActiveValue::Set(pub_dep_day),
                    platform: ActiveValue::Set(platform),
                    platform_zone: ActiveValue::Set(None),
                    line: ActiveValue::Set(line_code),
                    path: ActiveValue::Set(path_code),
                    engineering_allowance_s: ActiveValue::Set(eng_allowance),
                    pathing_allowance_s: ActiveValue::Set(path_allowance),
                    performance_allowance_s: ActiveValue::Set(perf_allowance),
                    change_en_route,
                    ..Default::default()
                };
                match &activities {
                    Some(activities) => new_location.populate_activities(activities),
                    None => new_location.populate_activities(
                        &train_location::Activities { ..Default::default() }
                    ),
                };

                route.push(new_location);
            }
        }
        Ok(route)
    }

    async fn read_vstp_variable_train(
        &self,
        schedule_segment: &NrJsonScheduleSegment,
        train_status: &TrainStatus,
        namespace: &str,
        transaction: &DatabaseTransaction,
    ) -> Result<variable_train::ActiveModelEx, Error> {
        let train_type = match read_train_type(
            &schedule_segment.cif_train_category,
            produce_nr_json_error_closure("CIF_train_category".to_string()),
        )? {
            Some(x) => x,
            None => match train_status {
                TrainStatus::Bus => TrainType::Bus,
                TrainStatus::Freight => TrainType::Freight,
                TrainStatus::PassengerParcels => TrainType::PassengerParcels,
                TrainStatus::Ship => TrainType::Ship,
                TrainStatus::Trip => TrainType::Trip,
                TrainStatus::StpPassengerParcels => TrainType::PassengerParcels,
                TrainStatus::StpFreight => TrainType::Freight,
                TrainStatus::StpTrip => TrainType::Trip,
                TrainStatus::StpShip => TrainType::Ship,
                TrainStatus::StpBus => TrainType::Bus,
                TrainStatus::VstpNone => TrainType::Trip,
            },
        };

        let public_id = &schedule_segment.signalling_id;

        let headcode = match &schedule_segment.cif_headcode {
            Some(x) => read_optional_string(x),
            None => None,
        };
        let service_group = &schedule_segment.cif_train_service_code;

        let power_type = match (
            &schedule_segment.cif_power_type,
            &schedule_segment.cif_timing_load,
        ) {
            (None, _) => None,
            (Some(x), None) => read_power_type(
                x,
                "",
                produce_nr_json_error_closure("CIF_power_type or CIF_timing_load".to_string()),
            )?,
            (Some(x), Some(y)) => read_power_type(
                x,
                y,
                produce_nr_json_error_closure("CIF_power_type or CIF_timing_load".to_string()),
            )?,
        };
        let speed_m_per_s = match schedule_segment.cif_speed.as_deref() {
            Some("022") => Some(22. * (1609.344 / (60. * 60.)) * (1609.344 / (60. * 60.))),
            Some("034") => Some(34. * (1609.344 / (60. * 60.)) * (1609.344 / (60. * 60.))),
            Some("056") => Some(56. * (1609.344 / (60. * 60.)) * (1609.344 / (60. * 60.))),
            Some("067") => Some(67. * (1609.344 / (60. * 60.)) * (1609.344 / (60. * 60.))),
            Some("078") => Some(78. * (1609.344 / (60. * 60.)) * (1609.344 / (60. * 60.))),
            Some("089") => Some(89. * (1609.344 / (60. * 60.)) * (1609.344 / (60. * 60.))),
            Some("101") => Some(101. * (1609.344 / (60. * 60.)) * (1609.344 / (60. * 60.))),
            Some("112") => Some(112. * (1609.344 / (60. * 60.)) * (1609.344 / (60. * 60.))),
            Some("123") => Some(123. * (1609.344 / (60. * 60.)) * (1609.344 / (60. * 60.))),
            Some("134") => Some(134. * (1609.344 / (60. * 60.)) * (1609.344 / (60. * 60.))),
            Some("157") => Some(157. * (1609.344 / (60. * 60.)) * (1609.344 / (60. * 60.))),
            Some("168") => Some(168. * (1609.344 / (60. * 60.)) * (1609.344 / (60. * 60.))),
            Some("179") => Some(179. * (1609.344 / (60. * 60.)) * (1609.344 / (60. * 60.))),
            Some("195") => Some(195. * (1609.344 / (60. * 60.)) * (1609.344 / (60. * 60.))),
            Some("201") => Some(201. * (1609.344 / (60. * 60.)) * (1609.344 / (60. * 60.))),
            Some("213") => Some(213. * (1609.344 / (60. * 60.)) * (1609.344 / (60. * 60.))),
            Some("224") => Some(224. * (1609.344 / (60. * 60.)) * (1609.344 / (60. * 60.))),
            Some("246") => Some(246. * (1609.344 / (60. * 60.)) * (1609.344 / (60. * 60.))),
            Some("280") => Some(280. * (1609.344 / (60. * 60.)) * (1609.344 / (60. * 60.))),
            Some("314") => Some(314. * (1609.344 / (60. * 60.)) * (1609.344 / (60. * 60.))),
            Some("417") => Some(417. * (1609.344 / (60. * 60.)) * (1609.344 / (60. * 60.))),
            Some(x) => read_speed(x, produce_nr_json_error_closure("CIF_speed".to_string()))?,
            None => None,
        };

        let (operating_characteristics, _) = match &schedule_segment.cif_operating_characteristics {
            Some(x) => {
                let (operating_characteristics, runs_as_required) = read_operating_characteristics(
                    x,
                    produce_nr_json_error_closure("CIF_operating_characteristics".to_string()),
                )?;
                (Some(operating_characteristics), runs_as_required)
            },
            None => (None, false),
        };

        let timing_load_str = match (
            &schedule_segment.cif_power_type,
            &schedule_segment.cif_timing_load,
        ) {
            (None, _) => None,
            (Some(x), None) => read_timing_load(
                x,
                "",
                operating_characteristics
                .as_ref()
                .map(|x| x.br_mark_four_coaches)
                .unwrap_or(false),
                produce_nr_json_error_closure("CIF_power_type or CIF_timing_load".to_string()),
            )?,
            (Some(x), Some(y)) => read_timing_load(
                x,
                y,
                operating_characteristics
                .as_ref()
                .map(|x| x.br_mark_four_coaches)
                .unwrap_or(false),
                produce_nr_json_error_closure("CIF_power_type or CIF_timing_load".to_string()),
            )?,
        };
        let timing_load_id = match (
            &schedule_segment.cif_power_type,
            &schedule_segment.cif_timing_load,
        ) {
            (None, None) => "       ".to_string(),
            (None, Some(x)) => format!("   {: <4}", x),
            (Some(x), None) => format!("{: <3}    ", x),
            (Some(x), Some(y)) => format!("{: <3}{: <4}", x, y),
        } + if operating_characteristics
            .as_ref()
            .map(|x| x.br_mark_four_coaches)
            .unwrap_or(false) { "1" }
            else { "0" };

        let (first_seating, standard_seating) = match &schedule_segment.cif_train_class {
            Some(x) => read_seating_class(
                x,
                train_type,
                produce_nr_json_error_closure("CIF_train_class".to_string()),
            )?,
            None => read_seating_class(
                "",
                train_type,
                produce_nr_json_error_closure("CIF_train_class".to_string()),
            )?,
        };
        let (first_sleepers, standard_sleepers) = match &schedule_segment.cif_sleepers {
            Some(x) => read_sleeper_class(
                x,
                produce_nr_json_error_closure("CIF_train_class".to_string()),
            )?,
            None => (false, false),
        };

        let (catering, wheelchair_reservations) = match &schedule_segment.cif_catering_code {
            Some(x) => {
                let (catering, wheelchair_reservations) = read_catering(
                    x,
                    produce_nr_json_error_closure("CIF_catering_code".to_string()),
                )?;
                (Some(catering), wheelchair_reservations)
            },
            None => (None, false),
        };

        let reservations_str = match &schedule_segment.cif_reservations {
            Some(x) => x,
            None => "",
        };
        let reservations = read_reservations(
            reservations_str,
            wheelchair_reservations,
            first_seating,
            standard_seating,
            first_sleepers,
            standard_sleepers,
            train_type,
            produce_nr_json_error_closure("CIF_reservations".to_string()),
        )?;

        let brand = match &schedule_segment.cif_service_branding {
            Some(x) => read_brand(
                x,
                produce_nr_json_error_closure("CIF_service_branding".to_string()),
            )?,
            None => None,
        };

        // now we also have the data from BX records to load

        let uic_code = match &schedule_segment.uic_code {
            Some(x) => read_optional_string(x),
            None => None,
        };

        let atoc_code = match &schedule_segment.atoc_code {
            Some(x) => x,
            None => "ZZ",
        };

        let train_operator_desc = read_train_operator(
            atoc_code,
            produce_nr_json_error_closure("atoc_code".to_string()),
        )?;

        // From this point on we will need to ensure that the line, allocation, and operator are in
        // the database, so fill them in here
        match &timing_load_str {
            Some(timing_load_str) => {
                let timing_load = train_allocation::Entity::load()
                    .filter(train_allocation::COLUMN.id.eq(&timing_load_id))
                    .filter(train_allocation::COLUMN.namespace.eq(namespace))
                    .one(transaction)
                    .await?;
                if timing_load.is_none() {
                    let timing_load = train_allocation::ActiveModelEx {
                        id: ActiveValue::Set(timing_load_id.clone()),
                        namespace: ActiveValue::Set(namespace.to_string()),
                        description: ActiveValue::Set(timing_load_str.clone()),
                        vehicles: ActiveHasMany::Append(vec![]),
                        ..Default::default()
                    };

                    timing_load.insert(transaction).await?;
                }
            }
            None => (),
        };

        match &service_group {
            Some(service_group) => {
                let line = line::Entity::load()
                    .filter(line::COLUMN.id.eq(service_group))
                    .filter(line::COLUMN.namespace.eq(namespace))
                    .one(transaction)
                    .await?;
                if line.is_none() {
                    let line = line::ActiveModelEx {
                        id: ActiveValue::Set(service_group.clone()),
                        namespace: ActiveValue::Set(namespace.to_string()),
                        public_id: ActiveValue::Set(None),
                        name: ActiveValue::Set(None),
                        description: ActiveValue::Set(None),
                        url: ActiveValue::Set(None),
                        background_colour: ActiveValue::Set(None),
                        foreground_colour: ActiveValue::Set(None),
                        ..Default::default()
                    };

                    line.insert(transaction).await?;
                }
            },
            None => (),
        };

        let operator = train_operator::Entity::load()
            .filter(train_operator::COLUMN.id.eq(atoc_code))
            .filter(train_operator::COLUMN.namespace.eq(namespace))
            .one(transaction)
            .await?;
        if operator.is_none() {
            let operator = train_operator::ActiveModelEx {
                id: ActiveValue::Set(atoc_code.to_string()),
                namespace: ActiveValue::Set(namespace.to_string()),
                public_id: ActiveValue::Set(None),
                description: ActiveValue::Set(train_operator_desc),
                ..Default::default()
            };

            operator.insert(transaction).await?;
        }

        let mut variable_train = variable_train::ActiveModelEx {
            namespace: ActiveValue::Set(namespace.to_string()),
            train_type: ActiveValue::Set(train_type),
            public_id: ActiveValue::Set(Some(public_id.to_string())),
            headcode: ActiveValue::Set(headcode),
            power_type: ActiveValue::Set(power_type),
            timing_allocation_id: ActiveValue::Set(match timing_load_str {
                None => None,
                Some(_) => Some(timing_load_id),
            }),
            actual_allocation_id: ActiveValue::Set(None),
            timing_speed_m_per_s: ActiveValue::Set(speed_m_per_s),
            accommodation: ActiveHasMany::Append(vec![
                accommodation_types::ActiveModelEx {
                    class: ActiveValue::Set(AccommodationClass::First),
                    standing: ActiveValue::Set(None),
                    seating: ActiveValue::Set(Some(first_seating)),
                    reclining_seating: ActiveValue::Set(None),
                    special_seating: ActiveValue::Set(None),
                    sleeper: ActiveValue::Set(Some(first_sleepers)),
                    single_sleeper: ActiveValue::Set(None),
                    double_sleeper: ActiveValue::Set(None),
                    special_sleeper: ActiveValue::Set(None),
                    couchette: ActiveValue::Set(None),
                    single_couchette: ActiveValue::Set(None),
                    double_couchette: ActiveValue::Set(None),
                    baby: ActiveValue::Set(None),
                    family: ActiveValue::Set(None),
                    recreation: ActiveValue::Set(None),
                    panoramic: ActiveValue::Set(None),
                    pullman: ActiveValue::Set(None),
                    pushchair: ActiveValue::Set(None),
                    wheelchair: ActiveValue::Set(None),
                    has_female_only: ActiveValue::Set(None),
                    has_male_only: ActiveValue::Set(None),
                    has_same_sex_only: ActiveValue::Set(None),
                    ..Default::default()
                },
                accommodation_types::ActiveModelEx {
                    class: ActiveValue::Set(AccommodationClass::Second),
                    standing: ActiveValue::Set(None),
                    seating: ActiveValue::Set(Some(standard_seating)),
                    reclining_seating: ActiveValue::Set(None),
                    special_seating: ActiveValue::Set(None),
                    sleeper: ActiveValue::Set(Some(standard_sleepers)),
                    single_sleeper: ActiveValue::Set(None),
                    double_sleeper: ActiveValue::Set(None),
                    special_sleeper: ActiveValue::Set(None),
                    couchette: ActiveValue::Set(None),
                    single_couchette: ActiveValue::Set(None),
                    double_couchette: ActiveValue::Set(None),
                    baby: ActiveValue::Set(None),
                    family: ActiveValue::Set(None),
                    recreation: ActiveValue::Set(None),
                    panoramic: ActiveValue::Set(None),
                    pullman: ActiveValue::Set(None),
                    pushchair: ActiveValue::Set(None),
                    wheelchair: ActiveValue::Set(None),
                    has_female_only: ActiveValue::Set(None),
                    has_male_only: ActiveValue::Set(None),
                    has_same_sex_only: ActiveValue::Set(None),
                    ..Default::default()
                },
            ]),
            brand: ActiveValue::Set(brand),
            name: ActiveValue::Set(None),
            line_id: ActiveValue::Set(service_group.clone()),
            uic_code: ActiveValue::Set(uic_code),
            operator_id: ActiveValue::Set(Some(atoc_code.to_string())),
            wheelchair_accessible: ActiveValue::Set(None),
            has_toilets: ActiveValue::Set(false),
            has_luggage: ActiveValue::Set(false),
            has_families: ActiveValue::Set(false),
            has_passenger_communications: ActiveValue::Set(false),
            has_assistance: ActiveValue::Set(false),
            has_passenger_information: ActiveValue::Set(false),
            ..Default::default()
        };
        variable_train.populate_reservations(&reservations);
        match &catering {
            Some(catering) => variable_train.populate_catering(catering),
            None => variable_train.has_catering = ActiveValue::Set(false),
        };
        match &operating_characteristics {
            Some(x) => variable_train.populate_operating_characteristics(x),
            None => variable_train.has_operating_characteristics = ActiveValue::Set(false),
        };
        Ok(variable_train)
    }

    async fn read_vstp_entry(
        &self,
        parsed_json: &NrJsonVstp,
        schedule: &schedule::ModelEx,
        transaction: &DatabaseTransaction,
    ) -> Result<bool, Error> {
        let namespace = &schedule.namespace;
        let modification_type = match parsed_json
            .vstp_cif_msg_v1
            .schedule
            .transaction_type
            .as_str()
        {
            "Create" => ModificationType::Insert,
            "Delete" => ModificationType::Delete,
            "Update" => ModificationType::Amend,
            x => {
                return Err(NrJsonError {
                    error_type: CifErrorType::InvalidTransactionType(x.to_string()),
                    field_name: "transaction_type".to_string(),
                }.into())
            }
        };
        let (stp_modification_type, is_stp) = read_stp_indicator(
            parsed_json
                .vstp_cif_msg_v1
                .schedule
                .cif_stp_indicator
                .as_str(),
            produce_nr_json_error_closure("CIF_stp_indicator".to_string()),
        )?;

        let main_train_id = parsed_json.vstp_cif_msg_v1.schedule.cif_train_uid.trim();
        let begin = read_vstp_date(
            &parsed_json.vstp_cif_msg_v1.schedule.schedule_start_date,
            produce_nr_json_error_closure("schedule_start_date".to_string()),
        )?;

        // check that our schedule is the correct one
        if begin > *schedule.valid_end.as_ref().unwrap() {
            println!(
                "{} is later than {}, skipping...",
                begin,
                schedule.valid_end.as_ref().unwrap()
            );
            return Ok(false);
        }

        // At this stage we have all the data we need for a simple delete, so handle this here
        //
        // Note these are NOT the same as STP cancels and indeed handled completely differently
        if modification_type == ModificationType::Delete {
            let train_variants = get_all_train_variants_for_delete_without_cache(
                main_train_id,
                namespace,
                transaction
            ).await?;

            match stp_modification_type {
                ModificationType::Insert => {
                    let train_variant_ids_to_delete: HashSet<i64> = train_variants
                        .into_iter()
                        .filter(|train_variant|
                            train_variant.train_id.is_some()
                            && train_variant.validity[0].valid_begin == begin
                            && ((is_stp && train_variant.source.unwrap() != TrainSource::LongTerm)
                                ||
                                (!is_stp && train_variant.source.unwrap() == TrainSource::LongTerm)
                            )
                        )
                        .map(|x| x.id)
                        .collect();
                    train_variant::Entity::delete_many()
                        .filter(train_variant::COLUMN.id.is_in(train_variant_ids_to_delete.clone()))
                        .exec(transaction)
                        .await?;
                },
                ModificationType::Amend => {
                    let train_variant_ids_to_delete: HashSet<i64> = train_variants
                        .into_iter()
                        .filter(|train_variant|
                            train_variant.train_id.is_none()
                            && train_variant.validity[0].valid_begin == begin
                        )
                        .map(|x| x.id)
                        .collect();
                    train_variant::Entity::delete_many()
                        .filter(train_variant::COLUMN.id.is_in(train_variant_ids_to_delete.clone()))
                        .exec(transaction)
                        .await?;
                },
                ModificationType::Delete => {
                    let cancellation_ids_to_delete: Vec<i64> = train_variants
                        .into_iter()
                        .flat_map(|train_variant|
                            train_variant.cancellations.clone().into_iter().filter(|cancellation|
                                cancellation.validity[0].valid_begin == begin
                            ).collect::<Vec<train_cancellation::ModelEx>>()).map(|x| x.id)
                        .collect();
                    train_cancellation::Entity::delete_many()
                        .filter(train_cancellation::COLUMN.id.is_in(cancellation_ids_to_delete))
                        .exec(transaction)
                        .await?;
                },
            };

            println!("Successfully deleted train {}", main_train_id);
            return Ok(true);
        }

        let end = read_vstp_date(
            &parsed_json.vstp_cif_msg_v1.schedule.schedule_end_date,
            produce_nr_json_error_closure("schedule_end_date".to_string()),
        )?;

        // check that our schedule is the correct one
        if end < *schedule.valid_begin.as_ref().unwrap() {
            println!(
                "{} is earlier than {}, skipping...",
                begin,
                schedule.valid_end.as_ref().unwrap()
            );
            return Ok(false);
        }

        let days_of_week = read_days_of_week(
            &parsed_json.vstp_cif_msg_v1.schedule.schedule_days_runs,
            produce_nr_json_error_closure("schedule_days_runs".to_string()),
        )?;

        // Now we handle STP cancellations; these are where long-running
        // trains are deleted as a one-off
        if stp_modification_type == ModificationType::Delete
            && modification_type == ModificationType::Insert
        {
            let train_variants = get_all_train_variants_for_cancel_without_cache(
                main_train_id,
                namespace,
                transaction
            ).await?;
            for train_variant in &train_variants {
                if !check_date_applicability(
                    &train_variant.validity[0].clone().into(), begin, end, &days_of_week
                ) {
                    continue;
                }
                let mut validity = train_validity_period::ActiveModelEx {
                    valid_begin: ActiveValue::Set(begin),
                    valid_end: ActiveValue::Set(end),
                    timezone: ActiveValue::Set(London.name().to_string()),
                    ..Default::default()
                };
                validity.populate_days_of_week(&days_of_week);
                let new_cancel = train_cancellation::ActiveModelEx {
                    validity: ActiveHasMany::Append(vec![validity]),
                    source: ActiveValue::Set(Some(TrainSource::VeryShortTerm)),
                    ..Default::default()
                };
                let mut train_variant: train_variant::ActiveModelEx = train_variant.clone().into();
                train_variant
                    .cancellations
                    .push(new_cancel);
                train_variant.save(transaction).await?;
            }

            println!("Successfully cancelled train {}", main_train_id);
            return Ok(true);
        }

        if modification_type == ModificationType::Amend
            && stp_modification_type == ModificationType::Delete
        {
            let train_variants = get_all_train_variants_for_amend_cancel_without_cache(
                main_train_id,
                namespace,
                transaction
            ).await?;
            for train_variant in &train_variants {
                for cancellation in train_variant.cancellations.iter() {
                    if cancellation.validity[0].valid_begin == begin {
                        let mut validity = train_validity_period::ActiveModelEx {
                            valid_begin: ActiveValue::Set(begin),
                            valid_end: ActiveValue::Set(end),
                            timezone: ActiveValue::Set(London.name().to_string()),
                            ..Default::default()
                        };
                        validity.populate_days_of_week(&days_of_week);

                        let mut cancellation: train_cancellation::ActiveModelEx
                            = cancellation.clone().into();
                        cancellation.validity = ActiveHasMany::Replace(vec![validity]);
                        cancellation.save(transaction).await?;
                    }
                }
            }

            println!("Successfully updated cancellation {}", main_train_id);

            return Ok(true);
        }

        let train_status = read_train_status(
            &parsed_json.vstp_cif_msg_v1.schedule.train_status,
            produce_nr_json_error_closure("train_status".to_string()),
        )?;

        if parsed_json
            .vstp_cif_msg_v1
            .schedule
            .schedule_segment
            .is_none()
            || parsed_json
                .vstp_cif_msg_v1
                .schedule
                .schedule_segment
                .as_ref()
                .unwrap()
                .len()
                == 0
        {
            return Err(NrJsonError {
                error_type: CifErrorType::NoScheduleSegments,
                field_name: "schedule_segment".to_string(),
            }.into());
        }

        // actually in the variable train, but re-run it here to get runs as required
        let (_, runs_as_required) = match &parsed_json
            .vstp_cif_msg_v1
            .schedule
            .schedule_segment
            .as_ref()
            .unwrap()[0]
            .cif_operating_characteristics
        {
            Some(x) => read_operating_characteristics(
                x,
                produce_nr_json_error_closure("CIF_operating_characteristics".to_string()),
            )?,
            None => (
                variable_train::OperatingCharacteristics {
                    ..Default::default()
                },
                false,
            ),
        };

        let performance_monitoring =
            match &parsed_json.vstp_cif_msg_v1.schedule.applicable_timetable {
                Some(x) => Some(read_ats_code(
                    x,
                    produce_nr_json_error_closure("applicable_timetable".to_string()),
                )?),
                None => None,
            };

        // all of the below will use this so construct it now
        let mut new_validity = train_validity_period::ActiveModelEx {
                valid_begin: ActiveValue::Set(begin),
                valid_end: ActiveValue::Set(end),
                timezone: ActiveValue::Set(London.name().to_string()),
                ..Default::default()
            };
        new_validity.populate_days_of_week(&days_of_week);

        let mut new_train_variant = train_variant::ActiveModelEx {
            namespace: ActiveValue::Set(namespace.clone()),
            // Performance cost but VSTP is relatively low volume so not an issue
            validity: ActiveHasMany::Replace(vec![new_validity]),
            cancellations: ActiveHasMany::Append(vec![]),
            replacements: ActiveHasMany::Append(vec![]),
            variable_train: ActiveHasOne::Set(Some(Box::new(self.read_vstp_variable_train(
                &parsed_json
                    .vstp_cif_msg_v1
                    .schedule
                    .schedule_segment
                    .as_ref()
                    .unwrap()[0],
                &train_status,
                namespace,
                transaction,
            ).await?))),
            source: ActiveValue::Set(Some(TrainSource::VeryShortTerm)),
            runs_as_required: ActiveValue::Set(runs_as_required),
            performance_monitoring: ActiveValue::Set(performance_monitoring),
            // Performance cost but VSTP is relatively low volume so not an issue
            route: ActiveHasMany::Replace(self.read_vstp_route(
                &parsed_json
                    .vstp_cif_msg_v1
                    .schedule
                    .schedule_segment
                    .as_ref()
                    .unwrap(),
                &train_status,
                namespace,
                transaction,
            ).await?),
            ..Default::default()
        };

        if modification_type == ModificationType::Insert
            && stp_modification_type == ModificationType::Insert
        {
            let train = train::Entity::load()
                .filter(train::COLUMN.id.eq(main_train_id))
                .filter(train::COLUMN.namespace.eq(namespace))
                .one(transaction)
                .await?;
            if train.is_none() {
                // Construct it now
                let new_train = train::ActiveModelEx {
                    id: ActiveValue::Set(main_train_id.to_string()),
                    namespace: ActiveValue::Set(namespace.to_string()),
                    ..Default::default()
                };

                new_train_variant.train = ActiveBelongsTo::Set(Some(Box::new(new_train)));
            }
            else {
                new_train_variant.train_id = ActiveValue::Set(Some(main_train_id.to_string()));
            }

            let public_id
                = new_train_variant.variable_train.as_ref().unwrap().public_id.clone().unwrap();
            new_train_variant.insert(transaction).await?;
            println!(
                "[{}] VSTP Successfully written train {} ({})",
                namespace,
                main_train_id,
                public_id.unwrap(),
            );
            //println!("Output: {:#?}", new_train);

            return Ok(true);
        }

        if modification_type == ModificationType::Amend {
            // We are finding an existing train and completely replacing it in the DB
            let train_variants = get_all_train_variants_for_amend_without_cache(
                main_train_id,
                namespace,
                transaction,
            ).await?;

            for train_variant in &train_variants {
                if stp_modification_type == ModificationType::Insert
                    && ((!is_stp && train_variant.source == Some(TrainSource::LongTerm))
                        || is_stp && train_variant.source != Some(TrainSource::LongTerm))
                    && train_variant.validity[0].valid_begin == begin
                    && train_variant.train_id.is_some()
                {
                    let mut train_variant: train_variant::ActiveModelEx
                        = train_variant.clone().into();
                    amend_train(&mut train_variant, new_train_variant.clone());
                    train_variant.save(transaction).await?;
                    println!("Successfully updated train {}", main_train_id);
                }
                else if stp_modification_type == ModificationType::Amend
                    && train_variant.validity[0].valid_begin == begin
                    && train_variant.train_id.is_none()
                {
                    let mut train_variant: train_variant::ActiveModelEx
                        = train_variant.clone().into();
                    amend_train(&mut train_variant, new_train_variant.clone());
                    train_variant.save(transaction).await?;
                    println!("Successfully updated train {}", main_train_id);
                }
            }

            return Ok(true);
        }


        if stp_modification_type == ModificationType::Amend {
            let train_variants = get_all_train_variants_for_replace_without_cache(
                main_train_id,
                namespace,
                transaction,
            ).await?;

            for train_variant in &train_variants {
                // We replace main trains
                if train_variant.train_id.is_none() {
                    continue;
                }
                if !check_date_applicability(
                    &train_variant.validity[0].clone().into(), begin, end, &days_of_week
                ) {
                    continue;
                }
                new_train_variant.parent_train_variant_id
                    = ActiveValue::Set(Some(train_variant.id));
                new_train_variant.clone().insert(transaction).await?;
                println!("Successfully replaced train {}", main_train_id);
            }

            return Ok(true);
        }

        panic!("Unreachable");
    }
}

#[async_trait]
impl FastImporter for NrJsonImporter {
    async fn overlay(
        &self, data: Vec<u8>, schedule: &schedule::ModelEx, transaction: &DatabaseTransaction
    ) -> Result<(), Error> {
        let parsed_json = serde_json::from_slice::<NrJsonVstp>(&data)?;
        self.read_vstp_entry(&parsed_json, schedule, transaction).await?;

        Ok(())
    }
}
