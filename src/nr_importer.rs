use crate::error::Error;
use crate::importer::{EphemeralImporter, FastImporter, SlowImporter};
use crate::schedule::{
    Activities, AssociationNode, Catering, DaysOfWeek, Location, OperatingCharacteristics,
    ReservationField, Reservations, Schedule, Train, TrainAllocation, TrainLocation, TrainOperator,
    TrainPower, TrainSource, TrainType, TrainValidityPeriod, VariableTrain,
};

use async_trait::async_trait;
use chrono::format::ParseError;
use chrono::naive::Days;
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
use chrono_tz::Europe::London;
use chrono_tz::Tz;
use itertools::Itertools;

use serde::{Deserialize, Serialize};

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::ops::{Add, Sub};
use std::sync::{Arc, RwLock};

use tokio::fs;
use tokio::io::AsyncBufReadExt;
use tokio::sync::Mutex;

#[derive(Default)]
pub struct CifImporter {
    last_train: Option<(String, DateTime<Tz>, ModificationType, bool)>,
    unwritten_assocs:
        HashMap<(String, String, Option<String>), Vec<(AssociationNode, AssociationCategory)>>,
    change_en_route: Option<VariableTrain>,
    cr_location: Option<(String, Option<String>)>,
    orphaned_overlay_trains: HashMap<(String, DateTime<Tz>), Train>,
}

#[derive(Debug)]
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
enum AssociationCategory {
    Join,
    Divide,
    Next,
    IsJoinedToBy,
    DividesFrom,
    FormsFrom,
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

fn rev_days(days: &DaysOfWeek, day_diff: i8) -> DaysOfWeek {
    match day_diff {
        0 => days.clone(),
        -1 => DaysOfWeek {
            monday: days.tuesday,
            tuesday: days.wednesday,
            wednesday: days.thursday,
            thursday: days.friday,
            friday: days.saturday,
            saturday: days.sunday,
            sunday: days.monday,
        },
        1 => DaysOfWeek {
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

fn rev_date(date: &DateTime<Tz>, day_diff: i8) -> DateTime<Tz> {
    if day_diff < 0 {
        date.sub(Days::new(u64::try_from(-day_diff).unwrap()))
    } else {
        date.add(Days::new(u64::try_from(day_diff).unwrap()))
    }
}

fn check_date_applicability(
    existing_validity: &TrainValidityPeriod,
    existing_days: &DaysOfWeek,
    new_begin: DateTime<Tz>,
    new_end: DateTime<Tz>,
    new_days: &DaysOfWeek,
) -> bool {
    // check for no overlapping days at all
    if existing_days
        .into_iter()
        .zip(new_days.into_iter())
        .find(|(existing_day, new_day)| *existing_day && *new_day)
        .is_none()
    {
        false
    } else if new_begin > existing_validity.valid_end || new_end < existing_validity.valid_begin {
        false
    } else {
        true
    }
}

fn write_assocs_to_trains(
    trains: &mut Vec<Train>,
    train_id: &str,
    location: &str,
    location_suffix: &Option<String>,
    assocs: &Vec<(AssociationNode, AssociationCategory)>,
) {
    for ref mut train in trains.iter_mut() {
        // recurse on replacements
        write_assocs_to_trains(
            &mut train.replacements,
            &train_id,
            &location,
            &location_suffix,
            &assocs,
        );

        for ref mut train_location in train.route.iter_mut() {
            if train_location.id == location && train_location.id_suffix == *location_suffix {
                for (assoc, category) in assocs {
                    if !check_date_applicability(
                        &train.validity[0],
                        &train.days_of_week,
                        assoc.validity[0].valid_begin,
                        assoc.validity[0].valid_end,
                        &assoc.days,
                    ) {
                        continue;
                    }
                    // we now know this is applicable to this train, so add it
                    match category {
                        AssociationCategory::Join => train_location.joins_to.push(assoc.clone()),
                        AssociationCategory::Divide => {
                            train_location.divides_to_form.push(assoc.clone())
                        }
                        AssociationCategory::Next => train_location.becomes = Some(assoc.clone()),
                        AssociationCategory::IsJoinedToBy => {
                            train_location.is_joined_to_by.push(assoc.clone())
                        }
                        AssociationCategory::DividesFrom => {
                            train_location.divides_from.push(assoc.clone())
                        }
                        AssociationCategory::FormsFrom => {
                            train_location.forms_from = Some(assoc.clone())
                        }
                    };
                }
            }
        }
    }
}

fn is_matching_assoc_for_modify_insertion(
    assoc: &AssociationNode,
    other_train_id: &str,
    begin: &DateTime<Tz>,
    other_train_location_suffix: &Option<String>,
    is_stp: bool,
    use_rev: bool,
) -> bool {
    return match is_stp {
        false => assoc.source.unwrap() == TrainSource::LongTerm, // match the entire association for deleted or modified inserts
        true => assoc.source.unwrap() == TrainSource::ShortTerm,
    } && assoc.validity[0].valid_begin
        == if use_rev {
            rev_date(begin, assoc.day_diff)
        } else {
            *begin
        }
        && other_train_id == assoc.other_train_id
        && *other_train_location_suffix == assoc.other_train_location_id_suffix;
}

fn is_matching_assoc_for_modify_replacement_or_cancel(
    validity: &TrainValidityPeriod,
    begin: &DateTime<Tz>,
    day_diff: i8,
    use_rev: bool,
) -> bool {
    validity.valid_begin
        == if use_rev {
            rev_date(begin, day_diff)
        } else {
            *begin
        }
}

fn delete_single_assoc_replacements_cancellations(
    assoc: &mut AssociationNode,
    other_train_id: &str,
    begin: &DateTime<Tz>,
    other_train_location_suffix: &Option<String>,
    stp_modification_type: &ModificationType,
    use_rev: bool,
) {
    if other_train_id != assoc.other_train_id
        || *other_train_location_suffix != assoc.other_train_location_id_suffix
    {
        return;
    }
    if *stp_modification_type == ModificationType::Amend {
        assoc.replacements.retain(|assoc| {
            !is_matching_assoc_for_modify_replacement_or_cancel(
                &assoc.validity[0],
                begin,
                assoc.day_diff,
                use_rev,
            )
        });
    } else if *stp_modification_type == ModificationType::Delete {
        assoc.cancellations.retain(|(validity, _days_of_week)| {
            !is_matching_assoc_for_modify_replacement_or_cancel(
                validity,
                begin,
                assoc.day_diff,
                use_rev,
            )
        });
    }
}

fn delete_single_vec_assocs(
    assocs: &mut Vec<AssociationNode>,
    other_train_id: &str,
    begin: &DateTime<Tz>,
    other_train_location_suffix: &Option<String>,
    stp_modification_type: &ModificationType,
    is_stp: bool,
    use_rev: bool,
) {
    if *stp_modification_type == ModificationType::Insert {
        assocs.retain(|assoc| {
            !is_matching_assoc_for_modify_insertion(
                assoc,
                other_train_id,
                begin,
                other_train_location_suffix,
                is_stp,
                use_rev,
            )
        });
    } else {
        for ref mut assoc in assocs.iter_mut() {
            delete_single_assoc_replacements_cancellations(
                assoc,
                other_train_id,
                begin,
                other_train_location_suffix,
                stp_modification_type,
                use_rev,
            );
        }
    }
}

fn amend_individual_assoc(
    assoc: &mut AssociationNode,
    begin: &DateTime<Tz>,
    end: &DateTime<Tz>,
    days_of_week: &DaysOfWeek,
    day_diff: i8,
    for_passengers: bool,
) {
    assoc.validity = vec![TrainValidityPeriod {
        valid_begin: begin.clone(),
        valid_end: end.clone(),
    }];
    assoc.days = days_of_week.clone();
    assoc.day_diff = day_diff;
    assoc.for_passengers = for_passengers;
}

fn amend_single_assoc_replacements_cancellations(
    assoc: &mut AssociationNode,
    other_train_id: &str,
    begin: &DateTime<Tz>,
    end: &DateTime<Tz>,
    days_of_week: &DaysOfWeek,
    other_train_location_suffix: &Option<String>,
    stp_modification_type: &ModificationType,
    day_diff: i8,
    for_passengers: bool,
) {
    if assoc.other_train_id != other_train_id
        || assoc.other_train_location_id_suffix != *other_train_location_suffix
    {
        return;
    }
    if *stp_modification_type == ModificationType::Amend {
        for replacement in assoc.replacements.iter_mut() {
            if replacement.validity[0].valid_begin == *begin {
                amend_individual_assoc(
                    replacement,
                    begin,
                    end,
                    days_of_week,
                    day_diff,
                    for_passengers,
                );
            }
        }
    } else if *stp_modification_type == ModificationType::Delete {
        for (cancellation, old_days_of_week) in assoc.cancellations.iter_mut() {
            if cancellation.valid_begin == *begin {
                *cancellation = TrainValidityPeriod {
                    valid_begin: begin.clone(),
                    valid_end: end.clone(),
                };
                *old_days_of_week = days_of_week.clone();
            }
        }
    }
}

fn amend_single_vec_assocs(
    assocs: &mut Vec<AssociationNode>,
    other_train_id: &str,
    begin: &DateTime<Tz>,
    end: &DateTime<Tz>,
    days_of_week: &DaysOfWeek,
    other_train_location_suffix: &Option<String>,
    stp_modification_type: &ModificationType,
    is_stp: bool,
    day_diff: i8,
    for_passengers: bool,
) {
    for ref mut assoc in assocs.iter_mut() {
        if *stp_modification_type == ModificationType::Insert {
            if is_matching_assoc_for_modify_insertion(
                assoc,
                other_train_id,
                begin,
                other_train_location_suffix,
                is_stp,
                false,
            ) {
                amend_individual_assoc(assoc, begin, end, days_of_week, day_diff, for_passengers);
            }
        } else {
            amend_single_assoc_replacements_cancellations(
                assoc,
                other_train_id,
                begin,
                end,
                days_of_week,
                other_train_location_suffix,
                stp_modification_type,
                day_diff,
                for_passengers,
            );
        }
    }
}

fn cancel_single_assoc(
    assoc: &mut AssociationNode,
    other_train_id: &str,
    begin: &DateTime<Tz>,
    end: &DateTime<Tz>,
    days_of_week: &DaysOfWeek,
    other_train_location_suffix: &Option<String>,
    use_rev: bool,
) {
    if other_train_id == assoc.other_train_id
        && *other_train_location_suffix == assoc.other_train_location_id_suffix
    {
        let (rev_begin, rev_end, rev_days_of_week) = if use_rev {
            (
                rev_date(&begin, assoc.day_diff),
                rev_date(&end, assoc.day_diff),
                rev_days(&days_of_week, assoc.day_diff),
            )
        } else {
            (*begin, *end, *days_of_week)
        };

        if !check_date_applicability(
            &assoc.validity[0],
            &assoc.days,
            rev_begin,
            rev_end,
            &rev_days_of_week,
        ) {
            return;
        }
        let new_cancel = TrainValidityPeriod {
            valid_begin: rev_begin,
            valid_end: rev_end,
        };
        assoc
            .cancellations
            .push((new_cancel, rev_days_of_week.clone()))
    }
}

fn cancel_single_vec_assocs(
    assocs: &mut Vec<AssociationNode>,
    other_train_id: &str,
    begin: &DateTime<Tz>,
    end: &DateTime<Tz>,
    days_of_week: &DaysOfWeek,
    other_train_location_suffix: &Option<String>,
    use_rev: bool,
) {
    for ref mut assoc in assocs.iter_mut() {
        cancel_single_assoc(
            assoc,
            other_train_id,
            begin,
            end,
            days_of_week,
            other_train_location_suffix,
            use_rev,
        );
    }
}

fn replace_single_vec_assocs(
    assocs: &mut Vec<AssociationNode>,
    other_train_id: &str,
    other_train_location_suffix: &Option<String>,
    new_assoc: &AssociationNode,
) {
    for ref mut assoc in assocs.iter_mut() {
        if other_train_id == assoc.other_train_id
            && *other_train_location_suffix == assoc.other_train_location_id_suffix
        {
            // check for no overlapping days at all
            if !check_date_applicability(
                &assoc.validity[0],
                &assoc.days,
                new_assoc.validity[0].valid_begin,
                new_assoc.validity[0].valid_end,
                &new_assoc.days,
            ) {
                continue;
            }
            assoc.replacements.push(new_assoc.clone());
        }
    }
}

fn find_replacement_train<'a>(
    trains: &'a mut Vec<Train>,
    begin: &DateTime<Tz>,
) -> Option<&'a mut Train> {
    for train in trains.iter_mut() {
        for replacement_train in train.replacements.iter_mut() {
            if replacement_train.validity[0].valid_begin == *begin {
                return Some(replacement_train);
            }
        }
    }
    None
}

fn trains_delete_assoc(
    trains: &mut Vec<Train>,
    other_train_id: &str,
    begin: &DateTime<Tz>,
    location: &str,
    location_suffix: &Option<String>,
    other_train_location_suffix: &Option<String>,
    stp_modification_type: &ModificationType,
    is_stp: bool,
) {
    for ref mut train in trains.iter_mut() {
        // recurse on replacements
        trains_delete_assoc(
            &mut train.replacements,
            &other_train_id,
            &begin,
            &location,
            &location_suffix,
            &other_train_location_suffix,
            &stp_modification_type,
            is_stp,
        );

        for ref mut train_location in train.route.iter_mut() {
            if train_location.id != location || train_location.id_suffix != *location_suffix {
                continue;
            }
            delete_single_vec_assocs(
                &mut train_location.divides_to_form,
                other_train_id,
                begin,
                other_train_location_suffix,
                stp_modification_type,
                is_stp,
                false,
            );
            delete_single_vec_assocs(
                &mut train_location.joins_to,
                other_train_id,
                begin,
                other_train_location_suffix,
                stp_modification_type,
                is_stp,
                false,
            );
            if let Some(ref mut assoc) = &mut train_location.becomes {
                delete_single_assoc_replacements_cancellations(
                    assoc,
                    other_train_id,
                    begin,
                    other_train_location_suffix,
                    stp_modification_type,
                    false,
                );
                if *stp_modification_type == ModificationType::Insert
                    && is_matching_assoc_for_modify_insertion(
                        assoc,
                        other_train_id,
                        begin,
                        other_train_location_suffix,
                        is_stp,
                        false,
                    )
                {
                    train_location.becomes = None;
                }
            }
        }
    }
}

fn trains_delete_rev_assoc(
    trains: &mut Vec<Train>,
    other_train_id: &str,
    begin: &DateTime<Tz>,
    location: &str,
    location_suffix: &Option<String>,
    other_train_location_suffix: &Option<String>,
    stp_modification_type: &ModificationType,
    is_stp: bool,
) {
    for ref mut train in trains.iter_mut() {
        // recurse on replacements
        trains_delete_rev_assoc(
            &mut train.replacements,
            &other_train_id,
            &begin,
            &location,
            &location_suffix,
            &other_train_location_suffix,
            &stp_modification_type,
            is_stp,
        );

        for ref mut train_location in train.route.iter_mut() {
            if train_location.id != location || train_location.id_suffix != *location_suffix {
                continue;
            }
            delete_single_vec_assocs(
                &mut train_location.divides_from,
                other_train_id,
                begin,
                other_train_location_suffix,
                stp_modification_type,
                is_stp,
                true,
            );
            delete_single_vec_assocs(
                &mut train_location.is_joined_to_by,
                other_train_id,
                begin,
                other_train_location_suffix,
                stp_modification_type,
                is_stp,
                true,
            );
            if let Some(ref mut assoc) = &mut train_location.forms_from {
                delete_single_assoc_replacements_cancellations(
                    assoc,
                    other_train_id,
                    begin,
                    other_train_location_suffix,
                    stp_modification_type,
                    true,
                );
                if *stp_modification_type == ModificationType::Insert
                    && is_matching_assoc_for_modify_insertion(
                        assoc,
                        other_train_id,
                        begin,
                        other_train_location_suffix,
                        is_stp,
                        true,
                    )
                {
                    train_location.forms_from = None;
                }
            }
        }
    }
}

fn trains_amend_assoc(
    trains: &mut Vec<Train>,
    other_train_id: &str,
    begin: &DateTime<Tz>,
    end: &DateTime<Tz>,
    days_of_week: &DaysOfWeek,
    location: &str,
    location_suffix: &Option<String>,
    other_train_location_suffix: &Option<String>,
    stp_modification_type: &ModificationType,
    is_stp: bool,
    day_diff: i8,
    for_passengers: bool,
) {
    for ref mut train in trains.iter_mut() {
        // recurse on replacements
        trains_amend_assoc(
            &mut train.replacements,
            &other_train_id,
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
        );

        for ref mut train_location in train.route.iter_mut() {
            if train_location.id != location || train_location.id_suffix != *location_suffix {
                continue;
            }
            amend_single_vec_assocs(
                &mut train_location.divides_to_form,
                other_train_id,
                begin,
                end,
                days_of_week,
                other_train_location_suffix,
                stp_modification_type,
                is_stp,
                day_diff,
                for_passengers,
            );
            amend_single_vec_assocs(
                &mut train_location.joins_to,
                other_train_id,
                begin,
                end,
                days_of_week,
                other_train_location_suffix,
                stp_modification_type,
                is_stp,
                day_diff,
                for_passengers,
            );
            if let Some(ref mut assoc) = &mut train_location.becomes {
                if *stp_modification_type == ModificationType::Insert
                    && is_matching_assoc_for_modify_insertion(
                        assoc,
                        other_train_id,
                        begin,
                        other_train_location_suffix,
                        is_stp,
                        false,
                    )
                {
                    amend_individual_assoc(
                        assoc,
                        begin,
                        end,
                        days_of_week,
                        day_diff,
                        for_passengers,
                    );
                }
                amend_single_assoc_replacements_cancellations(
                    assoc,
                    other_train_id,
                    begin,
                    end,
                    days_of_week,
                    other_train_location_suffix,
                    stp_modification_type,
                    day_diff,
                    for_passengers,
                );
            }
        }
    }
}

fn trains_amend_rev_assoc(
    trains: &mut Vec<Train>,
    other_train_id: &str,
    begin: &DateTime<Tz>,
    end: &DateTime<Tz>,
    days_of_week: &DaysOfWeek,
    location: &str,
    location_suffix: &Option<String>,
    other_train_location_suffix: &Option<String>,
    stp_modification_type: &ModificationType,
    is_stp: bool,
    day_diff: i8,
    for_passengers: bool,
) {
    for ref mut train in trains.iter_mut() {
        // recurse on replacements
        trains_amend_rev_assoc(
            &mut train.replacements,
            &other_train_id,
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
        );

        for ref mut train_location in train.route.iter_mut() {
            if train_location.id != location || train_location.id_suffix != *location_suffix {
                continue;
            }
            amend_single_vec_assocs(
                &mut train_location.divides_from,
                other_train_id,
                begin,
                end,
                days_of_week,
                other_train_location_suffix,
                stp_modification_type,
                is_stp,
                day_diff,
                for_passengers,
            );
            amend_single_vec_assocs(
                &mut train_location.is_joined_to_by,
                other_train_id,
                begin,
                end,
                days_of_week,
                other_train_location_suffix,
                stp_modification_type,
                is_stp,
                day_diff,
                for_passengers,
            );
            if let Some(ref mut assoc) = &mut train_location.forms_from {
                if *stp_modification_type == ModificationType::Insert
                    && is_matching_assoc_for_modify_insertion(
                        assoc,
                        other_train_id,
                        begin,
                        other_train_location_suffix,
                        is_stp,
                        false,
                    )
                {
                    amend_individual_assoc(
                        assoc,
                        begin,
                        end,
                        days_of_week,
                        day_diff,
                        for_passengers,
                    );
                }
                amend_single_assoc_replacements_cancellations(
                    assoc,
                    other_train_id,
                    begin,
                    end,
                    days_of_week,
                    other_train_location_suffix,
                    stp_modification_type,
                    day_diff,
                    for_passengers,
                );
            }
        }
    }
}

fn trains_cancel_assoc(
    trains: &mut Vec<Train>,
    other_train_id: &str,
    begin: &DateTime<Tz>,
    end: &DateTime<Tz>,
    days_of_week: &DaysOfWeek,
    location: &str,
    location_suffix: &Option<String>,
    other_train_location_suffix: &Option<String>,
) {
    for ref mut train in trains.iter_mut() {
        // recurse on replacements
        trains_cancel_assoc(
            &mut train.replacements,
            &other_train_id,
            &begin,
            &end,
            &days_of_week,
            &location,
            &location_suffix,
            &other_train_location_suffix,
        );

        for ref mut train_location in train.route.iter_mut() {
            if train_location.id == location && train_location.id_suffix == *location_suffix {
                cancel_single_vec_assocs(
                    &mut train_location.divides_to_form,
                    other_train_id,
                    begin,
                    end,
                    days_of_week,
                    other_train_location_suffix,
                    false,
                );
                cancel_single_vec_assocs(
                    &mut train_location.joins_to,
                    other_train_id,
                    begin,
                    end,
                    days_of_week,
                    other_train_location_suffix,
                    false,
                );
                if let Some(assoc) = &mut train_location.becomes {
                    cancel_single_assoc(
                        assoc,
                        other_train_id,
                        begin,
                        end,
                        days_of_week,
                        other_train_location_suffix,
                        false,
                    );
                }
            }
        }
    }
}

fn trains_cancel_rev_assoc(
    trains: &mut Vec<Train>,
    other_train_id: &str,
    begin: &DateTime<Tz>,
    end: &DateTime<Tz>,
    days_of_week: &DaysOfWeek,
    location: &str,
    location_suffix: &Option<String>,
    other_train_location_suffix: &Option<String>,
) {
    for ref mut train in trains.iter_mut() {
        // recurse on replacements
        trains_cancel_rev_assoc(
            &mut train.replacements,
            &other_train_id,
            &begin,
            &end,
            &days_of_week,
            &location,
            &location_suffix,
            &other_train_location_suffix,
        );

        for ref mut train_location in train.route.iter_mut() {
            if train_location.id == location && train_location.id_suffix == *location_suffix {
                cancel_single_vec_assocs(
                    &mut train_location.divides_from,
                    other_train_id,
                    begin,
                    end,
                    days_of_week,
                    other_train_location_suffix,
                    true,
                );
                cancel_single_vec_assocs(
                    &mut train_location.is_joined_to_by,
                    other_train_id,
                    begin,
                    end,
                    days_of_week,
                    other_train_location_suffix,
                    true,
                );
                if let Some(assoc) = &mut train_location.forms_from {
                    cancel_single_assoc(
                        assoc,
                        other_train_id,
                        begin,
                        end,
                        days_of_week,
                        other_train_location_suffix,
                        true,
                    );
                }
            }
        }
    }
}

fn trains_replace_assoc(
    trains: &mut Vec<Train>,
    other_train_id: &str,
    location: &str,
    location_suffix: &Option<String>,
    other_train_location_suffix: &Option<String>,
    new_assoc: &AssociationNode,
) {
    for ref mut train in trains.iter_mut() {
        // recurse on replacements
        trains_replace_assoc(
            &mut train.replacements,
            &other_train_id,
            &location,
            &location_suffix,
            &other_train_location_suffix,
            &new_assoc,
        );

        for ref mut train_location in train.route.iter_mut() {
            if train_location.id == location && train_location.id_suffix == *location_suffix {
                replace_single_vec_assocs(
                    &mut train_location.divides_to_form,
                    other_train_id,
                    other_train_location_suffix,
                    new_assoc,
                );
                replace_single_vec_assocs(
                    &mut train_location.joins_to,
                    other_train_id,
                    other_train_location_suffix,
                    new_assoc,
                );
                if let Some(assoc) = &mut train_location.becomes {
                    if other_train_id == assoc.other_train_id
                        && *other_train_location_suffix == assoc.other_train_location_id_suffix
                    {
                        // check for no overlapping days at all
                        if !check_date_applicability(
                            &assoc.validity[0],
                            &assoc.days,
                            new_assoc.validity[0].valid_begin,
                            new_assoc.validity[0].valid_end,
                            &new_assoc.days,
                        ) {
                            continue;
                        }
                        assoc.replacements.push(new_assoc.clone());
                    }
                }
            }
        }
    }
}

fn trains_replace_rev_assoc(
    trains: &mut Vec<Train>,
    other_train_id: &str,
    location: &str,
    location_suffix: &Option<String>,
    other_train_location_suffix: &Option<String>,
    new_assoc: &AssociationNode,
) {
    for ref mut train in trains.iter_mut() {
        // recurse on replacements
        trains_replace_rev_assoc(
            &mut train.replacements,
            &other_train_id,
            &location,
            &location_suffix,
            &other_train_location_suffix,
            &new_assoc,
        );

        for ref mut train_location in train.route.iter_mut() {
            if train_location.id == location && train_location.id_suffix == *location_suffix {
                replace_single_vec_assocs(
                    &mut train_location.divides_from,
                    other_train_id,
                    other_train_location_suffix,
                    new_assoc,
                );
                replace_single_vec_assocs(
                    &mut train_location.is_joined_to_by,
                    other_train_id,
                    other_train_location_suffix,
                    new_assoc,
                );
                if let Some(assoc) = &mut train_location.forms_from {
                    if other_train_id == assoc.other_train_id
                        && *other_train_location_suffix == assoc.other_train_location_id_suffix
                    {
                        // check for no overlapping days at all
                        if !check_date_applicability(
                            &assoc.validity[0],
                            &assoc.days,
                            new_assoc.validity[0].valid_begin,
                            new_assoc.validity[0].valid_end,
                            &new_assoc.days,
                        ) {
                            continue;
                        }
                        assoc.replacements.push(new_assoc.clone());
                    }
                }
            }
        }
    }
}

fn produce_cif_error_closure(
    number: u64,
    column: usize,
) -> Box<dyn FnOnce(CifErrorType) -> CifError> {
    Box::new(move |x| CifError {
        error_type: x,
        line: number,
        column: column,
    })
}

fn produce_nr_json_error_closure(
    field_name: String,
) -> Box<dyn FnOnce(CifErrorType) -> NrJsonError> {
    Box::new(move |x| NrJsonError {
        error_type: x,
        field_name: field_name,
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

fn read_date<F, T>(date_slice: &str, error_logic: F) -> Result<DateTime<Tz>, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    let parsed_date = NaiveDate::parse_from_str(date_slice, "%y%m%d");
    let parsed_date = match parsed_date {
        Ok(x) => x,
        Err(x) => return Err(error_logic(CifErrorType::ChronoParseError(x))),
    };
    Ok(London
        .from_local_datetime(&parsed_date.and_hms_opt(0, 0, 0).unwrap())
        .unwrap())
}

fn read_backwards_date<F, T>(date_slice: &str, error_logic: F) -> Result<DateTime<Tz>, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    let parsed_date = NaiveDate::parse_from_str(date_slice, "%d%m%y");
    let parsed_date = match parsed_date {
        Ok(x) => x,
        Err(x) => return Err(error_logic(CifErrorType::ChronoParseError(x))),
    };
    Ok(London
        .from_local_datetime(&parsed_date.and_hms_opt(0, 0, 0).unwrap())
        .unwrap())
}

fn read_vstp_date<F, T>(date_slice: &str, error_logic: F) -> Result<DateTime<Tz>, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    let parsed_date = NaiveDate::parse_from_str(date_slice, "%Y-%m-%d");
    let parsed_date = match parsed_date {
        Ok(x) => x,
        Err(x) => return Err(error_logic(CifErrorType::ChronoParseError(x))),
    };
    Ok(London
        .from_local_datetime(&parsed_date.and_hms_opt(0, 0, 0).unwrap())
        .unwrap())
}

fn read_optional_string(slice: &str) -> Option<String> {
    if slice.chars().fold(true, |acc, x| acc && x == ' ') {
        None
    } else {
        Some(slice.to_string())
    }
}

fn read_days_of_week<F, T>(slice: &str, error_logic: F) -> Result<DaysOfWeek, T>
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
        Ok(DaysOfWeek {
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
        "" => Ok(None),
        x => Err(error_logic(CifErrorType::InvalidTrainPower(x.to_string()))),
    }
}

fn read_speed<F, T>(slice: &str, error_logic: F) -> Result<Option<f64>, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    let speed_mph = match slice {
        "   " => None,
        x => match x.parse::<u16>() {
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
) -> Result<(OperatingCharacteristics, bool), T>
where
    F: FnOnce(CifErrorType) -> T,
{
    let mut operating_characteristics = OperatingCharacteristics {
        ..Default::default()
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
            "755" => Some("Class 755 'FLIRT' bi-mode running on diesel".to_string()),
            "777" => Some("Class 777/1 'METRO' bi-mode running on battery".to_string()),
            "800" => Some("Class 800 'Azuma' bi-mode running on diesel".to_string()),
            "802" => {
                Some("Class 800/802 'IET/Nova 1/Paragon' bi-mode running on diesel".to_string())
            }
            "805" => Some("Class 805 'Hitachi AT300' bi-mode running on diesel".to_string()),
            "1400" => Some("Diesel locomotive hauling 1400 tons".to_string()), // lol
            "" => None,
            x => return Err(error_logic(CifErrorType::InvalidTimingLoad(x.to_string()))),
        },
        "E" => match timing_load.trim() {
            "325" => Some("Class 325 Parcels EMU".to_string()),
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

fn read_catering<F, T>(slice: &str, error_logic: F) -> Result<(Catering, bool), T>
where
    F: FnOnce(CifErrorType) -> T,
{
    let mut catering = Catering {
        ..Default::default()
    };
    let mut wheelchair_reservations = false;

    for chr in slice.chars() {
        match chr {
            'C' => catering.buffet = true,
            'F' => catering.first_class_restaurant = true,
            'H' => catering.hot_food = true,
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
) -> Result<Reservations, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    match slice.trim() {
        "A" => Ok(Reservations {
            seats: if first_seating || standard_seating {
                ReservationField::Mandatory
            } else {
                ReservationField::NotApplicable
            },
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
        }),
        "E" => Ok(Reservations {
            seats: if first_seating || standard_seating {
                ReservationField::NotMandatory
            } else {
                ReservationField::NotApplicable
            },
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
        }),
        "R" => Ok(Reservations {
            seats: if first_seating || standard_seating {
                ReservationField::Recommended
            } else {
                ReservationField::NotApplicable
            },
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
        }),
        "S" => Ok(Reservations {
            seats: if first_seating || standard_seating {
                ReservationField::Possible
            } else {
                ReservationField::NotApplicable
            },
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
        }),
        "" => Ok(Reservations {
            seats: if first_seating || standard_seating {
                ReservationField::Impossible
            } else {
                ReservationField::NotApplicable
            },
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

fn amend_train(train: &mut Train, new_train: Train) {
    train.validity = new_train.validity;
    train.days_of_week = new_train.days_of_week;
    train.runs_as_required = new_train.runs_as_required;
    train.performance_monitoring = None;
    train.route = vec![];
    train.variable_train = new_train.variable_train;
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

fn read_activities<F, T>(slice: &str, error_logic: F) -> Result<Activities, T>
where
    F: FnOnce(CifErrorType) -> T,
{
    let mut activities = Activities {
        ..Default::default()
    };

    for activity in slice
        .chars()
        .chunks(2)
        .into_iter()
        .map(|chunk| chunk.collect::<String>())
    {
        match activity.as_str() {
            "A " => activities.other_trains_pass = true,
            "AE" => activities.attach_or_detach_assisting_loco = true,
            "AX" => activities.x_on_arrival = true,
            "BL" => activities.banking_loco = true,
            "C " => activities.crew_change = true,
            "D " => activities.set_down_only = true,
            "-D" => activities.detach = true,
            "E " => activities.examination = true,
            "G " => activities.gbprtt = true,
            "H " => activities.prevent_column_merge = true,
            "HH" => activities.prevent_third_column_merge = true,
            "K " => activities.passenger_count = true,
            "KC" => activities.ticket_collection = true,
            "KE" => activities.ticket_examination = true,
            "KF" => activities.first_class_ticket_examination = true,
            "KS" => activities.selective_ticket_examination = true,
            "L " => activities.change_loco = true,
            "N " => activities.unadvertised_stop = true,
            "OP" => activities.operational_stop = true,
            "OR" => activities.train_locomotive_on_rear = true,
            "PR" => activities.propelling = true,
            "R " => activities.request_stop = true,
            "RM" => activities.reversing_move = true,
            "RR" => activities.run_round = true,
            "S " => activities.staff_stop = true,
            "T " => activities.normal_passenger_stop = true,
            "-T" => (activities.detach, activities.attach) = (true, true),
            "TB" => activities.train_begins = true,
            "TF" => activities.train_finishes = true,
            "TS" => activities.tops_reporting = true,
            "TW" => activities.token_etc = true,
            "U " => activities.pick_up_only = true,
            "-U" => activities.attach = true,
            "W " => activities.watering_stock = true,
            "X " => activities.cross_at_passing_point = true,
            "  " => (),
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
        "P" => TrainStatus::PassengerParcels,
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

fn get_working_time(location: &TrainLocation) -> (NaiveTime, u8) {
    // no error checking needed as any issue here should be a panic; trains are
    // checked for validity as they are written
    match location.working_dep {
        Some(x) => (x, location.working_dep_day.unwrap()),
        None => (
            location.working_pass.unwrap(),
            location.working_pass_day.unwrap(),
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
    pub fn new() -> CifImporter {
        CifImporter {
            ..Default::default()
        }
    }

    fn delete_unwritten_assocs(
        &mut self,
        main_train_id: &str,
        location: &str,
        location_suffix: &Option<String>,
        other_train_id: &str,
        begin: &DateTime<Tz>,
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
            old_assoc.retain(|(assoc, _category)| {
                !is_matching_assoc_for_modify_insertion(
                    assoc,
                    other_train_id,
                    &begin,
                    &other_train_location_suffix,
                    is_stp,
                    use_rev,
                )
            });
        } else {
            for (ref mut assoc, ref _category) in old_assoc.iter_mut() {
                delete_single_assoc_replacements_cancellations(
                    assoc,
                    other_train_id,
                    &begin,
                    &other_train_location_suffix,
                    &stp_modification_type,
                    use_rev,
                );
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
        begin: &DateTime<Tz>,
        end: &DateTime<Tz>,
        days_of_week: &DaysOfWeek,
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

        for (ref mut assoc, ref _category) in old_assoc.iter_mut() {
            cancel_single_assoc(
                assoc,
                other_train_id,
                begin,
                end,
                days_of_week,
                other_train_location_suffix,
                use_rev,
            );
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
        begin: &DateTime<Tz>,
        end: &DateTime<Tz>,
        days_of_week: &DaysOfWeek,
        other_train_location_suffix: &Option<String>,
        stp_modification_type: &ModificationType,
        is_stp: bool,
        day_diff: i8,
        for_passengers: bool,
        category: &AssociationCategory,
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

        for (ref mut assoc, ref mut old_category) in old_assoc.iter_mut() {
            if *stp_modification_type == ModificationType::Insert {
                if is_matching_assoc_for_modify_insertion(
                    assoc,
                    other_train_id,
                    begin,
                    other_train_location_suffix,
                    is_stp,
                    false,
                ) {
                    amend_individual_assoc(
                        assoc,
                        begin,
                        end,
                        days_of_week,
                        day_diff,
                        for_passengers,
                    );
                    *old_category = *category
                }
            } else {
                amend_single_assoc_replacements_cancellations(
                    assoc,
                    other_train_id,
                    begin,
                    end,
                    days_of_week,
                    other_train_location_suffix,
                    stp_modification_type,
                    day_diff,
                    for_passengers,
                );
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
        new_assoc: &AssociationNode,
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

        for (ref mut assoc, ref _category) in old_assoc.iter_mut() {
            if other_train_id == assoc.other_train_id
                && *other_train_location_suffix == assoc.other_train_location_id_suffix
            {
                // check for no overlapping days at all
                if !check_date_applicability(
                    &assoc.validity[0],
                    &assoc.days,
                    new_assoc.validity[0].valid_begin,
                    new_assoc.validity[0].valid_end,
                    &new_assoc.days,
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
        schedule: &'a mut Schedule,
        number: u64,
        record_type: &str,
    ) -> Result<&'a mut Train, CifError> {
        let (main_train_id, begin, stp_modification_type, is_stp) = match &self.last_train {
            Some(x) => x,
            None => {
                return Err(CifError {
                    error_type: CifErrorType::UnexpectedRecordType(
                        record_type.to_string(),
                        "No preceding BS".to_string(),
                    ),
                    line: number,
                    column: 0,
                })
            }
        };

        let trains = match (
            schedule.trains.get_mut(main_train_id),
            &stp_modification_type,
        ) {
            (Some(x), _) => x,
            (None, ModificationType::Amend) => match self
                .orphaned_overlay_trains
                .get_mut(&(main_train_id.clone(), begin.clone()))
            {
                Some(x) => return Ok(x),
                None => panic!("Unable to find last-written train, even in orphaned overlays"),
            },
            _ => panic!("Unable to find last-written train"),
        };

        let train = match (&stp_modification_type, &is_stp) {
            (ModificationType::Insert, false) => trains.iter_mut().find(|train| {
                train.source.unwrap() == TrainSource::LongTerm
                    && train.validity[0].valid_begin == *begin
            }),
            (ModificationType::Insert, true) => trains.iter_mut().find(|train| {
                train.source.unwrap() == TrainSource::ShortTerm
                    && train.validity[0].valid_begin == *begin
            }),
            (ModificationType::Amend, _) => find_replacement_train(trains, begin),
            (ModificationType::Delete, _) => panic!("Unexpected train modification type"),
        };

        Ok(match (train, &stp_modification_type) {
            (Some(x), _) => x,
            (None, ModificationType::Amend) => match self
                .orphaned_overlay_trains
                .get_mut(&(main_train_id.clone(), begin.clone()))
            {
                Some(x) => x,
                None => panic!("Unable to find last-written train, even in orphaned overlays"),
            },
            _ => panic!("Unable to find last-written train"),
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

    fn read_association(
        &mut self,
        line: &str,
        mut schedule: Schedule,
        number: u64,
    ) -> Result<Schedule, CifError> {
        let modification_type =
            read_modification_type(&line[2..3], produce_cif_error_closure(number, 2))?;
        let (stp_modification_type, is_stp) =
            read_stp_indicator(&line[79..80], produce_cif_error_closure(number, 79))?;

        let main_train_id = &line[3..9];
        let other_train_id = &line[9..15];
        let begin = read_date(&line[15..21], produce_cif_error_closure(number, 15))?;
        let location = &line[37..44];
        let location_suffix = read_optional_string(&line[44..45]);
        let other_train_location_suffix = read_optional_string(&line[45..46]);

        // At this stage we have all the data we need for a simple delete, so handle this here
        //
        // Note these are NOT the same as STP cancels and indeed handled completely differently
        if modification_type == ModificationType::Delete {
            // first find any committed associations and delete
            trains_delete_assoc(
                schedule
                    .trains
                    .get_mut(main_train_id)
                    .as_mut()
                    .unwrap_or(&mut &mut vec![]),
                &other_train_id,
                &begin,
                &location,
                &location_suffix,
                &other_train_location_suffix,
                &stp_modification_type,
                is_stp,
            );
            trains_delete_rev_assoc(
                schedule
                    .trains
                    .get_mut(other_train_id)
                    .as_mut()
                    .unwrap_or(&mut &mut vec![]),
                &main_train_id,
                &begin,
                &location,
                &other_train_location_suffix,
                &location_suffix,
                &stp_modification_type,
                is_stp,
            );

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

            return Ok(schedule);
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
                schedule
                    .trains
                    .get_mut(main_train_id)
                    .as_mut()
                    .unwrap_or(&mut &mut vec![]),
                &other_train_id,
                &begin,
                &end,
                &days_of_week,
                &location,
                &location_suffix,
                &other_train_location_suffix,
            );
            trains_cancel_rev_assoc(
                schedule
                    .trains
                    .get_mut(other_train_id)
                    .as_mut()
                    .unwrap_or(&mut &mut vec![]),
                &main_train_id,
                &begin,
                &end,
                &days_of_week,
                &location,
                &other_train_location_suffix,
                &location_suffix,
            );

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

            return Ok(schedule);
        }

        let day_diff = match &line[36..37] {
            "S" => 0,
            "N" => 1,
            "P" => -1,
            x => {
                return Err(CifError {
                    error_type: CifErrorType::InvalidAssociationDateIndicator(x.to_string()),
                    line: number,
                    column: 36,
                })
            }
        };
        let for_passengers = match &line[47..48] {
            "P" => true,
            "O" => false,
            x => {
                return Err(CifError {
                    error_type: CifErrorType::InvalidAssociationType(x.to_string()),
                    line: number,
                    column: 47,
                })
            }
        };

        let category = match &line[34..36] {
            "JJ" => AssociationCategory::Join,
            "VV" => AssociationCategory::Divide,
            "NP" => AssociationCategory::Next,
            x => {
                return Err(CifError {
                    error_type: CifErrorType::InvalidAssociationCategory(x.to_string()),
                    line: number,
                    column: 34,
                })
            }
        };

        let rev_days_of_week = rev_days(&days_of_week, day_diff);
        let rev_begin = rev_date(&begin, day_diff);
        let rev_end = rev_date(&end, day_diff);
        let rev_category = match category {
            AssociationCategory::Join => AssociationCategory::IsJoinedToBy,
            AssociationCategory::Divide => AssociationCategory::DividesFrom,
            AssociationCategory::Next => AssociationCategory::FormsFrom,
            _ => panic!("Invalid association category"),
        };

        if modification_type == ModificationType::Amend {
            // first find any committed associations and modify
            trains_amend_assoc(
                schedule
                    .trains
                    .get_mut(main_train_id)
                    .as_mut()
                    .unwrap_or(&mut &mut vec![]),
                &other_train_id,
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
            );
            trains_amend_rev_assoc(
                schedule
                    .trains
                    .get_mut(other_train_id)
                    .as_mut()
                    .unwrap_or(&mut &mut vec![]),
                &main_train_id,
                &rev_begin,
                &rev_end,
                &rev_days_of_week,
                &location,
                &other_train_location_suffix,
                &location_suffix,
                &stp_modification_type,
                is_stp,
                -day_diff,
                for_passengers,
            );

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
                &category,
            );
            self.amend_unwritten_assocs(
                other_train_id,
                location,
                &other_train_location_suffix,
                main_train_id,
                &rev_begin,
                &rev_end,
                &rev_days_of_week,
                &location_suffix,
                &stp_modification_type,
                is_stp,
                -day_diff,
                for_passengers,
                &rev_category,
            );

            return Ok(schedule);
        }

        // all of the below will use AssociationNodes, so construct them here
        let new_assoc = AssociationNode {
            other_train_id: other_train_id.to_string(),
            other_train_location_id_suffix: other_train_location_suffix.clone(),
            validity: vec![TrainValidityPeriod {
                valid_begin: begin,
                valid_end: end,
            }],
            cancellations: vec![],
            replacements: vec![],
            days: days_of_week,
            day_diff,
            for_passengers,
            source: Some(if is_stp {
                TrainSource::ShortTerm
            } else {
                TrainSource::LongTerm
            }),
        };

        let new_rev_assoc = AssociationNode {
            other_train_id: main_train_id.to_string(),
            other_train_location_id_suffix: location_suffix.clone(),
            validity: vec![TrainValidityPeriod {
                valid_begin: rev_begin,
                valid_end: rev_end,
            }],
            cancellations: vec![],
            replacements: vec![],
            days: rev_days_of_week,
            day_diff: -day_diff,
            for_passengers,
            source: Some(if is_stp {
                TrainSource::ShortTerm
            } else {
                TrainSource::LongTerm
            }),
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
                .push((new_assoc, category));
            self.unwritten_assocs
                .entry((
                    other_train_id.to_string(),
                    location.to_string(),
                    other_train_location_suffix,
                ))
                .or_insert(vec![])
                .push((new_rev_assoc, rev_category));

            return Ok(schedule);
        }

        if stp_modification_type == ModificationType::Amend {
            // first find any committed associations and modify
            trains_replace_assoc(
                schedule
                    .trains
                    .get_mut(main_train_id)
                    .as_mut()
                    .unwrap_or(&mut &mut vec![]),
                &other_train_id,
                &location,
                &location_suffix,
                &other_train_location_suffix,
                &new_assoc,
            );
            trains_replace_rev_assoc(
                schedule
                    .trains
                    .get_mut(other_train_id)
                    .as_mut()
                    .unwrap_or(&mut &mut vec![]),
                &main_train_id,
                &location,
                &other_train_location_suffix,
                &location_suffix,
                &new_rev_assoc,
            );

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

            return Ok(schedule);
        }

        Ok(schedule)
    }

    fn read_basic_schedule(
        &mut self,
        line: &str,
        mut schedule: Schedule,
        number: u64,
    ) -> Result<Schedule, CifError> {
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
            let old_trains = schedule.trains.remove(main_train_id);
            let mut old_trains = match old_trains {
                None => return Ok(schedule),
                Some(x) => x,
            };

            if stp_modification_type == ModificationType::Insert {
                // first we delete main trains
                old_trains.retain(|train| {
                    match is_stp {
                        false => {
                            train.source.unwrap() != TrainSource::LongTerm
                                || train.validity[0].valid_begin != begin
                        } // delete the entire train for deleted inserts
                        true => {
                            train.source.unwrap() != TrainSource::ShortTerm
                                || train.validity[0].valid_begin != begin
                        }
                    }
                });
            } else {
                // now we clean up modifications/cancellations
                for ref mut train in old_trains.iter_mut() {
                    match stp_modification_type {
                        ModificationType::Insert => {
                            panic!("Insert found where Amend or Cancel expected")
                        }
                        ModificationType::Amend => train
                            .replacements
                            .retain(|replacement| replacement.validity[0].valid_begin != begin),
                        ModificationType::Delete => {
                            train.cancellations.retain(|(cancellation, _days_of_week)| {
                                cancellation.valid_begin != begin
                            })
                        }
                    }
                }
            }

            schedule
                .trains
                .insert(main_train_id.to_string(), old_trains);

            return Ok(schedule);
        }

        let end = read_date(&line[15..21], produce_cif_error_closure(number, 15))?;
        let days_of_week = read_days_of_week(&line[21..28], produce_cif_error_closure(number, 27))?;

        // Now we handle STP cancellations; these are where long-running
        // trains are deleted as a one-off
        if stp_modification_type == ModificationType::Delete
            && modification_type == ModificationType::Insert
        {
            let old_trains = schedule.trains.remove(main_train_id);
            let mut old_trains = match old_trains {
                None => return Ok(schedule),
                Some(x) => x,
            };

            // we cancel main trains
            for train in old_trains.iter_mut() {
                if !check_date_applicability(
                    &train.validity[0],
                    &train.days_of_week,
                    begin,
                    end,
                    &days_of_week,
                ) {
                    continue;
                }
                let new_cancel = TrainValidityPeriod {
                    valid_begin: begin.clone(),
                    valid_end: end.clone(),
                };
                train.cancellations.push((new_cancel, days_of_week.clone()))
            }

            schedule
                .trains
                .insert(main_train_id.to_string(), old_trains);

            return Ok(schedule);
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
                        })
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
        let timing_load_id = &line[50..57];

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

        // all of the below will use this so construct it now
        let new_train = Train {
            id: main_train_id.to_string(),
            validity: vec![TrainValidityPeriod {
                valid_begin: begin,
                valid_end: end,
            }],
            cancellations: vec![],
            replacements: vec![],
            days_of_week,
            variable_train: VariableTrain {
                train_type,
                public_id: Some(public_id.to_string()),
                headcode,
                service_group: Some(service_group.to_string()),
                power_type: power_type,
                timing_allocation: match timing_load_str {
                    None => None,
                    Some(x) => Some(TrainAllocation {
                        id: timing_load_id.to_string(),
                        description: x,
                        vehicles: None,
                    }),
                },
                actual_allocation: None,
                timing_speed_m_per_s: speed_m_per_s,
                operating_characteristics,
                has_first_class_seats: first_seating,
                has_second_class_seats: standard_seating,
                has_first_class_sleepers: first_sleepers,
                has_second_class_sleepers: standard_sleepers,
                carries_vehicles: train_type == TrainType::CarCarryingPassenger,
                reservations,
                catering,
                brand,
                name: None,
                uic_code: None,
                operator: None,
            },
            source: Some(if is_stp {
                TrainSource::ShortTerm
            } else {
                TrainSource::LongTerm
            }),
            runs_as_required,
            performance_monitoring: None,
            route: vec![],
        };

        schedule
            .trains_indexed_by_public_id
            .entry(public_id.to_string())
            .or_insert(HashSet::new())
            .insert(main_train_id.to_string());

        if modification_type == ModificationType::Amend {
            // we can write a (partial) train now, and continue updating it later.
            self.last_train = Some((
                main_train_id.to_string(),
                begin,
                stp_modification_type,
                is_stp,
            ));

            let old_trains = schedule.trains.remove(main_train_id);
            let mut old_trains = match old_trains {
                None => return Ok(schedule),
                Some(x) => x,
            };

            // first we amend main trains
            if stp_modification_type == ModificationType::Insert {
                for ref mut train in old_trains.iter_mut() {
                    if match is_stp {
                        false => {
                            train.source.unwrap() == TrainSource::LongTerm
                                && train.validity[0].valid_begin == begin
                        }
                        true => {
                            train.source.unwrap() == TrainSource::ShortTerm
                                && train.validity[0].valid_begin == begin
                        }
                    } {
                        amend_train(train, new_train.clone());
                    }
                }
            } else {
                // now we clean up modifications/cancellations
                for ref mut train in old_trains.iter_mut() {
                    if stp_modification_type == ModificationType::Amend {
                        for replacement in train.replacements.iter_mut() {
                            if replacement.validity[0].valid_begin == begin {
                                amend_train(replacement, new_train.clone());
                            }
                        }
                    } else if stp_modification_type == ModificationType::Delete {
                        for (cancellation, old_days_of_week) in train.cancellations.iter_mut() {
                            if cancellation.valid_begin == begin {
                                *cancellation = TrainValidityPeriod {
                                    valid_begin: begin,
                                    valid_end: end,
                                };
                                *old_days_of_week = days_of_week.clone();
                            }
                        }
                    }
                }
            }

            schedule
                .trains
                .insert(main_train_id.to_string(), old_trains);

            return Ok(schedule);
        }

        if modification_type == ModificationType::Insert
            && stp_modification_type == ModificationType::Insert
        {
            // we can write a (partial) train now, and continue updating it later.
            self.last_train = Some((
                main_train_id.to_string(),
                begin,
                stp_modification_type,
                is_stp,
            ));

            schedule
                .trains
                .entry(main_train_id.to_string())
                .or_insert(vec![])
                .push(new_train);

            return Ok(schedule);
        }

        if stp_modification_type == ModificationType::Amend {
            // we can write a (partial) train now, and continue updating it later.
            self.last_train = Some((
                main_train_id.to_string(),
                begin,
                stp_modification_type,
                is_stp,
            ));

            let old_trains = schedule.trains.remove(main_train_id);
            let mut old_trains = match old_trains {
                None => {
                    self.orphaned_overlay_trains
                        .insert((main_train_id.to_string(), begin), new_train);
                    return Ok(schedule);
                }
                Some(x) => x,
            };

            // we replace main trains
            let mut replaced = false;
            for train in old_trains.iter_mut() {
                if !check_date_applicability(
                    &train.validity[0],
                    &train.days_of_week,
                    begin,
                    end,
                    &days_of_week,
                ) {
                    continue;
                }
                replaced = true;
                train.replacements.push(new_train.clone())
            }

            if !replaced {
                self.orphaned_overlay_trains
                    .insert((main_train_id.to_string(), begin), new_train);
            }

            schedule
                .trains
                .insert(main_train_id.to_string(), old_trains);

            return Ok(schedule);
        }

        Ok(schedule)
    }

    fn read_extended_schedule(
        &mut self,
        line: &str,
        mut schedule: Schedule,
        number: u64,
    ) -> Result<Schedule, CifError> {
        // at this stage we can only be in an insert or amend statement, for STP other than CAN. So
        // we find the train we are inserting or amending.

        let uic_code = read_optional_string(&line[6..11]);

        let atoc_code = &line[11..13];

        let train_operator_desc =
            read_train_operator(atoc_code, produce_cif_error_closure(number, 11))?;

        let performance_monitoring =
            read_ats_code(&line[13..14], produce_cif_error_closure(number, 13))?;

        let train = self.get_last_train(&mut schedule, number, "BX")?;

        train.variable_train.uic_code = uic_code;
        train.variable_train.operator = Some(TrainOperator {
            id: atoc_code.to_string(),
            description: train_operator_desc,
        });
        train.performance_monitoring = Some(performance_monitoring);

        Ok(schedule)
    }

    fn read_location_origin(
        &mut self,
        line: &str,
        mut schedule: Schedule,
        number: u64,
    ) -> Result<Schedule, CifError> {
        // at this stage we can only be in an insert or amend statement, for STP other than CAN. So
        // we find the train we are inserting or amending.

        let location_id = &line[2..9];
        let location_suffix = read_optional_string(&line[9..10]);

        let wtt_dep =
            read_mandatory_wtt_time(&line[10..15], produce_cif_error_closure(number, 10))?;
        let pub_dep = read_public_time(&line[15..19], produce_cif_error_closure(number, 15))?;

        let platform = read_optional_string(&line[19..22]);
        let line_code = read_optional_string(&line[22..25]);

        let eng_allowance = read_allowance(&line[25..27], produce_cif_error_closure(number, 25))?;
        let path_allowance = read_allowance(&line[27..29], produce_cif_error_closure(number, 27))?;

        let activities = read_activities(&line[29..41], produce_cif_error_closure(number, 29))?;

        let perf_allowance = read_allowance(&line[41..43], produce_cif_error_closure(number, 41))?;

        let new_location = TrainLocation {
            timezone: London,
            id: location_id.to_string(),
            id_suffix: location_suffix,
            working_arr: None,
            working_arr_day: None,
            working_dep: Some(wtt_dep),
            working_dep_day: Some(0),
            working_pass: None,
            working_pass_day: None,
            public_arr: None,
            public_arr_day: None,
            public_dep: pub_dep,
            public_dep_day: Some(0),
            platform,
            line: line_code,
            path: None,
            engineering_allowance_s: Some(eng_allowance),
            pathing_allowance_s: Some(path_allowance),
            performance_allowance_s: Some(perf_allowance),
            activities,
            change_en_route: None,
            divides_to_form: vec![],
            joins_to: vec![],
            becomes: None,
            divides_from: vec![],
            is_joined_to_by: vec![],
            forms_from: None,
        };

        {
            let train = self.get_last_train(&mut schedule, number, "LI")?;

            if !train.route.is_empty() {
                return Err(CifError {
                    error_type: CifErrorType::UnexpectedRecordType(
                        "LO".to_string(),
                        "Train route not empty".to_string(),
                    ),
                    line: number,
                    column: 0,
                });
            }

            train.route.push(new_location);
        }
        schedule
            .trains_indexed_by_location
            .entry(location_id.to_string())
            .or_insert(HashSet::new())
            .insert(self.last_train.as_ref().unwrap().0.clone());

        Ok(schedule)
    }

    fn read_location_intermediate(
        &mut self,
        line: &str,
        mut schedule: Schedule,
        number: u64,
    ) -> Result<Schedule, CifError> {
        // at this stage we can only be in an insert or amend statement, for STP other than CAN. So
        // we find the train we are inserting or amending.

        let location_id = &line[2..9];
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
                })
            }
        };

        let pub_arr = read_public_time(&line[25..29], produce_cif_error_closure(number, 25))?;
        let pub_dep = read_public_time(&line[29..33], produce_cif_error_closure(number, 29))?;

        let platform = read_optional_string(&line[33..36]);
        let line_code = read_optional_string(&line[36..39]);
        let path_code = read_optional_string(&line[39..42]);

        let activities = read_activities(&line[42..54], produce_cif_error_closure(number, 42))?;

        let eng_allowance = read_allowance(&line[54..56], produce_cif_error_closure(number, 54))?;
        let path_allowance = read_allowance(&line[56..58], produce_cif_error_closure(number, 56))?;
        let perf_allowance = read_allowance(&line[58..60], produce_cif_error_closure(number, 58))?;

        let change_en_route = self.change_en_route.take();

        self.cr_location = None;

        {
            let train = self.get_last_train(&mut schedule, number, "LI")?;

            if train.route.is_empty() {
                return Err(CifError {
                    error_type: CifErrorType::UnexpectedRecordType(
                        "LI".to_string(),
                        "Train route is empty".to_string(),
                    ),
                    line: number,
                    column: 0,
                });
            }

            let (last_wtt_time, last_wtt_day) = get_working_time(train.route.last().unwrap());

            let wtt_arr_day = calculate_day(&wtt_arr, &last_wtt_time, last_wtt_day);
            let wtt_dep_day = calculate_day(&wtt_dep, &last_wtt_time, last_wtt_day);
            let wtt_pass_day = calculate_day(&wtt_pass, &last_wtt_time, last_wtt_day);

            // TODO maybe should change this to calculate based on last public time?
            let pub_arr_day = calculate_day(&pub_arr, &last_wtt_time, last_wtt_day);
            let pub_dep_day = calculate_day(&pub_dep, &last_wtt_time, last_wtt_day);

            let new_location = TrainLocation {
                timezone: London,
                id: location_id.to_string(),
                id_suffix: location_suffix,
                working_arr: wtt_arr,
                working_arr_day: wtt_arr_day,
                working_dep: wtt_dep,
                working_dep_day: wtt_dep_day,
                working_pass: wtt_pass,
                working_pass_day: wtt_pass_day,
                public_arr: pub_arr,
                public_arr_day: pub_arr_day,
                public_dep: pub_dep,
                public_dep_day: pub_dep_day,
                platform,
                line: line_code,
                path: path_code,
                engineering_allowance_s: Some(eng_allowance),
                pathing_allowance_s: Some(path_allowance),
                performance_allowance_s: Some(perf_allowance),
                activities,
                change_en_route: change_en_route,
                divides_to_form: vec![],
                joins_to: vec![],
                becomes: None,
                divides_from: vec![],
                is_joined_to_by: vec![],
                forms_from: None,
            };

            train.route.push(new_location);
        }
        schedule
            .trains_indexed_by_location
            .entry(location_id.to_string())
            .or_insert(HashSet::new())
            .insert(self.last_train.as_ref().unwrap().0.clone());

        Ok(schedule)
    }

    fn read_location_terminating(
        &mut self,
        line: &str,
        mut schedule: Schedule,
        number: u64,
    ) -> Result<Schedule, CifError> {
        // at this stage we can only be in an insert or amend statement, for STP other than CAN. So
        // we find the train we are inserting or amending.

        let location_id = &line[2..9];
        let location_suffix = read_optional_string(&line[9..10]);

        self.validate_change_en_route_location(location_id, &location_suffix, number, 2)?;

        let wtt_arr =
            read_mandatory_wtt_time(&line[10..15], produce_cif_error_closure(number, 10))?;
        let pub_arr = read_public_time(&line[15..19], produce_cif_error_closure(number, 15))?;

        let platform = read_optional_string(&line[19..22]);
        let path_code = read_optional_string(&line[22..25]);

        let activities = read_activities(&line[25..37], produce_cif_error_closure(number, 25))?;

        self.cr_location = None;
        let change_en_route = self.change_en_route.take();

        {
            let train = self.get_last_train(&mut schedule, number, "LT")?;

            if train.route.is_empty() {
                return Err(CifError {
                    error_type: CifErrorType::UnexpectedRecordType(
                        "LT".to_string(),
                        "Train route is empty".to_string(),
                    ),
                    line: number,
                    column: 0,
                });
            }

            let (last_wtt_time, last_wtt_day) = get_working_time(train.route.last().unwrap());

            let wtt_arr_day = calculate_day(&Some(wtt_arr), &last_wtt_time, last_wtt_day).unwrap();
            let pub_arr_day = calculate_day(&pub_arr, &last_wtt_time, last_wtt_day);

            let new_location = TrainLocation {
                timezone: London,
                id: location_id.to_string(),
                id_suffix: location_suffix,
                working_arr: Some(wtt_arr),
                working_arr_day: Some(wtt_arr_day),
                working_dep: None,
                working_dep_day: None,
                working_pass: None,
                working_pass_day: None,
                public_arr: pub_arr,
                public_arr_day: pub_arr_day,
                public_dep: None,
                public_dep_day: None,
                platform,
                line: None,
                path: path_code,
                engineering_allowance_s: None,
                pathing_allowance_s: None,
                performance_allowance_s: None,
                activities,
                change_en_route: change_en_route,
                divides_to_form: vec![],
                joins_to: vec![],
                becomes: None,
                divides_from: vec![],
                is_joined_to_by: vec![],
                forms_from: None,
            };

            train.route.push(new_location);
        }
        schedule
            .trains_indexed_by_location
            .entry(location_id.to_string())
            .or_insert(HashSet::new())
            .insert(self.last_train.as_ref().unwrap().0.clone());

        // we can now unset the last_train as this should be the last message received for any
        // given train
        self.last_train = None;

        Ok(schedule)
    }

    fn read_change_en_route(
        &mut self,
        line: &str,
        mut schedule: Schedule,
        number: u64,
    ) -> Result<Schedule, CifError> {
        // at this stage we can only be in an insert or amend statement, for STP other than CAN. So
        // we find the train we are inserting or amending.

        let (train_type, operator) = {
            let train = self.get_last_train(&mut schedule, number, "CR")?;

            if train.route.is_empty() {
                return Err(CifError {
                    error_type: CifErrorType::UnexpectedRecordType(
                        "CR".to_string(),
                        "Train route is empty".to_string(),
                    ),
                    line: number,
                    column: 0,
                });
            }

            let train_type =
                match read_train_type(&line[10..12], produce_cif_error_closure(number, 10))? {
                    Some(x) => x,
                    None => train.variable_train.train_type, // should only really happen for ships
                };

            (train_type, train.variable_train.operator.clone())
        };

        let location_id = &line[2..9];
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
        let timing_load_id = &line[30..37];

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

        self.change_en_route = Some(VariableTrain {
            train_type,
            public_id: Some(public_id.to_string()),
            headcode,
            service_group: Some(service_group.to_string()),
            power_type: power_type,
            timing_allocation: match timing_load_str {
                None => None,
                Some(x) => Some(TrainAllocation {
                    id: timing_load_id.to_string(),
                    description: x,
                    vehicles: None,
                }),
            },
            actual_allocation: None,
            timing_speed_m_per_s: speed_m_per_s,
            operating_characteristics,
            has_first_class_seats: first_seating,
            has_second_class_seats: standard_seating,
            has_first_class_sleepers: first_sleepers,
            has_second_class_sleepers: standard_sleepers,
            carries_vehicles: train_type == TrainType::CarCarryingPassenger,
            reservations: reservations,
            catering: catering,
            brand: brand,
            name: None,
            uic_code: uic_code,
            operator,
        });

        Ok(schedule)
    }

    fn read_tiploc(
        &self,
        line: &str,
        mut schedule: Schedule,
        number: u64,
        modification_type: ModificationType,
    ) -> Result<Schedule, CifError> {
        let tiploc = &line[2..9];
        let name = &line[18..44];
        let opt_crs = read_optional_string(&line[53..56]);

        let location = match modification_type {
            ModificationType::Insert => Location {
                id: tiploc.to_string(),
                name: name.to_string(),
                public_id: opt_crs.clone(),
            },
            ModificationType::Amend => {
                let location = schedule.locations.remove(tiploc);
                let mut location = match location {
                    None => {
                        return Err(CifError {
                            error_type: CifErrorType::LocationNotFound(tiploc.to_string()),
                            line: number,
                            column: 2,
                        })
                    }
                    Some(x) => x,
                };
                location.id = tiploc.to_string();
                location.name = name.to_string();
                location.public_id = opt_crs.clone();
                location
            }
            ModificationType::Delete => {
                schedule.locations.remove(tiploc); // it's OK if the TIPLOC isn't found
                return Ok(schedule);
            }
        };
        schedule.locations.insert(tiploc.to_string(), location);
        match opt_crs {
            None => (),
            Some(crs) => {
                schedule
                    .locations_indexed_by_public_id
                    .entry(crs.clone())
                    .or_insert(HashSet::new())
                    .insert(tiploc.to_string());
            }
        }
        Ok(schedule)
    }

    fn read_header(
        &self,
        line: &str,
        mut schedule: Schedule,
        number: u64,
    ) -> Result<Schedule, CifError> {
        schedule.their_id = Some(line[2..22].to_string());
        let parsed_datetime = NaiveDateTime::parse_from_str(&line[22..32], "%y%m%d%H%M");
        let parsed_datetime = match parsed_datetime {
            Ok(x) => x,
            Err(x) => {
                return Err(CifError {
                    error_type: CifErrorType::ChronoParseError(x),
                    line: number,
                    column: 22,
                })
            }
        };
        schedule.last_updated = Some(London.from_local_datetime(&parsed_datetime).unwrap());
        if &line[46..47] == "F" {
            schedule.valid_begin = Some(read_backwards_date(
                &line[48..54],
                produce_cif_error_closure(number, 48),
            )?);
            schedule.valid_end = Some(read_backwards_date(
                &line[54..60],
                produce_cif_error_closure(number, 48),
            )?);
        }
        Ok(schedule)
    }

    fn finalise(
        &mut self,
        _line: &str,
        mut schedule: Schedule,
        number: u64,
    ) -> Result<Schedule, CifError> {
        for ((train_id, location, location_suffix), assocs) in &self.unwritten_assocs {
            let mut trains = match schedule.trains.get_mut(train_id) {
                Some(x) => x,
                None => {
                    return Err(CifError {
                        error_type: CifErrorType::TrainNotFound(train_id.clone()),
                        line: number,
                        column: 0,
                    })
                }
            };

            write_assocs_to_trains(&mut trains, &train_id, &location, &location_suffix, &assocs);
        }
        self.unwritten_assocs.clear();

        for ((train_id, _begin), new_train) in &self.orphaned_overlay_trains {
            let old_trains = schedule.trains.remove(train_id);
            let mut old_trains = match old_trains {
                None => return Ok(schedule),
                Some(x) => x,
            };

            // we replace main trains
            for train in old_trains.iter_mut() {
                if !check_date_applicability(
                    &train.validity[0],
                    &train.days_of_week,
                    new_train.validity[0].valid_begin,
                    new_train.validity[0].valid_end,
                    &new_train.days_of_week,
                ) {
                    continue;
                }
                train.replacements.push(new_train.clone())
            }

            schedule.trains.insert(train_id.to_string(), old_trains);

            return Ok(schedule);
        }

        Ok(schedule)
    }

    fn read_record(
        &mut self,
        line: String,
        schedule: Schedule,
        number: u64,
    ) -> Result<Schedule, CifError> {
        if line.is_empty() {
            return Ok(schedule);
        }
        if line.len() != 80 {
            return Err(CifError {
                error_type: CifErrorType::InvalidRecordLength(line.len()),
                line: number,
                column: 0,
            });
        }
        match &line[..2] {
            "HD" => Ok(self.read_header(&line, schedule, number)?),
            "TI" => Ok(self.read_tiploc(&line, schedule, number, ModificationType::Insert)?),
            "TA" => Ok(self.read_tiploc(&line, schedule, number, ModificationType::Amend)?),
            "TD" => Ok(self.read_tiploc(&line, schedule, number, ModificationType::Delete)?),
            "AA" => Ok(self.read_association(&line, schedule, number)?),
            "BS" => Ok(self.read_basic_schedule(&line, schedule, number)?),
            "BX" => Ok(self.read_extended_schedule(&line, schedule, number)?),
            "LO" => Ok(self.read_location_origin(&line, schedule, number)?),
            "LI" => Ok(self.read_location_intermediate(&line, schedule, number)?),
            "LT" => Ok(self.read_location_terminating(&line, schedule, number)?),
            "CR" => Ok(self.read_change_en_route(&line, schedule, number)?),
            "ZZ" => Ok(self.finalise(&line, schedule, number)?),
            x => Err(CifError {
                error_type: CifErrorType::InvalidRecordType(x.to_string()),
                line: number,
                column: 0,
            }),
        }
    }
}

#[async_trait]
impl SlowImporter for CifImporter {
    async fn overlay(
        &mut self,
        reader: impl AsyncBufReadExt + Unpin + Send,
        mut schedule: Schedule,
    ) -> Result<Schedule, Error> {
        let mut lines = reader.lines();

        let mut i: u64 = 0;

        while let Some(line) = lines.next_line().await? {
            i += 1;
            schedule = self.read_record(line, schedule, i)?;
        }
        println!(
            "Successfully loaded {} trains from {} lines of CIF",
            schedule.trains.len(),
            i
        );
        Ok(schedule)
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
    previously_received: Arc<RwLock<Vec<NrJsonVstp>>>,
    config: NrJsonImporterConfig,
    persister_mutex: Arc<Mutex<()>>,
}

#[derive(Clone, Deserialize)]
pub struct NrJsonImporterConfig {
    filename: Option<String>,
}

impl NrJsonImporter {
    pub async fn new(config: NrJsonImporterConfig) -> Result<NrJsonImporter, Error> {
        let mut previously_received = vec![];
        match &config.filename {
            None => (),
            Some(filename) => match fs::read_to_string(filename).await {
                Ok(contents) => {
                    previously_received = serde_json::from_str::<Vec<NrJsonVstp>>(&contents)?;
                }
                Err(x) => {
                    println!("WARNING: Failed to load previous VSTP workings: {}", x);
                }
            },
        }
        Ok(NrJsonImporter {
            previously_received: Arc::new(RwLock::new(previously_received)),
            config,
            persister_mutex: Arc::new(Mutex::new(())),
        })
    }

    fn read_vstp_route(
        &self,
        schedule_segments: &Vec<NrJsonScheduleSegment>,
        train_status: &TrainStatus,
        train_id: &str,
        schedule: &mut Schedule,
    ) -> Result<Vec<TrainLocation>, NrJsonError> {
        let mut route = vec![];
        for (i, segment) in schedule_segments.iter().enumerate() {
            if segment.schedule_location.len() == 0 {
                return Err(NrJsonError {
                    error_type: CifErrorType::NotEnoughLocations,
                    field_name: "schedule_location".to_string(),
                });
            }
            for (j, location) in segment.schedule_location.iter().enumerate() {
                // don't populate a change en route on the first segment as this
                // will be populated quite happily in the main train's variable_train field.
                let change_en_route = if i == 0 || j != 0 {
                    None
                } else {
                    Some(self.read_vstp_variable_train(segment, train_status)?)
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
                    });
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
                        })
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
                    Some(x) => read_optional_string(&x),
                    None => None,
                };
                let line_code = match &location.cif_line {
                    Some(x) => read_optional_string(&x),
                    None => None,
                };
                let path_code = match &location.cif_path {
                    Some(x) => read_optional_string(&x),
                    None => None,
                };

                let activities = match &location.cif_activity {
                    Some(x) => read_activities(
                        format!("{: <12}", x).as_str(),
                        produce_nr_json_error_closure("CIF_activity".to_string()),
                    )?,
                    None => Activities {
                        ..Default::default()
                    },
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

                let new_location = TrainLocation {
                    timezone: London,
                    id: location_id.to_string(),
                    id_suffix: location_suffix,
                    working_arr: wtt_arr,
                    working_arr_day: wtt_arr_day,
                    working_dep: wtt_dep,
                    working_dep_day: wtt_dep_day,
                    working_pass: wtt_pass,
                    working_pass_day: wtt_pass_day,
                    public_arr: pub_arr,
                    public_arr_day: pub_arr_day,
                    public_dep: pub_dep,
                    public_dep_day: pub_dep_day,
                    platform,
                    line: line_code,
                    path: path_code,
                    engineering_allowance_s: eng_allowance,
                    pathing_allowance_s: path_allowance,
                    performance_allowance_s: perf_allowance,
                    activities,
                    change_en_route: change_en_route,
                    divides_to_form: vec![],
                    joins_to: vec![],
                    becomes: None,
                    divides_from: vec![],
                    is_joined_to_by: vec![],
                    forms_from: None,
                };

                route.push(new_location);
                schedule
                    .trains_indexed_by_location
                    .entry(location_id.to_string())
                    .or_insert(HashSet::new())
                    .insert(train_id.to_string());
            }
        }
        Ok(route)
    }

    fn read_vstp_variable_train(
        &self,
        schedule_segment: &NrJsonScheduleSegment,
        train_status: &TrainStatus,
    ) -> Result<VariableTrain, NrJsonError> {
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
            Some(x) => read_operating_characteristics(
                x,
                produce_nr_json_error_closure("CIF_operating_characteristics".to_string()),
            )?,
            None => (
                OperatingCharacteristics {
                    ..Default::default()
                },
                false,
            ),
        };

        let timing_load_str = match (
            &schedule_segment.cif_power_type,
            &schedule_segment.cif_timing_load,
        ) {
            (None, _) => None,
            (Some(x), None) => read_timing_load(
                x,
                "",
                operating_characteristics.br_mark_four_coaches,
                produce_nr_json_error_closure("CIF_power_type or CIF_timing_load".to_string()),
            )?,
            (Some(x), Some(y)) => read_timing_load(
                x,
                y,
                operating_characteristics.br_mark_four_coaches,
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
        };

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
            Some(x) => read_catering(
                x,
                produce_nr_json_error_closure("CIF_catering_code".to_string()),
            )?,
            None => (
                Catering {
                    ..Default::default()
                },
                false,
            ),
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

        Ok(VariableTrain {
            train_type,
            public_id: Some(public_id.to_string()),
            headcode,
            service_group: service_group.clone(),
            power_type: power_type,
            timing_allocation: match timing_load_str {
                None => None,
                Some(x) => Some(TrainAllocation {
                    id: timing_load_id,
                    description: x,
                    vehicles: None,
                }),
            },
            actual_allocation: None,
            timing_speed_m_per_s: speed_m_per_s,
            operating_characteristics,
            has_first_class_seats: first_seating,
            has_second_class_seats: standard_seating,
            has_first_class_sleepers: first_sleepers,
            has_second_class_sleepers: standard_sleepers,
            carries_vehicles: train_type == TrainType::CarCarryingPassenger,
            reservations,
            catering,
            brand,
            name: None,
            uic_code,
            operator: Some(TrainOperator {
                id: atoc_code.to_string(),
                description: train_operator_desc,
            }),
        })
    }

    fn read_vstp_entry(
        &self,
        parsed_json: &NrJsonVstp,
        mut schedule: Schedule,
    ) -> Result<(Schedule, bool), NrJsonError> {
        println!("Input: {:#?}", parsed_json);
        let modification_type = match parsed_json
            .vstp_cif_msg_v1
            .schedule
            .transaction_type
            .as_str()
        {
            "Create" => ModificationType::Insert,
            "Delete" => ModificationType::Delete,
            x => {
                return Err(NrJsonError {
                    error_type: CifErrorType::InvalidTransactionType(x.to_string()),
                    field_name: "transaction_type".to_string(),
                })
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
            return Ok((schedule, false));
        }

        // At this stage we have all the data we need for a simple delete, so handle this here
        //
        // Note these are NOT the same as STP cancels and indeed handled completely differently
        if modification_type == ModificationType::Delete {
            let old_trains = schedule.trains.remove(main_train_id);
            let mut old_trains = match old_trains {
                None => return Ok((schedule, false)),
                Some(x) => x,
            };

            if stp_modification_type == ModificationType::Insert {
                // first we delete main trains
                old_trains.retain(|train| {
                    match is_stp {
                        false => {
                            train.source.unwrap() != TrainSource::LongTerm
                                || train.validity[0].valid_begin != begin
                        } // delete the entire train for deleted inserts
                        true => {
                            train.source.unwrap() == TrainSource::LongTerm
                                || train.validity[0].valid_begin != begin
                        }
                    }
                });
            } else {
                // now we clean up modifications/cancellations
                for ref mut train in old_trains.iter_mut() {
                    match stp_modification_type {
                        ModificationType::Insert => {
                            panic!("Insert found where Amend or Cancel expected")
                        }
                        ModificationType::Amend => train
                            .replacements
                            .retain(|replacement| replacement.validity[0].valid_begin != begin),
                        ModificationType::Delete => {
                            train.cancellations.retain(|(cancellation, _days_of_week)| {
                                cancellation.valid_begin != begin
                            })
                        }
                    }
                }
            }

            schedule
                .trains
                .insert(main_train_id.to_string(), old_trains);

            println!("Successfully deleted train {}", main_train_id);
            return Ok((schedule, true));
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
            return Ok((schedule, false));
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
            let old_trains = schedule.trains.remove(main_train_id);
            let mut old_trains = match old_trains {
                None => return Ok((schedule, false)),
                Some(x) => x,
            };

            // we cancel main trains
            for train in old_trains.iter_mut() {
                if !check_date_applicability(
                    &train.validity[0],
                    &train.days_of_week,
                    begin,
                    end,
                    &days_of_week,
                ) {
                    continue;
                }
                let new_cancel = TrainValidityPeriod {
                    valid_begin: begin.clone(),
                    valid_end: end.clone(),
                };
                train.cancellations.push((new_cancel, days_of_week.clone()))
            }

            schedule
                .trains
                .insert(main_train_id.to_string(), old_trains);

            println!("Successfully cancelled train {}", main_train_id);
            return Ok((schedule, true));
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
            });
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
                OperatingCharacteristics {
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
        let new_train = Train {
            id: main_train_id.to_string(),
            validity: vec![TrainValidityPeriod {
                valid_begin: begin,
                valid_end: end,
            }],
            cancellations: vec![],
            replacements: vec![],
            days_of_week,
            variable_train: self.read_vstp_variable_train(
                &parsed_json
                    .vstp_cif_msg_v1
                    .schedule
                    .schedule_segment
                    .as_ref()
                    .unwrap()[0],
                &train_status,
            )?,
            source: Some(TrainSource::VeryShortTerm),
            runs_as_required,
            performance_monitoring: performance_monitoring,
            route: self.read_vstp_route(
                &parsed_json
                    .vstp_cif_msg_v1
                    .schedule
                    .schedule_segment
                    .as_ref()
                    .unwrap(),
                &train_status,
                main_train_id,
                &mut schedule,
            )?,
        };

        if modification_type == ModificationType::Insert
            && stp_modification_type == ModificationType::Insert
        {
            println!(
                "Successfully written train {} ({})",
                new_train.id,
                new_train.variable_train.public_id.as_ref().unwrap()
            );
            println!("Output: {:#?}", new_train);
            schedule
                .trains
                .entry(main_train_id.to_string())
                .or_insert(vec![])
                .push(new_train);

            return Ok((schedule, true));
        }

        if stp_modification_type == ModificationType::Amend {
            let old_trains = schedule.trains.remove(main_train_id);
            let mut old_trains = match old_trains {
                None => return Ok((schedule, false)),
                Some(x) => x,
            };

            // we replace main trains
            for train in old_trains.iter_mut() {
                if !check_date_applicability(
                    &train.validity[0],
                    &train.days_of_week,
                    begin,
                    end,
                    &days_of_week,
                ) {
                    continue;
                }
                train.replacements.push(new_train.clone())
            }

            println!("Successfully replaced train {}", main_train_id);
            schedule
                .trains
                .insert(main_train_id.to_string(), old_trains);

            return Ok((schedule, true));
        }

        Ok((schedule, false))
    }

    async fn write(&self) -> Result<(), Error> {
        match &self.config.filename {
            None => Ok(()),
            Some(filename) => {
                let _mutex = self.persister_mutex.lock().await;
                let json_string = {
                    let previously_received = self.previously_received.read().unwrap();
                    serde_json::to_string(&*previously_received)?
                };

                let tmp_filename = format!("{}.bak", filename);

                fs::write(&tmp_filename, json_string).await?;

                fs::rename(tmp_filename, filename).await?;

                Ok(())
            }
        }
    }
}

#[async_trait]
impl FastImporter for NrJsonImporter {
    fn overlay(&self, data: Vec<u8>, schedule: Schedule) -> Result<Schedule, Error> {
        let parsed_json = serde_json::from_slice::<NrJsonVstp>(&data)?;
        let (schedule, change_made) = self.read_vstp_entry(&parsed_json, schedule)?;
        if change_made {
            let mut previously_received = self.previously_received.write().unwrap();
            previously_received.push(parsed_json);
        }

        Ok(schedule)
    }
}

#[async_trait]
impl EphemeralImporter for NrJsonImporter {
    async fn repopulate(&self, mut schedule: Schedule) -> Result<Schedule, Error> {
        println!("Repopulating VSTP entries...");
        let mut new_previously_received = vec![];
        {
            let previously_received = self.previously_received.read().unwrap();
            for parsed_json in &*previously_received {
                let (new_schedule, change_made) = self.read_vstp_entry(&parsed_json, schedule)?;
                schedule = new_schedule;
                if change_made {
                    new_previously_received.push(parsed_json.clone());
                }
            }
        }
        let mut previously_received = self.previously_received.write().unwrap();
        *previously_received = new_previously_received;

        Ok(schedule)
    }

    async fn persist(&self) -> Result<(), Error> {
        Ok(self.write().await?)
    }
}
