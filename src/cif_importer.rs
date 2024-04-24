use crate::schedule::{Activities, AssociationNode, Catering, DaysOfWeek, Location, OperatingCharacteristics, ReservationField, Reservations, Schedule, Train, TrainAllocation, TrainLocation, TrainOperator, TrainSource, TrainType, TrainPower, TrainValidityPeriod, VariableTrain};
use crate::importer::Importer;
use crate::error::Error;

use async_trait::async_trait;
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
use chrono::format::ParseError;
use chrono::naive::Days;
use chrono_tz::Tz;
use chrono_tz::Europe::London;
use itertools::Itertools;

use std::collections::HashMap;
use std::fmt;
use std::ops::{Add, Sub};
use tokio::io::AsyncBufReadExt;

#[derive(Default)]
pub struct CifImporter {
    last_train: Option<(String, DateTime::<Tz>, ModificationType, bool)>,
    unwritten_assocs: HashMap<(String, String, Option<String>), Vec<(AssociationNode, AssociationCategory)>>,
    change_en_route: Option<VariableTrain>,
    cr_location: Option<(String, Option<String>)>,
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
    InvalidSeatingClass(String),
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
            CifErrorType::InvalidSeatingClass(x) => write!(f, "Invalid seating class {}", x),
            CifErrorType::InvalidReservationType(x) => write!(f, "Invalid reservation type {}", x),
            CifErrorType::InvalidCatering(x) => write!(f, "Invalid catering code {}", x),
            CifErrorType::InvalidBrand(x) => write!(f, "Invalid brand code {}", x),
            CifErrorType::UnexpectedRecordType(x, y) => write!(f, "Unexpected record type {} — {}", x, y),
            CifErrorType::InvalidTrainOperator(x) => write!(f, "Invalid train operator {}", x),
            CifErrorType::InvalidAtsCode(x) => write!(f, "Invalid ATS Code {}", x),
            CifErrorType::InvalidMinuteFraction(x) => write!(f, "Invalid minute fraction {}", x),
            CifErrorType::InvalidAllowance(x) => write!(f, "Invalid allowance {}", x),
            CifErrorType::InvalidActivity(x) => write!(f, "Invalid activity code {}", x),
            CifErrorType::InvalidWttTimesCombo => write!(f, "Invalid combination of WTT times; must be arr+dep, or pass only"),
            CifErrorType::ChangeEnRouteLocationUnmatched((x, y), (a, b)) => write!(f, "Found location {}-{} but expected (from previous CR) {}-{}", x, match y { Some(y) => y, None => " ", }, a, match b { Some(b) => b, None => " ", }),
            CifErrorType::TrainNotFound(x) => write!(f, "Could not find train {}", x),
            CifErrorType::InvalidDaysOfWeek(x) => write!(f, "Invalid days of week string {}", x),
        }
    }
}

#[derive(Debug)]
pub struct CifError {
    error_type: CifErrorType,
    line: u64,
    column: usize,
}

impl fmt::Display for CifError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error reading CIF file line {} column {}: {}", self.line, self.column, self.error_type)
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
        _ => panic!("Only designed for prev or next day (as per NR)")
    }
}

fn rev_date(date: &DateTime::<Tz>, day_diff: i8) -> DateTime::<Tz> {
    if day_diff < 0 {
        date.sub(Days::new(u64::try_from(-day_diff).unwrap()))
    }
    else {
        date.add(Days::new(u64::try_from(day_diff).unwrap()))
    }
}

fn check_date_applicability(existing_validity: &TrainValidityPeriod, existing_days: &DaysOfWeek, new_begin: DateTime::<Tz>, new_end: DateTime::<Tz>, new_days: &DaysOfWeek) -> bool {
    // check for no overlapping days at all
    if existing_days.into_iter().zip(new_days.into_iter()).find(|(existing_day, new_day)| *existing_day && *new_day).is_none() {
        false
    }
    else if new_begin > existing_validity.valid_end || new_end < existing_validity.valid_begin {
        false
    }
    else {
        true
    }
}

fn write_assocs_to_trains(trains: &mut Vec<Train>, train_id: &str, location: &str, location_suffix: &Option<String>, assocs: &Vec<(AssociationNode, AssociationCategory)>) {
    for ref mut train in trains.iter_mut() {
        // recurse on replacements
        write_assocs_to_trains(&mut train.replacements, &train_id, &location, &location_suffix, &assocs);

        for ref mut train_location in train.route.iter_mut() {
            if train_location.id == location && train_location.id_suffix == *location_suffix {
                for (assoc, category) in assocs {
                    if !check_date_applicability(&train.validity[0], &train.days_of_week, assoc.validity[0].valid_begin, assoc.validity[0].valid_end, &assoc.days) {
                        continue;
                    }
                    // we now know this is applicable to this train, so add it
                    match category {
                        AssociationCategory::Join => train_location.joins_to.push(assoc.clone()),
                        AssociationCategory::Divide => train_location.divides_to_form.push(assoc.clone()),
                        AssociationCategory::Next => train_location.becomes = Some(assoc.clone()),
                        AssociationCategory::IsJoinedToBy => train_location.is_joined_to_by.push(assoc.clone()),
                        AssociationCategory::DividesFrom => train_location.divides_from.push(assoc.clone()),
                        AssociationCategory::FormsFrom => train_location.forms_from = Some(assoc.clone()),
                    };
                }
            }
        }
    }
}

fn is_matching_assoc_for_modify_insertion(assoc: &AssociationNode, other_train_id: &str, begin: &DateTime::<Tz>, other_train_location_suffix: &Option<String>, is_stp: bool, use_rev: bool) -> bool {
    return match is_stp {
        false => assoc.source.unwrap() == TrainSource::LongTerm, // match the entire association for deleted or modified inserts
        true => assoc.source.unwrap() == TrainSource::ShortTerm,
    } &&
        assoc.validity[0].valid_begin == if use_rev { rev_date(begin, assoc.day_diff) } else { *begin } &&
        other_train_id == assoc.other_train_id &&
        *other_train_location_suffix == assoc.other_train_location_id_suffix;
}

fn is_matching_assoc_for_modify_replacement_or_cancel(validity: &TrainValidityPeriod, begin: &DateTime::<Tz>, day_diff: i8, use_rev: bool) -> bool {
    validity.valid_begin == if use_rev { rev_date(begin, day_diff) } else { *begin }
}

fn delete_single_assoc_replacements_cancellations(assoc: &mut AssociationNode, other_train_id: &str, begin: &DateTime::<Tz>, other_train_location_suffix: &Option<String>, stp_modification_type: &ModificationType, use_rev: bool) {
    if other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix {
        return;
    }
    if *stp_modification_type == ModificationType::Amend {
        assoc.replacements.retain(|assoc| !is_matching_assoc_for_modify_replacement_or_cancel(&assoc.validity[0], begin, assoc.day_diff, use_rev));
    }
    else if *stp_modification_type == ModificationType::Delete {
        assoc.cancellations.retain(|(validity, _days_of_week)| !is_matching_assoc_for_modify_replacement_or_cancel(validity, begin, assoc.day_diff, use_rev));
    }
}

fn delete_single_vec_assocs(assocs: &mut Vec<AssociationNode>, other_train_id: &str, begin: &DateTime::<Tz>, other_train_location_suffix: &Option<String>, stp_modification_type: &ModificationType, is_stp: bool, use_rev: bool) {
    if *stp_modification_type == ModificationType::Insert {
        assocs.retain(|assoc| !is_matching_assoc_for_modify_insertion(assoc, other_train_id, begin, other_train_location_suffix, is_stp, use_rev));
    }
    else {
        for ref mut assoc in assocs.iter_mut() {
            delete_single_assoc_replacements_cancellations(assoc, other_train_id, begin, other_train_location_suffix, stp_modification_type, use_rev);
        }
    }
}

fn amend_individual_assoc(assoc: &mut AssociationNode, begin: &DateTime::<Tz>, end: &DateTime::<Tz>, days_of_week: &DaysOfWeek, day_diff: i8, for_passengers: bool) {
    assoc.validity = vec![TrainValidityPeriod {
        valid_begin: begin.clone(),
        valid_end: end.clone(),
    }];
    assoc.days = days_of_week.clone();
    assoc.day_diff = day_diff;
    assoc.for_passengers = for_passengers;
}

fn amend_single_assoc_replacements_cancellations(assoc: &mut AssociationNode, other_train_id: &str, begin: &DateTime::<Tz>, end: &DateTime::<Tz>, days_of_week: &DaysOfWeek, other_train_location_suffix: &Option<String>, stp_modification_type: &ModificationType, day_diff: i8, for_passengers: bool) {
    if assoc.other_train_id != other_train_id || assoc.other_train_location_id_suffix != *other_train_location_suffix {
        return;
    }
    if *stp_modification_type == ModificationType::Amend {
        for replacement in assoc.replacements.iter_mut() {
            if replacement.validity[0].valid_begin == *begin {
                amend_individual_assoc(replacement, begin, end, days_of_week, day_diff, for_passengers);
            }
        }
    }
    else if *stp_modification_type == ModificationType::Delete {
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

fn amend_single_vec_assocs(assocs: &mut Vec<AssociationNode>, other_train_id: &str, begin: &DateTime::<Tz>, end: &DateTime::<Tz>, days_of_week: &DaysOfWeek, other_train_location_suffix: &Option<String>, stp_modification_type: &ModificationType, is_stp: bool, day_diff: i8, for_passengers: bool) {
    for ref mut assoc in assocs.iter_mut() {
        if *stp_modification_type == ModificationType::Insert {
            if is_matching_assoc_for_modify_insertion(assoc, other_train_id, begin, other_train_location_suffix, is_stp, false) {
                amend_individual_assoc(assoc, begin, end, days_of_week, day_diff, for_passengers);
            }
        }
        else {
            amend_single_assoc_replacements_cancellations(assoc, other_train_id, begin, end, days_of_week, other_train_location_suffix, stp_modification_type, day_diff, for_passengers);
        }
    }
}

fn cancel_single_assoc(assoc: &mut AssociationNode, other_train_id: &str, begin: &DateTime::<Tz>, end: &DateTime::<Tz>, days_of_week: &DaysOfWeek, other_train_location_suffix: &Option<String>, use_rev: bool) {
    if other_train_id == assoc.other_train_id && *other_train_location_suffix == assoc.other_train_location_id_suffix {
        let (rev_begin, rev_end, rev_days_of_week) = if use_rev {
            (rev_date(&begin, assoc.day_diff), rev_date(&end, assoc.day_diff), rev_days(&days_of_week, assoc.day_diff))
        }
        else {
            (*begin, *end, *days_of_week)
        };

        if !check_date_applicability(&assoc.validity[0], &assoc.days, rev_begin, rev_end, &rev_days_of_week) {
            return;
        }
        let new_cancel = TrainValidityPeriod {
            valid_begin: rev_begin,
            valid_end: rev_end,
        };
        assoc.cancellations.push((new_cancel, rev_days_of_week.clone()))
    }
}

fn cancel_single_vec_assocs(assocs: &mut Vec<AssociationNode>, other_train_id: &str, begin: &DateTime::<Tz>, end: &DateTime::<Tz>, days_of_week: &DaysOfWeek, other_train_location_suffix: &Option<String>, use_rev: bool) {
    for ref mut assoc in assocs.iter_mut() {
        cancel_single_assoc(assoc, other_train_id, begin, end, days_of_week, other_train_location_suffix, use_rev);
    }
}

fn replace_single_vec_assocs(assocs: &mut Vec<AssociationNode>, other_train_id: &str, other_train_location_suffix: &Option<String>, new_assoc: &AssociationNode) {
    for ref mut assoc in assocs.iter_mut() {
        if other_train_id == assoc.other_train_id && *other_train_location_suffix == assoc.other_train_location_id_suffix {
            // check for no overlapping days at all
            if !check_date_applicability(&assoc.validity[0], &assoc.days, new_assoc.validity[0].valid_begin, new_assoc.validity[0].valid_end, &new_assoc.days) {
                continue;
            }
            assoc.replacements.push(new_assoc.clone());
        }
    }
}

fn find_replacement_train<'a>(trains: &'a mut Vec<Train>, begin: &DateTime::<Tz>) -> Option<&'a mut Train> {
    for train in trains.iter_mut() {
        for replacement_train in train.replacements.iter_mut() {
            if replacement_train.validity[0].valid_begin == *begin {
                return Some(replacement_train);
            }
        }
    }
    None
}

fn trains_delete_assoc(trains: &mut Vec<Train>, other_train_id: &str, begin: &DateTime::<Tz>, location: &str, location_suffix: &Option<String>, other_train_location_suffix: &Option<String>, stp_modification_type: &ModificationType, is_stp: bool) {
    for ref mut train in trains.iter_mut() {
        // recurse on replacements
        trains_delete_assoc(&mut train.replacements, &other_train_id, &begin, &location, &location_suffix, &other_train_location_suffix, &stp_modification_type, is_stp);

        for ref mut train_location in train.route.iter_mut() {
            if train_location.id != location || train_location.id_suffix != *location_suffix {
                continue;
            }
            delete_single_vec_assocs(&mut train_location.divides_to_form, other_train_id, begin, other_train_location_suffix, stp_modification_type, is_stp, false);
            delete_single_vec_assocs(&mut train_location.joins_to, other_train_id, begin, other_train_location_suffix, stp_modification_type, is_stp, false);
            if let Some(ref mut assoc) = &mut train_location.becomes {
                delete_single_assoc_replacements_cancellations(assoc, other_train_id, begin, other_train_location_suffix, stp_modification_type, false);
                if *stp_modification_type == ModificationType::Insert && is_matching_assoc_for_modify_insertion(assoc, other_train_id, begin, other_train_location_suffix, is_stp, false) {
                    train_location.becomes = None;
                }
            }
        }
    }
}

fn trains_delete_rev_assoc(trains: &mut Vec<Train>, other_train_id: &str, begin: &DateTime::<Tz>, location: &str, location_suffix: &Option<String>, other_train_location_suffix: &Option<String>, stp_modification_type: &ModificationType, is_stp: bool) {
    for ref mut train in trains.iter_mut() {
        // recurse on replacements
        trains_delete_rev_assoc(&mut train.replacements, &other_train_id, &begin, &location, &location_suffix, &other_train_location_suffix, &stp_modification_type, is_stp);

        for ref mut train_location in train.route.iter_mut() {
            if train_location.id != location || train_location.id_suffix != *location_suffix {
                continue;
            }
            delete_single_vec_assocs(&mut train_location.divides_from, other_train_id, begin, other_train_location_suffix, stp_modification_type, is_stp, true);
            delete_single_vec_assocs(&mut train_location.is_joined_to_by, other_train_id, begin, other_train_location_suffix, stp_modification_type, is_stp, true);
            if let Some(ref mut assoc) = &mut train_location.forms_from {
                delete_single_assoc_replacements_cancellations(assoc, other_train_id, begin, other_train_location_suffix, stp_modification_type, true);
                if *stp_modification_type == ModificationType::Insert && is_matching_assoc_for_modify_insertion(assoc, other_train_id, begin, other_train_location_suffix, is_stp, true) {
                    train_location.forms_from = None;
                }
            }
        }
    }
}

fn trains_amend_assoc(trains: &mut Vec<Train>, other_train_id: &str, begin: &DateTime::<Tz>, end: &DateTime::<Tz>, days_of_week: &DaysOfWeek, location: &str, location_suffix: &Option<String>, other_train_location_suffix: &Option<String>, stp_modification_type: &ModificationType, is_stp: bool, day_diff: i8, for_passengers: bool) {
    for ref mut train in trains.iter_mut() {
        // recurse on replacements
        trains_amend_assoc(&mut train.replacements, &other_train_id, &begin, &end, &days_of_week, &location, &location_suffix, &other_train_location_suffix, &stp_modification_type, is_stp, day_diff, for_passengers);

        for ref mut train_location in train.route.iter_mut() {
            if train_location.id != location || train_location.id_suffix != *location_suffix {
                continue;
            }
            amend_single_vec_assocs(&mut train_location.divides_to_form, other_train_id, begin, end, days_of_week, other_train_location_suffix, stp_modification_type, is_stp, day_diff, for_passengers);
            amend_single_vec_assocs(&mut train_location.joins_to, other_train_id, begin, end, days_of_week, other_train_location_suffix, stp_modification_type, is_stp, day_diff, for_passengers);
            if let Some(ref mut assoc) = &mut train_location.becomes {
                if *stp_modification_type == ModificationType::Insert && is_matching_assoc_for_modify_insertion(assoc, other_train_id, begin, other_train_location_suffix, is_stp, false) {
                    amend_individual_assoc(assoc, begin, end, days_of_week, day_diff, for_passengers);
                }
                amend_single_assoc_replacements_cancellations(assoc, other_train_id, begin, end, days_of_week, other_train_location_suffix, stp_modification_type, day_diff, for_passengers);
            }
        }
    }
}

fn trains_amend_rev_assoc(trains: &mut Vec<Train>, other_train_id: &str, begin: &DateTime::<Tz>, end: &DateTime::<Tz>, days_of_week: &DaysOfWeek, location: &str, location_suffix: &Option<String>, other_train_location_suffix: &Option<String>, stp_modification_type: &ModificationType, is_stp: bool, day_diff: i8, for_passengers: bool) {
    for ref mut train in trains.iter_mut() {
        // recurse on replacements
        trains_amend_rev_assoc(&mut train.replacements, &other_train_id, &begin, &end, &days_of_week, &location, &location_suffix, &other_train_location_suffix, &stp_modification_type, is_stp, day_diff, for_passengers);

        for ref mut train_location in train.route.iter_mut() {
            if train_location.id != location || train_location.id_suffix != *location_suffix {
                continue;
            }
            amend_single_vec_assocs(&mut train_location.divides_from, other_train_id, begin, end, days_of_week, other_train_location_suffix, stp_modification_type, is_stp, day_diff, for_passengers);
            amend_single_vec_assocs(&mut train_location.is_joined_to_by, other_train_id, begin, end, days_of_week, other_train_location_suffix, stp_modification_type, is_stp, day_diff, for_passengers);
            if let Some(ref mut assoc) = &mut train_location.forms_from {
                if *stp_modification_type == ModificationType::Insert && is_matching_assoc_for_modify_insertion(assoc, other_train_id, begin, other_train_location_suffix, is_stp, false) {
                    amend_individual_assoc(assoc, begin, end, days_of_week, day_diff, for_passengers);
                }
                amend_single_assoc_replacements_cancellations(assoc, other_train_id, begin, end, days_of_week, other_train_location_suffix, stp_modification_type, day_diff, for_passengers);
            }
        }
    }
}

fn trains_cancel_assoc(trains: &mut Vec<Train>, other_train_id: &str, begin: &DateTime::<Tz>, end: &DateTime::<Tz>, days_of_week: &DaysOfWeek, location: &str, location_suffix: &Option<String>, other_train_location_suffix: &Option<String>) {
    for ref mut train in trains.iter_mut() {
        // recurse on replacements
        trains_cancel_assoc(&mut train.replacements, &other_train_id, &begin, &end, &days_of_week, &location, &location_suffix, &other_train_location_suffix);

        for ref mut train_location in train.route.iter_mut() {
            if train_location.id == location && train_location.id_suffix == *location_suffix {
                cancel_single_vec_assocs(&mut train_location.divides_to_form, other_train_id, begin, end, days_of_week, other_train_location_suffix, false);
                cancel_single_vec_assocs(&mut train_location.joins_to, other_train_id, begin, end, days_of_week, other_train_location_suffix, false);
                if let Some(assoc) = &mut train_location.becomes {
                    cancel_single_assoc(assoc, other_train_id, begin, end, days_of_week, other_train_location_suffix, false);
                }
            }
        }
    }
}

fn trains_cancel_rev_assoc(trains: &mut Vec<Train>, other_train_id: &str, begin: &DateTime::<Tz>, end: &DateTime::<Tz>, days_of_week: &DaysOfWeek, location: &str, location_suffix: &Option<String>, other_train_location_suffix: &Option<String>) {
    for ref mut train in trains.iter_mut() {
        // recurse on replacements
        trains_cancel_rev_assoc(&mut train.replacements, &other_train_id, &begin, &end, &days_of_week, &location, &location_suffix, &other_train_location_suffix);

        for ref mut train_location in train.route.iter_mut() {
            if train_location.id == location && train_location.id_suffix == *location_suffix {
                cancel_single_vec_assocs(&mut train_location.divides_from, other_train_id, begin, end, days_of_week, other_train_location_suffix, true);
                cancel_single_vec_assocs(&mut train_location.is_joined_to_by, other_train_id, begin, end, days_of_week, other_train_location_suffix, true);
                if let Some(assoc) = &mut train_location.forms_from {
                    cancel_single_assoc(assoc, other_train_id, begin, end, days_of_week, other_train_location_suffix, true);
                }
            }
        }
    }
}

fn trains_replace_assoc(trains: &mut Vec<Train>, other_train_id: &str, location: &str, location_suffix: &Option<String>, other_train_location_suffix: &Option<String>, new_assoc: &AssociationNode) {
    for ref mut train in trains.iter_mut() {
        // recurse on replacements
        trains_replace_assoc(&mut train.replacements, &other_train_id, &location, &location_suffix, &other_train_location_suffix, &new_assoc);

        for ref mut train_location in train.route.iter_mut() {
            if train_location.id == location && train_location.id_suffix == *location_suffix {
                replace_single_vec_assocs(&mut train_location.divides_to_form, other_train_id, other_train_location_suffix, new_assoc);
                replace_single_vec_assocs(&mut train_location.joins_to, other_train_id, other_train_location_suffix, new_assoc);
                if let Some(assoc) = &mut train_location.becomes {
                    if other_train_id == assoc.other_train_id && *other_train_location_suffix == assoc.other_train_location_id_suffix {
                        // check for no overlapping days at all
                        if !check_date_applicability(&assoc.validity[0], &assoc.days, new_assoc.validity[0].valid_begin, new_assoc.validity[0].valid_end, &new_assoc.days) {
                            continue;
                        }
                        assoc.replacements.push(new_assoc.clone());
                    }
                }
            }
        }
    }
}

fn trains_replace_rev_assoc(trains: &mut Vec<Train>, other_train_id: &str, location: &str, location_suffix: &Option<String>, other_train_location_suffix: &Option<String>, new_assoc: &AssociationNode) {
    for ref mut train in trains.iter_mut() {
        // recurse on replacements
        trains_replace_rev_assoc(&mut train.replacements, &other_train_id, &location, &location_suffix, &other_train_location_suffix, &new_assoc);

        for ref mut train_location in train.route.iter_mut() {
            if train_location.id == location && train_location.id_suffix == *location_suffix {
                replace_single_vec_assocs(&mut train_location.divides_from, other_train_id, other_train_location_suffix, new_assoc);
                replace_single_vec_assocs(&mut train_location.is_joined_to_by, other_train_id, other_train_location_suffix, new_assoc);
                if let Some(assoc) = &mut train_location.forms_from {
                    if other_train_id == assoc.other_train_id && *other_train_location_suffix == assoc.other_train_location_id_suffix {
                        // check for no overlapping days at all
                        if !check_date_applicability(&assoc.validity[0], &assoc.days, new_assoc.validity[0].valid_begin, new_assoc.validity[0].valid_end, &new_assoc.days) {
                            continue;
                        }
                        assoc.replacements.push(new_assoc.clone());
                    }
                }
            }
        }
    }
}

fn read_modification_type(modification_slice: &str, number: u64, column: usize) -> Result<ModificationType, CifError> {
    match modification_slice {
        "N" => Ok(ModificationType::Insert),
        "D" => Ok(ModificationType::Delete),
        "R" => Ok(ModificationType::Amend),
        x => Err(CifError { error_type: CifErrorType::InvalidTransactionType(x.to_string()), line: number, column: column } ),
    }
}

fn read_stp_indicator(stp_slice: &str, number: u64, column: usize) -> Result<(ModificationType, bool), CifError> {
    let stp_modification_type = match stp_slice {
        " " => ModificationType::Insert,
        "P" => ModificationType::Insert,
        "N" => ModificationType::Insert,
        "O" => ModificationType::Amend,
        "C" => ModificationType::Delete,
        x => return Err(CifError { error_type: CifErrorType::InvalidStpIndicator(x.to_string()), line: number, column: column } ),
    };
    let is_stp = match stp_slice {
        " " => false,
        "P" => false,
        "N" => true,
        "O" => true,
        "C" => true,
        x => return Err(CifError { error_type: CifErrorType::InvalidStpIndicator(x.to_string()), line: number, column: column } ),
    };

    return Ok((stp_modification_type, is_stp))
}

fn read_date(date_slice: &str, number: u64, column: usize) -> Result<DateTime::<Tz>, CifError> {
    let parsed_date = NaiveDate::parse_from_str(date_slice, "%y%m%d");
    let parsed_date = match parsed_date {
        Ok(x) => x,
        Err(x) => return Err(CifError { error_type: CifErrorType::ChronoParseError(x), line: number, column: column }),
    };
    Ok(London.from_local_datetime(&parsed_date.and_hms_opt(0, 0, 0).unwrap()).unwrap())
}

fn read_optional_string(slice: &str) -> Option<String> {
    if slice.chars().fold(true, |acc, x| acc && x == ' ') {
        None
    }
    else {
        Some(slice.to_string())
    }
}

fn read_days_of_week(slice: &str, number: u64, column: usize) -> Result<DaysOfWeek, CifError> {
    if slice.chars().fold(false, |acc, x| acc || (x != '0' && x != '1')) {
        Err(CifError { error_type: CifErrorType::InvalidDaysOfWeek(slice.to_string()), line: number, column: column })
    }
    else {
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

impl CifImporter {
    pub fn new() -> CifImporter {
        CifImporter { ..Default::default() }
    }

    fn delete_unwritten_assocs(&mut self, main_train_id: &str, location: &str, location_suffix: &Option<String>, other_train_id: &str, begin: &DateTime::<Tz>, other_train_location_suffix: &Option<String>, stp_modification_type: &ModificationType, is_stp: bool, use_rev: bool) {
        let old_assoc = self.unwritten_assocs.remove(&(main_train_id.to_string(), location.to_string(), location_suffix.clone()));
        let mut old_assoc = match old_assoc {
            None => vec![],
            Some(x) => x,
        };

        if *stp_modification_type == ModificationType::Insert {
            old_assoc.retain(|(assoc, _category)| !is_matching_assoc_for_modify_insertion(assoc, other_train_id, &begin, &other_train_location_suffix, is_stp, use_rev));
        }
        else {
            for (ref mut assoc, ref _category) in old_assoc.iter_mut() {
                delete_single_assoc_replacements_cancellations(assoc, other_train_id, &begin, &other_train_location_suffix, &stp_modification_type, use_rev);
            }
        }

        self.unwritten_assocs.insert((main_train_id.to_string(), location.to_string(), location_suffix.clone()), old_assoc);
    }

    fn cancel_unwritten_assocs(&mut self, main_train_id: &str, location: &str, location_suffix: &Option<String>, other_train_id: &str, begin: &DateTime::<Tz>, end: &DateTime::<Tz>, days_of_week: &DaysOfWeek, other_train_location_suffix: &Option<String>, use_rev: bool) {
        let old_assoc = self.unwritten_assocs.remove(&(main_train_id.to_string(), location.to_string(), location_suffix.clone()));
        let mut old_assoc = match old_assoc {
            None => vec![],
            Some(x) => x,
        };

        for (ref mut assoc, ref _category) in old_assoc.iter_mut() {
            cancel_single_assoc(assoc, other_train_id, begin, end, days_of_week, other_train_location_suffix, use_rev);
        }

        self.unwritten_assocs.insert((main_train_id.to_string(), location.to_string(), location_suffix.clone()), old_assoc);
    }

    fn amend_unwritten_assocs(&mut self, main_train_id: &str, location: &str, location_suffix: &Option<String>, other_train_id: &str, begin: &DateTime::<Tz>, end: &DateTime::<Tz>, days_of_week: &DaysOfWeek, other_train_location_suffix: &Option<String>, stp_modification_type: &ModificationType, is_stp: bool, day_diff: i8, for_passengers: bool, category: &AssociationCategory) {
        let old_assoc = self.unwritten_assocs.remove(&(main_train_id.to_string(), location.to_string(), location_suffix.clone()));
        let mut old_assoc = match old_assoc {
            None => vec![],
            Some(x) => x,
        };

        for (ref mut assoc, ref mut old_category) in old_assoc.iter_mut() {
            if *stp_modification_type == ModificationType::Insert {
                if is_matching_assoc_for_modify_insertion(assoc, other_train_id, begin, other_train_location_suffix, is_stp, false) {
                    amend_individual_assoc(assoc, begin, end, days_of_week, day_diff, for_passengers);
                    *old_category = *category
                }
            }
            else {
                amend_single_assoc_replacements_cancellations(assoc, other_train_id, begin, end, days_of_week, other_train_location_suffix, stp_modification_type, day_diff, for_passengers);
            }
        }

        self.unwritten_assocs.insert((main_train_id.to_string(), location.to_string(), location_suffix.clone()), old_assoc);
    }

    fn replace_unwritten_assocs(&mut self, main_train_id: &str, location: &str, location_suffix: &Option<String>, other_train_id: &str, other_train_location_suffix: &Option<String>, new_assoc: &AssociationNode) {
        let old_assoc = self.unwritten_assocs.remove(&(main_train_id.to_string(), location.to_string(), location_suffix.clone()));
        let mut old_assoc = match old_assoc {
            None => vec![],
            Some(x) => x,
        };

        for (ref mut assoc, ref _category) in old_assoc.iter_mut() {
            if other_train_id == assoc.other_train_id && *other_train_location_suffix == assoc.other_train_location_id_suffix {
                // check for no overlapping days at all
                if !check_date_applicability(&assoc.validity[0], &assoc.days, new_assoc.validity[0].valid_begin, new_assoc.validity[0].valid_end, &new_assoc.days) {
                    continue;
                }
                assoc.replacements.push(new_assoc.clone());
            }
        }

        self.unwritten_assocs.insert((main_train_id.to_string(), location.to_string(), location_suffix.clone()), old_assoc);
    }

    fn read_association(&mut self, line: &str, mut schedule: Schedule, number: u64) -> Result<Schedule, CifError> {
        let modification_type = read_modification_type(&line[2..3], number, 2)?;
        let (stp_modification_type, is_stp) = read_stp_indicator(&line[79..80], number, 79)?;

        let main_train_id = &line[3..9];
        let other_train_id = &line[9..15];
        let begin = read_date(&line[15..21], number, 15)?;
        let location = &line[37..44];
        let location_suffix = read_optional_string(&line[44..45]);
        let other_train_location_suffix = read_optional_string(&line[45..46]);

        // At this stage we have all the data we need for a simple delete, so handle this here
        //
        // Note these are NOT the same as STP cancels and indeed handled completely differently
        if modification_type == ModificationType::Delete {
            // first find any committed associations and delete
            trains_delete_assoc(schedule.trains.get_mut(main_train_id).as_mut().unwrap_or(&mut &mut vec![]), &other_train_id, &begin, &location, &location_suffix, &other_train_location_suffix, &stp_modification_type, is_stp);
            trains_delete_rev_assoc(schedule.trains.get_mut(other_train_id).as_mut().unwrap_or(&mut &mut vec![]), &main_train_id, &begin, &location, &other_train_location_suffix, &location_suffix, &stp_modification_type, is_stp);

            // now delete from unwritten associations
            self.delete_unwritten_assocs(main_train_id, location, &location_suffix, other_train_id, &begin, &other_train_location_suffix, &stp_modification_type, is_stp, false);
            self.delete_unwritten_assocs(other_train_id, location, &other_train_location_suffix, main_train_id, &begin, &location_suffix, &stp_modification_type, is_stp, true);

            return Ok(schedule);
        }

        let end = read_date(&line[21..27], number, 21)?;
        let days_of_week = read_days_of_week(&line[27..34], number, 27)?;

        // Now we handle STP cancellations; these are where long-running
        // associations are deleted as a one-off
        if stp_modification_type == ModificationType::Delete && modification_type == ModificationType::Insert {
            // cancel written ones
            trains_cancel_assoc(schedule.trains.get_mut(main_train_id).as_mut().unwrap_or(&mut &mut vec![]), &other_train_id, &begin, &end, &days_of_week, &location, &location_suffix, &other_train_location_suffix);
            trains_cancel_rev_assoc(schedule.trains.get_mut(other_train_id).as_mut().unwrap_or(&mut &mut vec![]), &main_train_id, &begin, &end, &days_of_week, &location, &other_train_location_suffix, &location_suffix);

            // now cancel from unwritten associations
            self.cancel_unwritten_assocs(main_train_id, location, &location_suffix, other_train_id, &begin, &end, &days_of_week, &other_train_location_suffix, false);
            self.cancel_unwritten_assocs(other_train_id, location, &other_train_location_suffix, main_train_id, &begin, &end, &days_of_week, &location_suffix, true);

            return Ok(schedule);
        }

        let day_diff = match &line[36..37] {
            "S" => 0,
            "N" => 1,
            "P" => -1,
            x => return Err(CifError { error_type: CifErrorType::InvalidAssociationDateIndicator(x.to_string()), line: number, column: 36 } ),
        };
        let for_passengers = match &line[47..48] {
            "P" => true,
            "O" => false,
            x => return Err(CifError { error_type: CifErrorType::InvalidAssociationType(x.to_string()), line: number, column: 47 } ),
        };

        let category = match &line[34..36] {
            "JJ" => AssociationCategory::Join,
            "VV" => AssociationCategory::Divide,
            "NP" => AssociationCategory::Next,
            x => return Err(CifError { error_type: CifErrorType::InvalidAssociationCategory(x.to_string()), line: number, column: 34 } ),
        };

        let rev_days_of_week = rev_days(&days_of_week, day_diff);
        let rev_begin = rev_date(&begin, day_diff);
        let rev_end = rev_date(&end, day_diff);
        let rev_category = match category {
            AssociationCategory::Join => AssociationCategory::IsJoinedToBy,
            AssociationCategory::Divide => AssociationCategory::DividesFrom,
            AssociationCategory::Next => AssociationCategory::FormsFrom,
            _ => panic!("Invalid association category")
        };

        if modification_type == ModificationType::Amend {
            // first find any committed associations and modify
            trains_amend_assoc(schedule.trains.get_mut(main_train_id).as_mut().unwrap_or(&mut &mut vec![]), &other_train_id, &begin, &end, &days_of_week, &location, &location_suffix, &other_train_location_suffix, &stp_modification_type, is_stp, day_diff, for_passengers);
            trains_amend_rev_assoc(schedule.trains.get_mut(other_train_id).as_mut().unwrap_or(&mut &mut vec![]), &main_train_id, &rev_begin, &rev_end, &rev_days_of_week, &location, &other_train_location_suffix, &location_suffix, &stp_modification_type, is_stp, -day_diff, for_passengers);

            // now amend unwritten associations
            self.amend_unwritten_assocs(main_train_id, location, &location_suffix, other_train_id, &begin, &end, &days_of_week, &other_train_location_suffix, &stp_modification_type, is_stp, day_diff, for_passengers, &category);
            self.amend_unwritten_assocs(other_train_id, location, &other_train_location_suffix, main_train_id, &rev_begin, &rev_end, &rev_days_of_week, &location_suffix, &stp_modification_type, is_stp, -day_diff, for_passengers, &rev_category);

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
            source: Some(if is_stp { TrainSource::ShortTerm } else { TrainSource::LongTerm }),
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
            source: Some(if is_stp { TrainSource::ShortTerm } else { TrainSource::LongTerm }),
        };


        if modification_type == ModificationType::Insert && stp_modification_type == ModificationType::Insert {
            // As trains might not all have appeared yet, we temporarily add to unwritten_assocs
            self.unwritten_assocs.entry((main_train_id.to_string(), location.to_string(), location_suffix)).or_insert(vec![]).push((new_assoc, category));
            self.unwritten_assocs.entry((other_train_id.to_string(), location.to_string(), other_train_location_suffix)).or_insert(vec![]).push((new_rev_assoc, rev_category));

            return Ok(schedule);
        }

        if stp_modification_type == ModificationType::Amend {
            // first find any committed associations and modify
            trains_replace_assoc(schedule.trains.get_mut(main_train_id).as_mut().unwrap_or(&mut &mut vec![]), &other_train_id, &location, &location_suffix, &other_train_location_suffix, &new_assoc);
            trains_replace_rev_assoc(schedule.trains.get_mut(other_train_id).as_mut().unwrap_or(&mut &mut vec![]), &main_train_id, &location, &other_train_location_suffix, &location_suffix, &new_rev_assoc);

            self.replace_unwritten_assocs(&main_train_id, &location, &location_suffix, &other_train_id, &other_train_location_suffix, &new_assoc);
            self.replace_unwritten_assocs(&other_train_id, &location, &other_train_location_suffix, &main_train_id, &location_suffix, &new_rev_assoc);

            return Ok(schedule);
        }

        Ok(schedule)
    }

    fn read_basic_schedule(&mut self, line: &str, mut schedule: Schedule, number: u64) -> Result<Schedule, CifError> {
        let modification_type = read_modification_type(&line[2..3], number, 2)?;
        let (stp_modification_type, is_stp) = read_stp_indicator(&line[79..80], number, 79)?;

        let main_train_id = &line[3..9];
        let begin = read_date(&line[9..15], number, 9)?;

        // At this stage we have all the data we need for a simple delete, so handle this here
        //
        // Note these are NOT the same as STP cancels and indeed handled completely differently
        if matches!(modification_type, ModificationType::Delete) {
            let old_trains = schedule.trains.remove(main_train_id);
            let mut old_trains = match old_trains {
                None => return Ok(schedule),
                Some(x) => x,
            };

            // first we delete main trains
            old_trains.retain(|train| {
                match (&stp_modification_type, &is_stp) {
                    (ModificationType::Insert, false) => train.source.unwrap() != TrainSource::LongTerm || train.validity[0].valid_begin != begin, // delete the entire train for deleted inserts
                    (ModificationType::Insert, true) => train.source.unwrap() != TrainSource::ShortTerm || train.validity[0].valid_begin != begin,
                    (ModificationType::Amend, _) => true, // for deleted amendments we never delete an item here
                    (ModificationType::Delete, _) => true, // for deleted cancellations we never delete an item here
                }
            });

            // now we clean up modifications/cancellations
            if matches!(stp_modification_type, ModificationType::Amend) || matches!(stp_modification_type, ModificationType::Delete) {
                for ref mut train in old_trains.iter_mut() {
                    match stp_modification_type {
                        ModificationType::Insert => panic!("Insert found where Amend or Cancel expected"),
                        ModificationType::Amend => train.replacements.retain(|replacement| replacement.validity[0].valid_begin != begin),
                        ModificationType::Delete => train.cancellations.retain(|(cancellation, _days_of_week)| cancellation.valid_begin != begin),
                    }
                }
            }

            schedule.trains.insert(main_train_id.to_string(), old_trains);

            return Ok(schedule);
        }

        let parsed_end_date = NaiveDate::parse_from_str(&line[15..21], "%y%m%d");
        let parsed_end_date = match parsed_end_date {
            Ok(x) => x,
            Err(x) => return Err(CifError { error_type: CifErrorType::ChronoParseError(x), line: number, column: 15 }),
        };
        let end = London.from_local_datetime(&parsed_end_date.and_hms_opt(0, 0, 0).unwrap()).unwrap();
        let days_of_week = DaysOfWeek {
            monday: &line[21..22] == "1",
            tuesday: &line[22..23] == "1",
            wednesday: &line[23..24] == "1",
            thursday: &line[24..25] == "1",
            friday: &line[25..26] == "1",
            saturday: &line[26..27] == "1",
            sunday: &line[27..28] == "1",
        };

        // Now we handle STP cancellations; these are where long-running
        // trains are deleted as a one-off
        if matches!(stp_modification_type, ModificationType::Delete) && matches!(modification_type, ModificationType::Insert) {
            let old_trains = schedule.trains.remove(main_train_id);
            let mut old_trains = match old_trains {
                None => return Ok(schedule),
                Some(x) => x,
            };

            // we cancel main trains
            for train in old_trains.iter_mut() {
                // check for no overlapping days at all
                if days_of_week.into_iter().zip(train.days_of_week.into_iter()).find(|(new_day, train_day)| *new_day && *train_day).is_none() {
                    continue;
                }
                let new_begin = if begin > train.validity[0].valid_begin {
                    begin.clone()
                }
                else {
                    train.validity[0].valid_begin.clone()
                };
                let new_end = if end < train.validity[0].valid_end {
                    end.clone()
                }
                else {
                    train.validity[0].valid_end.clone()
                };
                if new_end < new_begin {
                    continue;
                }
                let new_cancel = TrainValidityPeriod {
                    valid_begin: new_begin,
                    valid_end: new_end,
                };
                train.cancellations.push((new_cancel, days_of_week.clone()))
            }

            schedule.trains.insert(main_train_id.to_string(), old_trains);

            return Ok(schedule);
        }

        let train_status = match &line[29..30] {
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
            x => return Err(CifError { error_type: CifErrorType::InvalidTrainStatus(x.to_string()), line: number, column: 29 } ),
        };

        let train_type = match &line[30..32] {
            "OL" => TrainType::Metro,
            "OU" => TrainType::UnadvertisedPassenger,
            "OO" => TrainType::OrdinaryPassenger,
            "OS" => TrainType::Staff,
            "OW" => TrainType::Mixed,
            "XC" => TrainType::InternationalPassenger,
            "XD" => TrainType::InternationalSleeperPassenger,
            "XI" => TrainType::InternationalPassenger,
            "XR" => TrainType::CarCarryingPassenger,
            "XU" => TrainType::UnadvertisedExpressPassenger,
            "XX" => TrainType::ExpressPassenger,
            "XZ" => TrainType::SleeperPassenger,
            "BR" => TrainType::ReplacementBus,
            "BS" => TrainType::ServiceBus,
            "SS" => TrainType::Ship,
            "EE" => TrainType::EmptyPassenger,
            "EL" => TrainType::EmptyMetro,
            "ES" => TrainType::EmptyPassengerAndStaff,
            "JJ" => TrainType::Post,
            "PM" => TrainType::Parcels,
            "PP" => TrainType::Parcels,
            "PV" => TrainType::EmptyNonPassenger,
            "DD" => TrainType::FreightDepartmental,
            "DH" => TrainType::FreightCivilEngineer,
            "DI" => TrainType::FreightMechanicalElectricalEngineer,
            "DQ" => TrainType::FreightStores,
            "DT" => TrainType::FreightTest,
            "DY" => TrainType::FreightSignalTelecoms,
            "ZB" => TrainType::LocomotiveBrakeVan,
            "ZZ" => TrainType::Locomotive,
            "J2" => TrainType::FreightAutomotiveComponents,
            "H2" => TrainType::FreightAutomotiveVehicles,
            "J6" => TrainType::FreightWagonloadBuildingMaterials,
            "J5" => TrainType::FreightChemicals,
            "J3" => TrainType::FreightEdibleProducts,
            "J9" => TrainType::FreightIntermodalContracts,
            "H9" => TrainType::FreightIntermodalOther,
            "H8" => TrainType::FreightInternational,
            "J8" => TrainType::FreightMerchandise,
            "J4" => TrainType::FreightIndustrialMinerals,
            "A0" => TrainType::FreightCoalDistributive,
            "E0" => TrainType::FreightCoalElectricity,
            "B0" => TrainType::FreightNuclear,
            "B1" => TrainType::FreightMetals,
            "B4" => TrainType::FreightAggregates,
            "B5" => TrainType::FreightWaste,
            "B6" => TrainType::FreightTrainloadBuildingMaterials,
            "B7" => TrainType::FreightPetroleum,
            "H0" => TrainType::FreightInternationalMixed,
            "H1" => TrainType::FreightInternationalIntermodal,
            "H3" => TrainType::FreightInternationalAutomotive,
            "H4" => TrainType::FreightInternationalContract,
            "H5" => TrainType::FreightInternationalHaulmark,
            "H6" => TrainType::FreightInternationalJointVenture,
            "  " => match train_status {
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
            },
            x => return Err(CifError { error_type: CifErrorType::InvalidTrainCategory(x.to_string()), line: number, column: 30 } ),
        };

        let public_id = &line[32..36];
        let headcode = match &line[36..40] {
            "    " => None,
            x => Some(x.to_string()),
        };
        let service_group = &line[41..49];

        let power_type = match &line[50..53] {
            "D  " => Some(TrainPower::DieselLocomotive),
            "DEM" => Some(TrainPower::DieselElectricMultipleUnit),
            "DMU" => match &line[53..54] {
                "D" => Some(TrainPower::DieselMechanicalMultipleUnit),
                "V" => Some(TrainPower::DieselElectricMultipleUnit),
                _   => Some(TrainPower::DieselHydraulicMultipleUnit),
            },
            "E  " => Some(TrainPower::ElectricLocomotive),
            "ED " => Some(TrainPower::ElectricAndDieselLocomotive),
            "EML" => Some(TrainPower::ElectricMultipleUnitWithLocomotive),
            "EMU" => Some(TrainPower::ElectricMultipleUnit),
            "HST" => Some(TrainPower::DieselElectricMultipleUnit),
            "   " => None,
            x => return Err(CifError { error_type: CifErrorType::InvalidTrainPower(x.to_string()), line: number, column: 50 } ),
        };

        let speed_mph = match &line[57..60] {
            "   " => None,
            x => match x.parse::<u16>() {
                Ok(speed) => Some(speed),
                Err(_) => return Err(CifError { error_type: CifErrorType::InvalidSpeed(line[57..60].to_string()), line: number, column: 57 } ),
            },
        };

        let speed_m_per_s = match speed_mph {
            Some(x) => Some(f64::from(x) * (1609.344 / (60. * 60.))),
            None => None,
        };

        let mut operating_characteristics = OperatingCharacteristics { ..Default::default() };
        let mut runs_as_required = false;

        for chr in line[60..66].chars() {
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
                x => return Err(CifError { error_type: CifErrorType::InvalidOperatingCharacteristic(x.to_string()), line: number, column: 60 } ),
            }
        }

        let timing_load_str = match &line[50..53] {
            "D  "       => match &line[53..57] {
                "    " => None,
                x      => if operating_characteristics.br_mark_four_coaches {
                    Some(format!("Diesel locomotive hauling {} tons of BR Mark 4 Coaches", x))
                }
                else {
                    Some(format!("Diesel locomotive hauling {} tons", x))
                },
            },
            "DEM"|"DMU" => match &line[53..57] {
                "69  " => Some("Class 172/0, 172/1, or 172/2 'Turbostar' DMU".to_string()),
                "A   " => Some("Class 14x 2-axle 'Pacer' DMU".to_string()),
                "E   " => Some("Class 158, 168, 170, 172, or 175 'Express' DMU".to_string()),
                "N   " => Some("Class 165/0 'Network Turbo' DMU".to_string()),
                "S   " => Some("Class 150, 153, 155, or 156 'Sprinter' DMU".to_string()),
                "T   " => Some("Class 165/1 or 166 'Network Turbo' DMU".to_string()),
                "V   " => Some("Class 220 or 221 'Voyager' DMU".to_string()),
                "X   " => Some("Class 159 'South Western Turbo' DMU".to_string()),
                "D1  " => Some("Vacuum-braked DMU with power car and trailer".to_string()),
                "D2  " => Some("Vacuum-braked DMU with two power cars and trailer".to_string()),
                "D3  " => Some("Vacuum-braked DMU with two power cars".to_string()),
                "195 " => Some("Class 195 'Civity' DMU".to_string()),
                "196 " => Some("Class 196 'Civity' DMU".to_string()),
                "197 " => Some("Class 197 'Civity' DMU".to_string()),
                "755 " => Some("Class 755 'FLIRT' bi-mode running on diesel".to_string()),
                "777 " => Some("Class 777/1 'METRO' bi-mode running on battery".to_string()),
                "800 " => Some("Class 800 'Azuma' bi-mode running on diesel".to_string()),
                "802 " => Some("Class 800/802 'IET/Nova 1/Paragon' bi-mode running on diesel".to_string()),
                "805 " => Some("Class 805 'Hitachi AT300' bi-mode running on diesel".to_string()),
                "1400" => Some("Diesel locomotive hauling 1400 tons".to_string()), // lol
                "    " => None,
                x => return Err(CifError { error_type: CifErrorType::InvalidTimingLoad(x.to_string()), line: number, column: 53 } ),
            },
            "E  "       => match &line[53..57] {
                "325 " => Some("Class 325 Parcels EMU".to_string()),
                "    " => None,
                x      => if operating_characteristics.br_mark_four_coaches {
                    Some(format!("Electric locomotive hauling {} tons of BR Mark 4 Coaches", x))
                }
                else {
                    Some(format!("Electric locomotive hauling {} tons", x))
                },
            },
            "ED "       => match &line[53..57] {
                "    " => None,
                x      => if operating_characteristics.br_mark_four_coaches {
                    Some(format!("Electric and diesel locomotive hauling {} tons of BR Mark 4 Coaches", x))
                }
                else {
                    Some(format!("Electric and diesel locomotive hauling {} tons", x))
                },
            },
            "EML"|"EMU" => match &line[53..56] {
                "AT " => Some("EMU with accelerated timings".to_string()),
                "E  " => Some("Class 458 EMU".to_string()),
                "0  " => Some("Class 380 EMU".to_string()),
                "506" => Some("Class 350/1 EMU".to_string()),
                "   " => None,
                x => Some(format!("Class {} EMU", x)),
            },
            "HST"       => Some("High Speed Train (IC125)".to_string()),
            "   "       => None,
            x => return Err(CifError { error_type: CifErrorType::InvalidTrainPower(x.to_string()), line: number, column: 50 } ),
        };

        let seating_class = match &line[66..67] {
            " " => match train_type {
                TrainType::Bus|TrainType::ServiceBus|TrainType::ReplacementBus|TrainType::OrdinaryPassenger|TrainType::ExpressPassenger|TrainType::InternationalPassenger|TrainType::SleeperPassenger|TrainType::InternationalSleeperPassenger|TrainType::CarCarryingPassenger|TrainType::UnadvertisedPassenger|TrainType::UnadvertisedExpressPassenger|TrainType::Staff|TrainType::EmptyPassengerAndStaff|TrainType::Mixed|TrainType::Metro|TrainType::PassengerParcels|TrainType::Ship => Class::Both,
                _ => Class::None,
            },
            "B" => Class::Both,
            "F" => Class::First,
            "S" => Class::Standard,
            x => return Err(CifError { error_type: CifErrorType::InvalidSeatingClass(x.to_string()), line: number, column: 66 } ),
        };

        let first_seating = match seating_class {
            Class::Both => true,
            Class::First => true,
            Class::Standard => false,
            Class::None => false,
        };
        let standard_seating = match seating_class {
            Class::Both => true,
            Class::First => false,
            Class::Standard => true,
            Class::None => false,
        };

        let sleeper_class = match &line[67..68] {
            " " => Class::None,
            "B" => Class::Both,
            "F" => Class::First,
            "S" => Class::Standard,
            x => return Err(CifError { error_type: CifErrorType::InvalidSeatingClass(x.to_string()), line: number, column: 67 } ),
        };

        let first_sleepers = match sleeper_class {
            Class::Both => true,
            Class::First => true,
            Class::Standard => false,
            Class::None => false,
        };
        let standard_sleepers = match sleeper_class {
            Class::Both => true,
            Class::First => false,
            Class::Standard => true,
            Class::None => false,
        };

        let mut catering = Catering { ..Default::default() };
        let mut wheelchair_reservations = false;
        for chr in line[70..74].chars() {
            match chr {
                'C' => catering.buffet = true,
                'F' => catering.first_class_restaurant = true,
                'H' => catering.hot_food = true,
                'M' => catering.first_class_meal = true,
                'P' => wheelchair_reservations = true,
                'R' => catering.restaurant = true,
                'T' => catering.trolley = true,
                ' ' => (),
                x => return Err(CifError { error_type: CifErrorType::InvalidCatering(x.to_string()), line: number, column: 70 } ),
            }
        }

        let reservations = match &line[68..69] {
            "A" => Reservations {
                seats: if first_seating || standard_seating { ReservationField::Mandatory } else { ReservationField::NotApplicable },
                bicycles: ReservationField::NotMandatory,
                sleepers: if first_sleepers || standard_sleepers { ReservationField::Mandatory } else { ReservationField::NotApplicable },
                vehicles: if train_type == TrainType::CarCarryingPassenger { ReservationField::Mandatory } else { ReservationField::NotApplicable },
                wheelchairs: ReservationField::Mandatory,
            },
            "E" => Reservations {
                seats: if first_seating || standard_seating { ReservationField::NotMandatory } else { ReservationField::NotApplicable },
                bicycles: ReservationField::Mandatory,
                sleepers: if first_sleepers || standard_sleepers { ReservationField::NotMandatory } else { ReservationField::NotApplicable },
                vehicles: if train_type == TrainType::CarCarryingPassenger { ReservationField::Mandatory } else { ReservationField::NotApplicable },
                wheelchairs: if wheelchair_reservations { ReservationField::Possible } else { ReservationField::NotMandatory },
            },
            "R" => Reservations {
                seats: if first_seating || standard_seating { ReservationField::Recommended } else { ReservationField::NotApplicable },
                bicycles: ReservationField::NotMandatory,
                sleepers: if first_sleepers || standard_sleepers { ReservationField::Recommended } else { ReservationField::NotApplicable },
                vehicles: if train_type == TrainType::CarCarryingPassenger { ReservationField::Mandatory } else { ReservationField::NotApplicable },
                wheelchairs: ReservationField::Recommended,
            },
            "S" => Reservations {
                seats: if first_seating || standard_seating { ReservationField::Possible } else { ReservationField::NotApplicable },
                bicycles: ReservationField::NotMandatory,
                sleepers: if first_sleepers || standard_sleepers { ReservationField::Possible } else { ReservationField::NotApplicable },
                vehicles: if train_type == TrainType::CarCarryingPassenger { ReservationField::Mandatory } else { ReservationField::NotApplicable },
                wheelchairs: ReservationField::Possible,
            },
            " " => Reservations {
                seats: if first_seating || standard_seating { ReservationField::Impossible } else { ReservationField::NotApplicable },
                bicycles: ReservationField::NotMandatory,
                sleepers: if first_sleepers || standard_sleepers { ReservationField::Impossible } else { ReservationField::NotApplicable },
                vehicles: if train_type == TrainType::CarCarryingPassenger { ReservationField::Mandatory } else { ReservationField::NotApplicable },
                wheelchairs: if wheelchair_reservations { ReservationField::Possible } else { if first_seating || standard_seating || first_sleepers || standard_sleepers { ReservationField::Impossible } else { ReservationField::NotApplicable } },
            },
            x => return Err(CifError { error_type: CifErrorType::InvalidReservationType(x.to_string()), line: number, column: 68 } ),
        };

        let mut brand = None;
        for chr in line[74..78].chars() {
            match chr {
                'E' => brand = Some("Eurostar".to_string()),
                'U' => brand = Some("Alphaline".to_string()),
                ' ' => (),
                x => return Err(CifError { error_type: CifErrorType::InvalidBrand(x.to_string()), line: number, column: 74 } ),
            }
        }

        if matches!(modification_type, ModificationType::Insert) && matches!(stp_modification_type, ModificationType::Insert) {
            // we can write a (partial) train now, and continue updating it later.
            self.last_train = Some((main_train_id.to_string(), begin, stp_modification_type, is_stp));

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
                            id: line[50..57].to_string(),
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
                source: Some(if is_stp { TrainSource::ShortTerm } else { TrainSource::LongTerm }),
                runs_as_required,
                performance_monitoring: None,
                route: vec![],
            };

            schedule.trains.entry(main_train_id.to_string()).or_insert(vec![]).push(new_train);

            return Ok(schedule);
        }
        
        if matches!(modification_type, ModificationType::Amend) {
            // we can write a (partial) train now, and continue updating it later.
            self.last_train = Some((main_train_id.to_string(), begin, stp_modification_type, is_stp));

            let old_trains = schedule.trains.remove(main_train_id);
            let mut old_trains = match old_trains {
                None => return Ok(schedule),
                Some(x) => x,
            };

            // first we amend main trains
            for ref mut train in old_trains.iter_mut() {
                if match (&stp_modification_type, &is_stp) {
                        (ModificationType::Insert, false) => train.source.unwrap() == TrainSource::LongTerm && train.validity[0].valid_begin == begin,
                        (ModificationType::Insert, true) => train.source.unwrap() == TrainSource::ShortTerm && train.validity[0].valid_begin == begin,
                        (ModificationType::Amend, _) => false,
                        (ModificationType::Delete, _) => false,
                    } {
                    train.validity = vec![TrainValidityPeriod {
                        valid_begin: begin,
                        valid_end: end,
                    }];
                    train.days_of_week = days_of_week.clone();
                    train.runs_as_required = runs_as_required;
                    train.performance_monitoring = None;
                    train.route = vec![];
                    train.variable_train = VariableTrain {
                        train_type,
                        public_id: Some(public_id.to_string()),
                        headcode: headcode.clone(),
                        service_group: Some(service_group.to_string()),
                        power_type: power_type,
                        timing_allocation: match timing_load_str {
                            None => None,
                            Some(ref x) => Some(TrainAllocation {
                                id: line[50..57].to_string(),
                                description: x.clone(),
                                vehicles: None,
                            }),
                        },
                        actual_allocation: None,
                        timing_speed_m_per_s: speed_m_per_s,
                        operating_characteristics: operating_characteristics.clone(),
                        has_first_class_seats: first_seating,
                        has_second_class_seats: standard_seating,
                        has_first_class_sleepers: first_sleepers,
                        has_second_class_sleepers: standard_sleepers,
                        carries_vehicles: train_type == TrainType::CarCarryingPassenger,
                        reservations: reservations.clone(),
                        catering: catering.clone(),
                        brand: brand.clone(),
                        name: None,
                        uic_code: None,
                        operator: None,
                    };
                }
            }

            // now we clean up modifications/cancellations
            if matches!(stp_modification_type, ModificationType::Amend) || matches!(stp_modification_type, ModificationType::Delete) {
                for ref mut train in old_trains.iter_mut() {
                    if matches!(stp_modification_type, ModificationType::Amend) {
                        for replacement in train.replacements.iter_mut() {
                            if replacement.validity[0].valid_begin == begin {
                                replacement.validity = vec![TrainValidityPeriod {
                                    valid_begin: begin,
                                    valid_end: end,
                                }];
                                replacement.days_of_week = days_of_week.clone();
                                replacement.runs_as_required = runs_as_required;
                                replacement.performance_monitoring = None;
                                replacement.route = vec![];
                                replacement.variable_train = VariableTrain {
                                    train_type,
                                    public_id: Some(public_id.to_string()),
                                    headcode: headcode.clone(),
                                    service_group: Some(service_group.to_string()),
                                    power_type: power_type,
                                    timing_allocation: match timing_load_str {
                                        None => None,
                                        Some(ref x) => Some(TrainAllocation {
                                            id: line[50..57].to_string(),
                                            description: x.clone(),
                                            vehicles: None,
                                        }),
                                    },
                                    actual_allocation: None,
                                    timing_speed_m_per_s: speed_m_per_s,
                                    operating_characteristics: operating_characteristics.clone(),
                                    has_first_class_seats: first_seating,
                                    has_second_class_seats: standard_seating,
                                    has_first_class_sleepers: first_sleepers,
                                    has_second_class_sleepers: standard_sleepers,
                                    carries_vehicles: train_type == TrainType::CarCarryingPassenger,
                                    reservations: reservations.clone(),
                                    catering: catering.clone(),
                                    brand: brand.clone(),
                                    name: None,
                                    uic_code: None,
                                    operator: None,
                                };
                            }
                        }
                    }
                    else if matches!(stp_modification_type, ModificationType::Delete) {
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
                    else {
                        panic!("Insert found where amend or cancel expected");
                    }
                }
            }

            schedule.trains.insert(main_train_id.to_string(), old_trains);

            return Ok(schedule);
        }

        if matches!(stp_modification_type, ModificationType::Amend) {
            // we can write a (partial) train now, and continue updating it later.
            self.last_train = Some((main_train_id.to_string(), begin, stp_modification_type, is_stp));

            let old_trains = schedule.trains.remove(main_train_id);
            let mut old_trains = match old_trains {
                None => return Ok(schedule),
                Some(x) => x,
            };

            // we replace main trains
            for train in old_trains.iter_mut() {
                // check for no overlapping days at all
                if days_of_week.into_iter().zip(train.days_of_week.into_iter()).find(|(new_day, train_day)| *new_day && *train_day).is_none() {
                    continue;
                }
                let new_begin = if begin > train.validity[0].valid_begin {
                    begin.clone()
                }
                else {
                    train.validity[0].valid_begin.clone()
                };
                let new_end = if end < train.validity[0].valid_end {
                    end.clone()
                }
                else {
                    train.validity[0].valid_end.clone()
                };
                if new_end < new_begin {
                    continue;
                }
                let new_train = Train {
                    id: main_train_id.to_string(),
                    validity: vec![TrainValidityPeriod {
                        valid_begin: new_begin,
                        valid_end: new_end,
                    }],
                    cancellations: vec![],
                    replacements: vec![],
                    days_of_week,
                    variable_train: VariableTrain {
                        train_type,
                        public_id: Some(public_id.to_string()),
                        headcode: headcode.clone(),
                        service_group: Some(service_group.to_string()),
                        power_type: power_type,
                        timing_allocation: match timing_load_str {
                            None => None,
                            Some(ref x) => Some(TrainAllocation {
                                id: line[50..57].to_string(),
                                description: x.clone(),
                                vehicles: None,
                            }),
                        },
                        actual_allocation: None,
                        timing_speed_m_per_s: speed_m_per_s,
                        operating_characteristics: operating_characteristics.clone(),
                        has_first_class_seats: first_seating,
                        has_second_class_seats: standard_seating,
                        has_first_class_sleepers: first_sleepers,
                        has_second_class_sleepers: standard_sleepers,
                        carries_vehicles: train_type == TrainType::CarCarryingPassenger,
                        reservations: reservations.clone(),
                        catering: catering.clone(),
                        brand: brand.clone(),
                        name: None,
                        uic_code: None,
                        operator: None,
                    },
                    source: Some(if is_stp { TrainSource::ShortTerm } else { TrainSource::LongTerm }),
                    runs_as_required,
                    performance_monitoring: None,
                    route: vec![],
                };

                train.replacements.push(new_train)
            }

            schedule.trains.insert(main_train_id.to_string(), old_trains);

            return Ok(schedule);
        }

        Ok(schedule)
    }

    fn read_extended_schedule(&mut self, line: &str, mut schedule: Schedule, number: u64) -> Result<Schedule, CifError> {
        // at this stage we can only be in an insert or amend statement, for STP other than CAN. So
        // we find the train we are inserting or amending.

        let (main_train_id, begin, stp_modification_type, is_stp) = match &self.last_train {
            Some(x) => x,
            None => return Err(CifError { error_type: CifErrorType::UnexpectedRecordType("BX".to_string(), "No preceding BS".to_string()), line: number, column: 0 } ),
        };

        let ref mut trains = match schedule.trains.get_mut(main_train_id) {
            Some(x) => x,
            None => panic!("Unable to find last-written train"),
        };

        let ref mut train = match (&stp_modification_type, &is_stp) {
            (ModificationType::Insert, false) => trains.iter_mut().find(|train| train.source.unwrap() == TrainSource::LongTerm && train.validity[0].valid_begin == *begin),
            (ModificationType::Insert, true) => trains.iter_mut().find(|train| train.source.unwrap() == TrainSource::ShortTerm && train.validity[0].valid_begin == *begin),
            (ModificationType::Amend, _) => find_replacement_train(trains, begin),
            (ModificationType::Delete, _) => panic!("Unexpected train modification type"),
        };

        let train = match train {
            Some(x) => x,
            None => panic!("Unable to find last-written train"),
        };

        let uic_code = match &line[6..11] {
            "     " => None,
            x => Some(x.to_string()),
        };

        let atoc_code = &line[11..13];

        let train_operator_desc = match atoc_code {
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
            x => return Err(CifError { error_type: CifErrorType::InvalidTrainOperator(x.to_string()), line: number, column: 11 } ),
        };

        let performance_monitoring = match &line[13..14] {
            "Y" => true,
            "N" => false,
            x => return Err(CifError { error_type: CifErrorType::InvalidAtsCode(x.to_string()), line: number, column: 13 } ),
        };

        train.variable_train.uic_code = uic_code;
        train.variable_train.operator = Some(TrainOperator {
            id: atoc_code.to_string(),
            description: train_operator_desc,
        });
        train.performance_monitoring = Some(performance_monitoring);

        Ok(schedule)
    }

    fn read_location_origin(&mut self, line: &str, mut schedule: Schedule, number: u64) -> Result<Schedule, CifError> {
        // at this stage we can only be in an insert or amend statement, for STP other than CAN. So
        // we find the train we are inserting or amending.

        let (main_train_id, begin, stp_modification_type, is_stp) = match &self.last_train {
            Some(x) => x,
            None => return Err(CifError { error_type: CifErrorType::UnexpectedRecordType("LO".to_string(), "No preceding BS".to_string()), line: number, column: 0 } ),
        };

        let ref mut trains = match schedule.trains.get_mut(main_train_id) {
            Some(x) => x,
            None => panic!("Unable to find last-written train"),
        };

        let ref mut train = match (&stp_modification_type, &is_stp) {
            (ModificationType::Insert, false) => trains.iter_mut().find(|train| train.source.unwrap() == TrainSource::LongTerm && train.validity[0].valid_begin == *begin),
            (ModificationType::Insert, true) => trains.iter_mut().find(|train| train.source.unwrap() == TrainSource::ShortTerm && train.validity[0].valid_begin == *begin),
            (ModificationType::Amend, _) => find_replacement_train(trains, begin),
            (ModificationType::Delete, _) => panic!("Unexpected train modification type"),
        };

        let train = match train {
            Some(x) => x,
            None => panic!("Unable to find last-written train"),
        };

        if !train.route.is_empty() {
            return Err(CifError { error_type: CifErrorType::UnexpectedRecordType("LO".to_string(), "Train route not empty".to_string()), line: number, column: 0 } );
        }

        let location_id = &line[2..9];
        let location_suffix = match &line[9..10] {
            " " => None,
            x => Some(x.to_string()),
        };

        let wtt_dep = NaiveTime::parse_from_str(&line[10..14], "%H%M");
        let wtt_dep = match wtt_dep {
            Ok(x) => x,
            Err(x) => return Err(CifError { error_type: CifErrorType::ChronoParseError(x), line: number, column: 10 }),
        };
        let wtt_dep = wtt_dep + match &line[14..15] {
            "H" => Duration::seconds(30),
            " " => Duration::seconds(0),
            x => return Err(CifError { error_type: CifErrorType::InvalidMinuteFraction(x.to_string()), line: number, column: 14 }),
        };

        let pub_dep = NaiveTime::parse_from_str(&line[15..19], "%H%M");
        let pub_dep = match pub_dep {
            Ok(x) => x,
            Err(x) => return Err(CifError { error_type: CifErrorType::ChronoParseError(x), line: number, column: 15 }),
        };
        // amazingly, public departure times of midnight are impossible in Britain!
        let pub_dep = if pub_dep == NaiveTime::from_hms_opt(0, 0, 0).unwrap() {
            None
        }
        else {
            Some(pub_dep)
        };

        let platform = match &line[19..22] {
            "   " => None,
            x => Some(x.trim().to_string()),
        };

        let line_code = match &line[22..25] {
            "   " => None,
            x => Some(x.trim().to_string()),
        };

        let (eng_minutes, eng_seconds) = match (&line[25..26], &line[26..27], &line[25..27]) {
            (_, _, "  ") => (Ok(0), 0),
            (_, _, " H") => (Ok(0), 30),
            (x, " ", _) => (x.parse::<u32>(), 0),
            (x, "H", _) => (x.parse::<u32>(), 30),
            (_, _, x) => (x.parse::<u32>(), 0),
        };
        let eng_minutes = match eng_minutes {
            Ok(x) => x,
            Err(_) => return Err(CifError { error_type: CifErrorType::InvalidAllowance(line[25..27].to_string()), line: number, column: 25 }),
        };
        let eng_allowance = eng_minutes * 60 + eng_seconds;

        let (path_minutes, path_seconds) = match (&line[27..28], &line[28..29], &line[27..29]) {
            (_, _, "  ") => (Ok(0), 0),
            (_, _, " H") => (Ok(0), 30),
            (x, " ", _) => (x.parse::<u32>(), 0),
            (x, "H", _) => (x.parse::<u32>(), 30),
            (_, _, x) => (x.parse::<u32>(), 0),
        };
        let path_minutes = match path_minutes {
            Ok(x) => x,
            Err(_) => return Err(CifError { error_type: CifErrorType::InvalidAllowance(line[27..29].to_string()), line: number, column: 27 }),
        };
        let path_allowance = path_minutes * 60 + path_seconds;

        let mut activities = Activities { ..Default::default() };

        for activity in line[29..41].chars().chunks(2).into_iter().map(|chunk| chunk.collect::<String>()) {
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
                x => return Err(CifError { error_type: CifErrorType::InvalidActivity(x.to_string()), line: number, column: 29 }),
            };
        };

        let (performance_minutes, performance_seconds) = match (&line[41..42], &line[42..43], &line[41..43]) {
            (_, _, "  ") => (Ok(0), 0),
            (_, _, " H") => (Ok(0), 30),
            (x, " ", _) => (x.parse::<u32>(), 0),
            (x, "H", _) => (x.parse::<u32>(), 30),
            (_, _, x) => (x.parse::<u32>(), 0),
        };
        let performance_minutes = match performance_minutes {
            Ok(x) => x,
            Err(_) => return Err(CifError { error_type: CifErrorType::InvalidAllowance(line[41..43].to_string()), line: number, column: 41 }),
        };
        let performance_allowance = performance_minutes * 60 + performance_seconds;

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
            performance_allowance_s: Some(performance_allowance),
            activities,
            change_en_route: None,
            divides_to_form: vec![],
            joins_to: vec![],
            becomes: None,
            divides_from: vec![],
            is_joined_to_by: vec![],
            forms_from: None,
        };

        train.route.push(new_location);

        Ok(schedule)
    }

    fn read_location_intermediate(&mut self, line: &str, mut schedule: Schedule, number: u64) -> Result<Schedule, CifError> {
        // at this stage we can only be in an insert or amend statement, for STP other than CAN. So
        // we find the train we are inserting or amending.

        let (main_train_id, begin, stp_modification_type, is_stp) = match &self.last_train {
            Some(x) => x,
            None => return Err(CifError { error_type: CifErrorType::UnexpectedRecordType("LI".to_string(), "No preceding BS".to_string()), line: number, column: 0 } ),
        };

        let ref mut trains = match schedule.trains.get_mut(main_train_id) {
            Some(x) => x,
            None => panic!("Unable to find last-written train"),
        };

        let ref mut train = match (&stp_modification_type, &is_stp) {
            (ModificationType::Insert, false) => trains.iter_mut().find(|train| train.source.unwrap() == TrainSource::LongTerm && train.validity[0].valid_begin == *begin),
            (ModificationType::Insert, true) => trains.iter_mut().find(|train| train.source.unwrap() == TrainSource::ShortTerm && train.validity[0].valid_begin == *begin),
            (ModificationType::Amend, _) => find_replacement_train(trains, begin),
            (ModificationType::Delete, _) => panic!("Unexpected train modification type"),
        };

        let train = match train {
            Some(x) => x,
            None => panic!("Unable to find last-written train"),
        };

        if train.route.is_empty() {
            return Err(CifError { error_type: CifErrorType::UnexpectedRecordType("LI".to_string(), "Train route is empty".to_string()), line: number, column: 0 } );
        }

        let (last_wtt_time, last_wtt_day) = match train.route.last().unwrap().working_dep {
            Some(x) => (x, train.route.last().unwrap().working_dep_day.unwrap()),
            None => (train.route.last().unwrap().working_pass.unwrap(), train.route.last().unwrap().working_pass_day.unwrap()),
        };

        let location_id = &line[2..9];
        let location_suffix = match &line[9..10] {
            " " => None,
            x => Some(x.to_string()),
        };

        match self.change_en_route {
            Some(_) => if (location_id.to_string(), location_suffix.clone()) != *self.cr_location.as_ref().unwrap() {
                return Err(CifError { error_type: CifErrorType::ChangeEnRouteLocationUnmatched((location_id.to_string(), location_suffix), self.cr_location.clone().unwrap()), line: number, column: 2 });
            },
            None => (),
        };

        let wtt_arr = match &line[10..15] {
            "     " => None,
            x => {
                let wtt = NaiveTime::parse_from_str(&x[0..4], "%H%M");
                let wtt = match wtt {
                    Ok(x) => x,
                    Err(x) => return Err(CifError { error_type: CifErrorType::ChronoParseError(x), line: number, column: 10 }),
                };
                Some(wtt + match &x[4..5] {
                    "H" => Duration::seconds(30),
                    " " => Duration::seconds(0),
                    x => return Err(CifError { error_type: CifErrorType::InvalidMinuteFraction(x.to_string()), line: number, column: 14 }),
                })
            },
        };
        let wtt_arr_day = match wtt_arr {
            Some(x) => if x < last_wtt_time {
                Some(last_wtt_day + 1)
            }
            else {
                Some(last_wtt_day)
            },
            None => None,
        };

        let wtt_dep = match &line[15..20] {
            "     " => None,
            x => {
                let wtt = NaiveTime::parse_from_str(&x[0..4], "%H%M");
                let wtt = match wtt {
                    Ok(x) => x,
                    Err(x) => return Err(CifError { error_type: CifErrorType::ChronoParseError(x), line: number, column: 15 }),
                };
                Some(wtt + match &x[4..5] {
                    "H" => Duration::seconds(30),
                    " " => Duration::seconds(0),
                    x => return Err(CifError { error_type: CifErrorType::InvalidMinuteFraction(x.to_string()), line: number, column: 19 }),
                })
            },
        };
        let wtt_dep_day = match wtt_dep {
            Some(x) => if x < last_wtt_time {
                Some(last_wtt_day + 1)
            }
            else {
                Some(last_wtt_day)
            },
            None => None,
        };

        let wtt_pass = match &line[20..25] {
            "     " => None,
            x => {
                let wtt = NaiveTime::parse_from_str(&x[0..4], "%H%M");
                let wtt = match wtt {
                    Ok(x) => x,
                    Err(x) => return Err(CifError { error_type: CifErrorType::ChronoParseError(x), line: number, column: 20 }),
                };
                Some(wtt + match &x[4..5] {
                    "H" => Duration::seconds(30),
                    " " => Duration::seconds(0),
                    x => return Err(CifError { error_type: CifErrorType::InvalidMinuteFraction(x.to_string()), line: number, column: 24 }),
                })
            },
        };
        let wtt_pass_day = match wtt_pass {
            Some(x) => if x < last_wtt_time {
                Some(last_wtt_day + 1)
            }
            else {
                Some(last_wtt_day)
            },
            None => None,
        };

        match (wtt_arr, wtt_dep, wtt_pass) {
            (None, None, Some(_)) => (),
            (Some(_), Some(_), None) => (),
            (_, _, _) => return Err(CifError { error_type: CifErrorType::InvalidWttTimesCombo, line: number, column: 10 }),
        };

        let pub_arr = NaiveTime::parse_from_str(&line[25..29], "%H%M");
        let pub_arr = match pub_arr {
            Ok(x) => x,
            Err(x) => return Err(CifError { error_type: CifErrorType::ChronoParseError(x), line: number, column: 25 }),
        };
        // amazingly, public departure times of midnight are impossible in Britain!
        let pub_arr = if pub_arr == NaiveTime::from_hms_opt(0, 0, 0).unwrap() {
            None
        }
        else {
            Some(pub_arr)
        };
        let pub_arr_day = match pub_arr {
            Some(x) => if x < last_wtt_time {
                Some(last_wtt_day + 1)
            }
            else {
                Some(last_wtt_day)
            },
            None => None,
        };

        let pub_dep = NaiveTime::parse_from_str(&line[29..33], "%H%M");
        let pub_dep = match pub_dep {
            Ok(x) => x,
            Err(x) => return Err(CifError { error_type: CifErrorType::ChronoParseError(x), line: number, column: 29 }),
        };
        // amazingly, public departure times of midnight are impossible in Britain!
        let pub_dep = if pub_dep == NaiveTime::from_hms_opt(0, 0, 0).unwrap() {
            None
        }
        else {
            Some(pub_dep)
        };
        let pub_dep_day = match pub_dep {
            Some(x) => if x < last_wtt_time {
                Some(last_wtt_day + 1)
            }
            else {
                Some(last_wtt_day)
            },
            None => None,
        };

        let platform = match &line[33..36] {
            "   " => None,
            x => Some(x.trim().to_string()),
        };

        let line_code = match &line[36..39] {
            "   " => None,
            x => Some(x.trim().to_string()),
        };
        let path_code = match &line[39..42] {
            "   " => None,
            x => Some(x.trim().to_string()),
        };

        let mut activities = Activities { ..Default::default() };

        for activity in line[42..54].chars().chunks(2).into_iter().map(|chunk| chunk.collect::<String>()) {
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
                x => return Err(CifError { error_type: CifErrorType::InvalidActivity(x.to_string()), line: number, column: 42 }),
            };
        };

        let (eng_minutes, eng_seconds) = match (&line[54..55], &line[55..56], &line[54..56]) {
            (_, _, "  ") => (Ok(0), 0),
            (_, _, " H") => (Ok(0), 30),
            (x, " ", _) => (x.parse::<u32>(), 0),
            (x, "H", _) => (x.parse::<u32>(), 30),
            (_, _, x) => (x.parse::<u32>(), 0),
        };
        let eng_minutes = match eng_minutes {
            Ok(x) => x,
            Err(_) => return Err(CifError { error_type: CifErrorType::InvalidAllowance(line[54..56].to_string()), line: number, column: 54 }),
        };
        let eng_allowance = eng_minutes * 60 + eng_seconds;

        let (path_minutes, path_seconds) = match (&line[56..57], &line[57..58], &line[56..58]) {
            (_, _, "  ") => (Ok(0), 0),
            (_, _, " H") => (Ok(0), 30),
            (x, " ", _) => (x.parse::<u32>(), 0),
            (x, "H", _) => (x.parse::<u32>(), 30),
            (_, _, x) => (x.parse::<u32>(), 0),
        };
        let path_minutes = match path_minutes {
            Ok(x) => x,
            Err(_) => return Err(CifError { error_type: CifErrorType::InvalidAllowance(line[56..58].to_string()), line: number, column: 56 }),
        };
        let path_allowance = path_minutes * 60 + path_seconds;

        let (performance_minutes, performance_seconds) = match (&line[58..59], &line[59..60], &line[58..60]) {
            (_, _, "  ") => (Ok(0), 0),
            (_, _, " H") => (Ok(0), 30),
            (x, " ", _) => (x.parse::<u32>(), 0),
            (x, "H", _) => (x.parse::<u32>(), 30),
            (_, _, x) => (x.parse::<u32>(), 0),
        };
        let performance_minutes = match performance_minutes {
            Ok(x) => x,
            Err(_) => return Err(CifError { error_type: CifErrorType::InvalidAllowance(line[58..60].to_string()), line: number, column: 58 }),
        };
        let performance_allowance = performance_minutes * 60 + performance_seconds;

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
            performance_allowance_s: Some(performance_allowance),
            activities,
            change_en_route: self.change_en_route.take(),
            divides_to_form: vec![],
            joins_to: vec![],
            becomes: None,
            divides_from: vec![],
            is_joined_to_by: vec![],
            forms_from: None,
        };

        self.cr_location = None;

        train.route.push(new_location);

        Ok(schedule)
    }

    fn read_location_terminating(&mut self, line: &str, mut schedule: Schedule, number: u64) -> Result<Schedule, CifError> {
        // at this stage we can only be in an insert or amend statement, for STP other than CAN. So
        // we find the train we are inserting or amending.

        let (main_train_id, begin, stp_modification_type, is_stp) = match &self.last_train {
            Some(x) => x,
            None => return Err(CifError { error_type: CifErrorType::UnexpectedRecordType("LT".to_string(), "No preceding BS".to_string()), line: number, column: 0 } ),
        };

        let ref mut trains = match schedule.trains.get_mut(main_train_id) {
            Some(x) => x,
            None => panic!("Unable to find last-written train"),
        };

        let ref mut train = match (&stp_modification_type, &is_stp) {
            (ModificationType::Insert, false) => trains.iter_mut().find(|train| train.source.unwrap() == TrainSource::LongTerm && train.validity[0].valid_begin == *begin),
            (ModificationType::Insert, true) => trains.iter_mut().find(|train| train.source.unwrap() == TrainSource::ShortTerm && train.validity[0].valid_begin == *begin),
            (ModificationType::Amend, _) => find_replacement_train(trains, begin),
            (ModificationType::Delete, _) => panic!("Unexpected train modification type"),
        };

        let train = match train {
            Some(x) => x,
            None => panic!("Unable to find last-written train"),
        };

        if train.route.is_empty() {
            return Err(CifError { error_type: CifErrorType::UnexpectedRecordType("LT".to_string(), "Train route is empty".to_string()), line: number, column: 0 } );
        }

        let (last_wtt_time, last_wtt_day) = match train.route.last().unwrap().working_dep {
            Some(x) => (x, train.route.last().unwrap().working_dep_day.unwrap()),
            None => (train.route.last().unwrap().working_pass.unwrap(), train.route.last().unwrap().working_pass_day.unwrap()),
        };

        // we can now unset the last_train as this should be the last message received for any
        // given train
        self.last_train = None;

        let location_id = &line[2..9];
        let location_suffix = match &line[9..10] {
            " " => None,
            x => Some(x.to_string()),
        };

        match self.change_en_route {
            Some(_) => if (location_id.to_string(), location_suffix.clone()) != *self.cr_location.as_ref().unwrap() {
                return Err(CifError { error_type: CifErrorType::ChangeEnRouteLocationUnmatched((location_id.to_string(), location_suffix), self.cr_location.clone().unwrap()), line: number, column: 2 });
            },
            None => (),
        };

        let wtt_arr = NaiveTime::parse_from_str(&line[10..14], "%H%M");
        let wtt_arr = match wtt_arr {
            Ok(x) => x,
            Err(x) => return Err(CifError { error_type: CifErrorType::ChronoParseError(x), line: number, column: 10 }),
        };
        let wtt_arr = wtt_arr + match &line[14..15] {
            "H" => Duration::seconds(30),
            " " => Duration::seconds(0),
            x => return Err(CifError { error_type: CifErrorType::InvalidMinuteFraction(x.to_string()), line: number, column: 14 }),
        };

        let wtt_arr_day = if wtt_arr < last_wtt_time {
            last_wtt_day + 1
        }
        else {
            last_wtt_day
        };

        let pub_arr = NaiveTime::parse_from_str(&line[15..19], "%H%M");
        let pub_arr = match pub_arr {
            Ok(x) => x,
            Err(x) => return Err(CifError { error_type: CifErrorType::ChronoParseError(x), line: number, column: 15 }),
        };
        // amazingly, public departure times of midnight are impossible in Britain!
        let pub_arr = if pub_arr == NaiveTime::from_hms_opt(0, 0, 0).unwrap() {
            None
        }
        else {
            Some(pub_arr)
        };

        let pub_arr_day = match pub_arr {
            Some(x) => if x < last_wtt_time {
                Some(last_wtt_day + 1)
            }
            else {
                Some(last_wtt_day)
            },
            None => None,
        };

        let platform = match &line[19..22] {
            "   " => None,
            x => Some(x.trim().to_string()),
        };

        let path_code = match &line[22..25] {
            "   " => None,
            x => Some(x.trim().to_string()),
        };

        let mut activities = Activities { ..Default::default() };

        for activity in line[25..37].chars().chunks(2).into_iter().map(|chunk| chunk.collect::<String>()) {
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
                x => return Err(CifError { error_type: CifErrorType::InvalidActivity(x.to_string()), line: number, column: 29 }),
            };
        };

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
            change_en_route: self.change_en_route.take(),
            divides_to_form: vec![],
            joins_to: vec![],
            becomes: None,
            divides_from: vec![],
            is_joined_to_by: vec![],
            forms_from: None,
        };

        self.cr_location = None;

        train.route.push(new_location);

        Ok(schedule)
    }

    fn read_change_en_route(&mut self, line: &str, mut schedule: Schedule, number: u64) -> Result<Schedule, CifError> {
        // at this stage we can only be in an insert or amend statement, for STP other than CAN. So
        // we find the train we are inserting or amending.

        let (main_train_id, begin, stp_modification_type, is_stp) = match &self.last_train {
            Some(x) => x,
            None => return Err(CifError { error_type: CifErrorType::UnexpectedRecordType("CR".to_string(), "No preceding BS".to_string()), line: number, column: 0 } ),
        };

        let ref mut trains = match schedule.trains.get_mut(main_train_id) {
            Some(x) => x,
            None => panic!("Unable to find last-written train"),
        };

        let ref mut train = match (&stp_modification_type, &is_stp) {
            (ModificationType::Insert, false) => trains.iter_mut().find(|train| train.source.unwrap() == TrainSource::LongTerm && train.validity[0].valid_begin == *begin),
            (ModificationType::Insert, true) => trains.iter_mut().find(|train| train.source.unwrap() == TrainSource::ShortTerm && train.validity[0].valid_begin == *begin),
            (ModificationType::Amend, _) => find_replacement_train(trains, begin),
            (ModificationType::Delete, _) => panic!("Unexpected train modification type"),
        };

        let train = match train {
            Some(x) => x,
            None => panic!("Unable to find last-written train"),
        };

        if train.route.is_empty() {
            return Err(CifError { error_type: CifErrorType::UnexpectedRecordType("CR".to_string(), "Train route is empty".to_string()), line: number, column: 0 } );
        }

        let location_id = &line[2..9];
        let location_suffix = match &line[9..10] {
            " " => None,
            x => Some(x.to_string()),
        };

        self.cr_location = Some((location_id.to_string(), location_suffix));

        let train_type = match &line[10..12] {
            "OL" => TrainType::Metro,
            "OU" => TrainType::UnadvertisedPassenger,
            "OO" => TrainType::OrdinaryPassenger,
            "OS" => TrainType::Staff,
            "OW" => TrainType::Mixed,
            "XC" => TrainType::InternationalPassenger,
            "XD" => TrainType::InternationalSleeperPassenger,
            "XI" => TrainType::InternationalPassenger,
            "XR" => TrainType::CarCarryingPassenger,
            "XU" => TrainType::UnadvertisedExpressPassenger,
            "XX" => TrainType::ExpressPassenger,
            "XZ" => TrainType::SleeperPassenger,
            "BR" => TrainType::ReplacementBus,
            "BS" => TrainType::ServiceBus,
            "SS" => TrainType::Ship,
            "EE" => TrainType::EmptyPassenger,
            "EL" => TrainType::EmptyMetro,
            "ES" => TrainType::EmptyPassengerAndStaff,
            "JJ" => TrainType::Post,
            "PM" => TrainType::Parcels,
            "PP" => TrainType::Parcels,
            "PV" => TrainType::EmptyNonPassenger,
            "DD" => TrainType::FreightDepartmental,
            "DH" => TrainType::FreightCivilEngineer,
            "DI" => TrainType::FreightMechanicalElectricalEngineer,
            "DQ" => TrainType::FreightStores,
            "DT" => TrainType::FreightTest,
            "DY" => TrainType::FreightSignalTelecoms,
            "ZB" => TrainType::LocomotiveBrakeVan,
            "ZZ" => TrainType::Locomotive,
            "J2" => TrainType::FreightAutomotiveComponents,
            "H2" => TrainType::FreightAutomotiveVehicles,
            "J6" => TrainType::FreightWagonloadBuildingMaterials,
            "J5" => TrainType::FreightChemicals,
            "J3" => TrainType::FreightEdibleProducts,
            "J9" => TrainType::FreightIntermodalContracts,
            "H9" => TrainType::FreightIntermodalOther,
            "H8" => TrainType::FreightInternational,
            "J8" => TrainType::FreightMerchandise,
            "J4" => TrainType::FreightIndustrialMinerals,
            "A0" => TrainType::FreightCoalDistributive,
            "E0" => TrainType::FreightCoalElectricity,
            "B0" => TrainType::FreightNuclear,
            "B1" => TrainType::FreightMetals,
            "B4" => TrainType::FreightAggregates,
            "B5" => TrainType::FreightWaste,
            "B6" => TrainType::FreightTrainloadBuildingMaterials,
            "B7" => TrainType::FreightPetroleum,
            "H0" => TrainType::FreightInternationalMixed,
            "H1" => TrainType::FreightInternationalIntermodal,
            "H3" => TrainType::FreightInternationalAutomotive,
            "H4" => TrainType::FreightInternationalContract,
            "H5" => TrainType::FreightInternationalHaulmark,
            "H6" => TrainType::FreightInternationalJointVenture,
            "  " => train.variable_train.train_type, // should only really happen for ships
            x => return Err(CifError { error_type: CifErrorType::InvalidTrainCategory(x.to_string()), line: number, column: 10 } ),
        };

        let public_id = &line[12..16];
        let headcode = match &line[16..20] {
            "    " => None,
            x => Some(x.to_string()),
        };
        let service_group = &line[21..29];

        let power_type = match &line[30..33] {
            "D  " => Some(TrainPower::DieselLocomotive),
            "DEM" => Some(TrainPower::DieselElectricMultipleUnit),
            "DMU" => match &line[33..34] {
                "D" => Some(TrainPower::DieselMechanicalMultipleUnit),
                "V" => Some(TrainPower::DieselElectricMultipleUnit),
                _   => Some(TrainPower::DieselHydraulicMultipleUnit),
            },
            "E  " => Some(TrainPower::ElectricLocomotive),
            "ED " => Some(TrainPower::ElectricAndDieselLocomotive),
            "EML" => Some(TrainPower::ElectricMultipleUnitWithLocomotive),
            "EMU" => Some(TrainPower::ElectricMultipleUnit),
            "HST" => Some(TrainPower::DieselElectricMultipleUnit),
            "   " => None,
            x => return Err(CifError { error_type: CifErrorType::InvalidTrainPower(x.to_string()), line: number, column: 30 } ),
        };

        let speed_mph = match &line[37..40] {
            "   " => None,
            x => match x.parse::<u16>() {
                Ok(speed) => Some(speed),
                Err(_) => return Err(CifError { error_type: CifErrorType::InvalidSpeed(line[57..60].to_string()), line: number, column: 37 } ),
            },
        };

        let speed_m_per_s = match speed_mph {
            Some(x) => Some(f64::from(x) * (1609.344 / (60. * 60.))),
            None => None,
        };

        let mut operating_characteristics = OperatingCharacteristics { ..Default::default() };
        let mut _runs_as_required = false;

        for chr in line[40..46].chars() {
            match chr {
                'B' => operating_characteristics.vacuum_braked = true,
                'C' => operating_characteristics.one_hundred_mph = true,
                'D' => operating_characteristics.driver_only_passenger = true,
                'E' => operating_characteristics.br_mark_four_coaches = true,
                'G' => operating_characteristics.guard_required = true,
                'M' => operating_characteristics.one_hundred_and_ten_mph = true,
                'P' => operating_characteristics.push_pull = true,
                'Q' => _runs_as_required = true,
                'R' => operating_characteristics.air_conditioned_with_pa = true,
                'S' => operating_characteristics.steam_heat = true,
                'Y' => operating_characteristics.runs_to_locations_as_required = true,
                'Z' => operating_characteristics.sb1c_gauge = true,
                ' ' => (),
                x => return Err(CifError { error_type: CifErrorType::InvalidOperatingCharacteristic(x.to_string()), line: number, column: 40 } ),
            }
        }

        let timing_load_str = match &line[30..33] {
            "D  "       => match &line[33..37] {
                "    " => None,
                x      => if operating_characteristics.br_mark_four_coaches {
                    Some(format!("Diesel locomotive hauling {} tons of BR Mark 4 Coaches", x))
                }
                else {
                    Some(format!("Diesel locomotive hauling {} tons", x))
                },
            },
            "DEM"|"DMU" => match &line[33..37] {
                "69  " => Some("Class 172/0, 172/1, or 172/2 'Turbostar' DMU".to_string()),
                "A   " => Some("Class 14x 2-axle 'Pacer' DMU".to_string()),
                "E   " => Some("Class 158, 168, 170, 172, or 175 'Express' DMU".to_string()),
                "N   " => Some("Class 165/0 'Network Turbo' DMU".to_string()),
                "S   " => Some("Class 150, 153, 155, or 156 'Sprinter' DMU".to_string()),
                "T   " => Some("Class 165/1 or 166 'Network Turbo' DMU".to_string()),
                "V   " => Some("Class 220 or 221 'Voyager' DMU".to_string()),
                "X   " => Some("Class 159 'South Western Turbo' DMU".to_string()),
                "D1  " => Some("Vacuum-braked DMU with power car and trailer".to_string()),
                "D2  " => Some("Vacuum-braked DMU with two power cars and trailer".to_string()),
                "D3  " => Some("Vacuum-braked DMU with two power cars".to_string()),
                "195 " => Some("Class 195 'Civity' DMU".to_string()),
                "196 " => Some("Class 196 'Civity' DMU".to_string()),
                "197 " => Some("Class 197 'Civity' DMU".to_string()),
                "755 " => Some("Class 755 'FLIRT' bi-mode running on diesel".to_string()),
                "777 " => Some("Class 777/1 'METRO' bi-mode running on battery".to_string()),
                "800 " => Some("Class 800 'Azuma' bi-mode running on diesel".to_string()),
                "802 " => Some("Class 800/802 'IET/Nova 1/Paragon' bi-mode running on diesel".to_string()),
                "805 " => Some("Class 805 'Hitachi AT300' bi-mode running on diesel".to_string()),
                "1400" => Some("Diesel locomotive hauling 1400 tons".to_string()), // lol
                "    " => None,
                x => return Err(CifError { error_type: CifErrorType::InvalidTimingLoad(x.to_string()), line: number, column: 33 } ),
            },
            "E  "       => match &line[33..37] {
                "325 " => Some("Class 325 Parcels EMU".to_string()),
                "    " => None,
                x      => if operating_characteristics.br_mark_four_coaches {
                    Some(format!("Electric locomotive hauling {} tons of BR Mark 4 Coaches", x))
                }
                else {
                    Some(format!("Electric locomotive hauling {} tons", x))
                },
            },
            "ED "       => match &line[33..37] {
                "    " => None,
                x      => if operating_characteristics.br_mark_four_coaches {
                    Some(format!("Electric and diesel locomotive hauling {} tons of BR Mark 4 Coaches", x))
                }
                else {
                    Some(format!("Electric and diesel locomotive hauling {} tons", x))
                },
            },
            "EML"|"EMU" => match &line[33..36] {
                "AT " => Some("EMU with accelerated timings".to_string()),
                "E  " => Some("Class 458 EMU".to_string()),
                "0  " => Some("Class 380 EMU".to_string()),
                "506" => Some("Class 350/1 EMU".to_string()),
                "   " => None,
                x => Some(format!("Class {} EMU", x)),
            },
            "HST"       => Some("High Speed Train (IC125)".to_string()),
            "   "       => None,
            x => return Err(CifError { error_type: CifErrorType::InvalidTrainPower(x.to_string()), line: number, column: 30 } ),
        };

        let seating_class = match &line[46..47] {
            " " => match train_type {
                TrainType::Bus|TrainType::ServiceBus|TrainType::ReplacementBus|TrainType::OrdinaryPassenger|TrainType::ExpressPassenger|TrainType::InternationalPassenger|TrainType::SleeperPassenger|TrainType::InternationalSleeperPassenger|TrainType::CarCarryingPassenger|TrainType::UnadvertisedPassenger|TrainType::UnadvertisedExpressPassenger|TrainType::Staff|TrainType::EmptyPassengerAndStaff|TrainType::Mixed|TrainType::Metro|TrainType::PassengerParcels|TrainType::Ship => Class::Both,
                _ => Class::None,
            },
            "B" => Class::Both,
            "F" => Class::First,
            "S" => Class::Standard,
            x => return Err(CifError { error_type: CifErrorType::InvalidSeatingClass(x.to_string()), line: number, column: 46 } ),
        };

        let first_seating = match seating_class {
            Class::Both => true,
            Class::First => true,
            Class::Standard => false,
            Class::None => false,
        };
        let standard_seating = match seating_class {
            Class::Both => true,
            Class::First => false,
            Class::Standard => true,
            Class::None => false,
        };

        let sleeper_class = match &line[47..48] {
            " " => Class::None,
            "B" => Class::Both,
            "F" => Class::First,
            "S" => Class::Standard,
            x => return Err(CifError { error_type: CifErrorType::InvalidSeatingClass(x.to_string()), line: number, column: 47 } ),
        };

        let first_sleepers = match sleeper_class {
            Class::Both => true,
            Class::First => true,
            Class::Standard => false,
            Class::None => false,
        };
        let standard_sleepers = match sleeper_class {
            Class::Both => true,
            Class::First => false,
            Class::Standard => true,
            Class::None => false,
        };

        let mut catering = Catering { ..Default::default() };
        let mut wheelchair_reservations = false;
        for chr in line[50..54].chars() {
            match chr {
                'C' => catering.buffet = true,
                'F' => catering.first_class_restaurant = true,
                'H' => catering.hot_food = true,
                'M' => catering.first_class_meal = true,
                'P' => wheelchair_reservations = true,
                'R' => catering.restaurant = true,
                'T' => catering.trolley = true,
                ' ' => (),
                x => return Err(CifError { error_type: CifErrorType::InvalidCatering(x.to_string()), line: number, column: 50 } ),
            }
        }

        let reservations = match &line[48..49] {
            "A" => Reservations {
                seats: if first_seating || standard_seating { ReservationField::Mandatory } else { ReservationField::NotApplicable },
                bicycles: ReservationField::NotMandatory,
                sleepers: if first_sleepers || standard_sleepers { ReservationField::Mandatory } else { ReservationField::NotApplicable },
                vehicles: if train_type == TrainType::CarCarryingPassenger { ReservationField::Mandatory } else { ReservationField::NotApplicable },
                wheelchairs: ReservationField::Mandatory,
            },
            "E" => Reservations {
                seats: if first_seating || standard_seating { ReservationField::NotMandatory } else { ReservationField::NotApplicable },
                bicycles: ReservationField::Mandatory,
                sleepers: if first_sleepers || standard_sleepers { ReservationField::NotMandatory } else { ReservationField::NotApplicable },
                vehicles: if train_type == TrainType::CarCarryingPassenger { ReservationField::Mandatory } else { ReservationField::NotApplicable },
                wheelchairs: if wheelchair_reservations { ReservationField::Possible } else { ReservationField::NotMandatory },
            },
            "R" => Reservations {
                seats: if first_seating || standard_seating { ReservationField::Recommended } else { ReservationField::NotApplicable },
                bicycles: ReservationField::NotMandatory,
                sleepers: if first_sleepers || standard_sleepers { ReservationField::Recommended } else { ReservationField::NotApplicable },
                vehicles: if train_type == TrainType::CarCarryingPassenger { ReservationField::Mandatory } else { ReservationField::NotApplicable },
                wheelchairs: ReservationField::Recommended,
            },
            "S" => Reservations {
                seats: if first_seating || standard_seating { ReservationField::Possible } else { ReservationField::NotApplicable },
                bicycles: ReservationField::NotMandatory,
                sleepers: if first_sleepers || standard_sleepers { ReservationField::Possible } else { ReservationField::NotApplicable },
                vehicles: if train_type == TrainType::CarCarryingPassenger { ReservationField::Mandatory } else { ReservationField::NotApplicable },
                wheelchairs: ReservationField::Possible,
            },
            " " => Reservations {
                seats: if first_seating || standard_seating { ReservationField::Impossible } else { ReservationField::NotApplicable },
                bicycles: ReservationField::NotMandatory,
                sleepers: if first_sleepers || standard_sleepers { ReservationField::Impossible } else { ReservationField::NotApplicable },
                vehicles: if train_type == TrainType::CarCarryingPassenger { ReservationField::Mandatory } else { ReservationField::NotApplicable },
                wheelchairs: if wheelchair_reservations { ReservationField::Possible } else { if first_seating || standard_seating || first_sleepers || standard_sleepers { ReservationField::Impossible } else { ReservationField::NotApplicable } },
            },
            x => return Err(CifError { error_type: CifErrorType::InvalidReservationType(x.to_string()), line: number, column: 48 } ),
        };

        let mut brand = None;
        for chr in line[54..58].chars() {
            match chr {
                'E' => brand = Some("Eurostar".to_string()),
                'U' => brand = Some("Alphaline".to_string()),
                ' ' => (),
                x => return Err(CifError { error_type: CifErrorType::InvalidBrand(x.to_string()), line: number, column: 54 } ),
            }
        }

        let uic_code = match &line[52..57] {
            "     " => None,
            x => Some(x.to_string()),
        };

        self.change_en_route = Some(VariableTrain {
            train_type,
            public_id: Some(public_id.to_string()),
            headcode,
            service_group: Some(service_group.to_string()),
            power_type: power_type,
            timing_allocation: match timing_load_str {
                None => None,
                Some(x) => Some(TrainAllocation {
                    id: line[30..37].to_string(),
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
            operator: train.variable_train.operator.clone(),
        });

        Ok(schedule)
    }

    fn read_tiploc(&self, line: &str, mut schedule: Schedule, number: u64, modification_type: ModificationType) -> Result<Schedule, CifError> {
        let tiploc = &line[2..9];
        let name = &line[18..44];
        let crs = &line[53..56];
        let opt_crs = match crs {
            "   " => None,
            x => Some(x.to_string()),
        };

        let location = match modification_type {
            ModificationType::Insert => Location {
                id: tiploc.to_string(),
                name: name.to_string(),
                public_id: opt_crs,
            },
            ModificationType::Amend => {
                let location = schedule.locations.remove(tiploc);
                let mut location = match location {
                    None => return Err(CifError { error_type: CifErrorType::LocationNotFound(tiploc.to_string()), line: number, column: 2 }),
                    Some(x) => x,
                };
                location.id = tiploc.to_string();
                location.name = name.to_string();
                location.public_id = opt_crs;
                location
            },
            ModificationType::Delete => {
                schedule.locations.remove(tiploc); // it's OK if the TIPLOC isn't found
                return Ok(schedule)
            },
        };
        schedule.locations.insert(tiploc.to_string(), location);
        Ok(schedule)
    }

    fn read_header(&self, line: &str, mut schedule: Schedule, number: u64) -> Result<Schedule, CifError> {
        schedule.their_id = Some(line[2..22].to_string());
        let parsed_datetime = NaiveDateTime::parse_from_str(&line[22..32], "%y%m%d%H%M");
        let parsed_datetime = match parsed_datetime {
            Ok(x) => x,
            Err(x) => return Err(CifError { error_type: CifErrorType::ChronoParseError(x), line: number, column: 22 }),
        };
        schedule.last_updated = Some(London.from_local_datetime(&parsed_datetime).unwrap());
        if &line[46..47] == "F" {
            let parsed_begin_date = NaiveDate::parse_from_str(&line[48..54], "%y%m%d");
            let parsed_begin_date = match parsed_begin_date {
                Ok(x) => x,
                Err(x) => return Err(CifError { error_type: CifErrorType::ChronoParseError(x), line: number, column: 48 }),
            };
            schedule.valid_begin = Some(London.from_local_datetime(&parsed_begin_date.and_hms_opt(0, 0, 0).unwrap()).unwrap());
            let parsed_end_date = NaiveDate::parse_from_str(&line[54..60], "%y%m%d");
            let parsed_end_date = match parsed_end_date {
                Ok(x) => x,
                Err(x) => return Err(CifError { error_type: CifErrorType::ChronoParseError(x), line: number, column: 54 }),
            };
            schedule.valid_end = Some(London.from_local_datetime(&parsed_end_date.and_hms_opt(0, 0, 0).unwrap()).unwrap());
        }
        Ok(schedule)
    }

    fn finalise(&mut self, _line: &str, mut schedule: Schedule, number: u64) -> Result<Schedule, CifError> {
        for ((train_id, location, location_suffix), assocs) in &self.unwritten_assocs {
            let mut trains = match schedule.trains.get_mut(train_id) {
                Some(x) => x,
                None => return Err(CifError { error_type: CifErrorType::TrainNotFound(train_id.clone()), line: number, column: 0 }),
            };

            write_assocs_to_trains(&mut trains, &train_id, &location, &location_suffix, &assocs);
        }
        self.unwritten_assocs.clear();
        Ok(schedule)
    }

    fn read_record(&mut self, line: String, schedule: Schedule, number: u64) -> Result<Schedule, CifError> {
        if line.is_empty() {
            return Ok(schedule)
        }
        if line.len() != 80 {
            return Err(CifError { error_type: CifErrorType::InvalidRecordLength(line.len()), line: number, column: 0});
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
            x => Err(CifError { error_type: CifErrorType::InvalidRecordType(x.to_string()), line: number, column: 0}),
        }
    }
}

#[async_trait]
impl Importer for CifImporter {
    async fn overlay(&mut self, reader: impl AsyncBufReadExt + Unpin + Send, mut schedule: Schedule) -> Result<Schedule, Error> {
        let mut lines = reader.lines();

        let mut i: u64 = 0;

        while let Some(line) = lines.next_line().await? {
            i += 1;
            schedule = self.read_record(line, schedule, i)?;
        }
        Ok(schedule)
    }
}
