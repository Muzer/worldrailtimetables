use crate::schedule::{AssociationNode, Catering, DaysOfWeek, Location, OperatingCharacteristics, ReservationField, Reservations, Schedule, Train, TrainAllocation, TrainSource, TrainType, TrainPower, TrainValidityPeriod, VariableTrain};
use crate::importer::Importer;
use crate::error::Error;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone};
use chrono::format::ParseError;
use chrono::naive::Days;
use chrono_tz::Tz;
use chrono_tz::Europe::London;

use std::collections::HashMap;
use std::fmt;
use std::ops::{Add, Sub};
use tokio::io::AsyncBufReadExt;

#[derive(Default)]
pub struct CifImporter {
    last_train: Option<String>,
    unwritten_assocs: HashMap<(String, String, Option<String>), Vec<(AssociationNode, AssociationCategory)>>,
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

fn delete_single_vec_assocs(assocs: &mut Vec<AssociationNode>, other_train_id: &str, begin: &DateTime::<Tz>, other_train_location_suffix: &Option<String>, stp_modification_type: &ModificationType, is_stp: bool, use_rev: bool) {
    assocs.retain(|assoc| match (&stp_modification_type, &is_stp) {
        (ModificationType::Insert, false) => assoc.source.unwrap() != TrainSource::LongTerm || assoc.validity[0].valid_begin != if use_rev { rev_date(begin, assoc.day_diff) } else { *begin }, // delete the entire association for deleted inserts
        (ModificationType::Insert, true) => assoc.source.unwrap() != TrainSource::ShortTerm || assoc.validity[0].valid_begin != if use_rev { rev_date(begin, assoc.day_diff) } else { *begin },
        (ModificationType::Amend, _) => true, // for deleted amendments we never delete an item here
        (ModificationType::Delete, _) => true, // for deleted cancellations we never delete an item here
    } || other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix);
    for ref mut assoc in assocs.iter_mut() {
        let rev_begin = if use_rev {
            rev_date(&begin, assoc.day_diff)
        }
        else {
            *begin
        };
        assoc.replacements.retain(|assoc| match &stp_modification_type {
            ModificationType::Insert => true, // never delete from here for insertions
            ModificationType::Amend => assoc.validity[0].valid_begin != rev_begin, // for deleted amendments we delete the actual replacement along with cleaning up the replacement list in the original
            ModificationType::Delete => true, // for deleted cancellations we never delete an item here
        } || other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix);
        assoc.cancellations.retain(|(validity, _days_of_week)| match &stp_modification_type {
            ModificationType::Insert => true, // never delete from here for insertions
            ModificationType::Amend => true,
            ModificationType::Delete => validity.valid_begin != rev_begin,
        } || other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix)
    }
}

fn amend_single_vec_assocs(assocs: &mut Vec<AssociationNode>, other_train_id: &str, begin: &DateTime::<Tz>, end: &DateTime::<Tz>, days_of_week: &DaysOfWeek, other_train_location_suffix: &Option<String>, stp_modification_type: &ModificationType, is_stp: bool, day_diff: i8, for_passengers: bool) {
    for ref mut assoc in assocs.iter_mut() {
        if !(match (&stp_modification_type, &is_stp) {
                (ModificationType::Insert, false) => assoc.source.unwrap() != TrainSource::LongTerm || assoc.validity[0].valid_begin != *begin, // delete the entire association for deleted inserts
                (ModificationType::Insert, true) => assoc.source.unwrap() != TrainSource::ShortTerm || assoc.validity[0].valid_begin != *begin,
                (ModificationType::Amend, _) => true, // for deleted amendments we never delete an item here
                (ModificationType::Delete, _) => true, // for deleted cancellations we never delete an item here
            } || other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix) {
            assoc.validity = vec![TrainValidityPeriod {
                valid_begin: begin.clone(),
                valid_end: end.clone(),
            }];
            assoc.days = days_of_week.clone();
            assoc.day_diff = day_diff;
            assoc.for_passengers = for_passengers;
        }
        if matches!(stp_modification_type, ModificationType::Amend) || matches!(stp_modification_type, ModificationType::Delete) {
            if assoc.other_train_id == other_train_id && assoc.other_train_location_id_suffix == *other_train_location_suffix {
                if matches!(stp_modification_type, ModificationType::Amend) {
                    for replacement in assoc.replacements.iter_mut() {
                        if replacement.validity[0].valid_begin == *begin {
                            replacement.validity = vec![TrainValidityPeriod {
                                valid_begin: begin.clone(),
                                valid_end: end.clone(),
                            }];
                            replacement.days = days_of_week.clone();
                            replacement.day_diff = day_diff;
                            replacement.for_passengers = for_passengers;
                        }
                    }
                }
                else if matches!(stp_modification_type, ModificationType::Delete) {
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
                else {
                    panic!("Insert found where amend or cancel expected");
                }
            }
        }
    }
}

fn cancel_single_vec_assocs(assocs: &mut Vec<AssociationNode>, other_train_id: &str, begin: &DateTime::<Tz>, end: &DateTime::<Tz>, days_of_week: &DaysOfWeek, other_train_location_suffix: &Option<String>, use_rev: bool) {
    for ref mut assoc in assocs.iter_mut() {
        if !(other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix) {
            let rev_begin = if use_rev {
                rev_date(&begin, assoc.day_diff)
            }
            else {
                *begin
            };
            let rev_end = if use_rev {
                rev_date(&end, assoc.day_diff)
            }
            else {
                *end
            };
            let rev_days_of_week = if use_rev {
                rev_days(&days_of_week, assoc.day_diff)
            }
            else {
                *days_of_week
            };
            // check for no overlapping days at all
            if rev_days_of_week.into_iter().zip(assoc.days.into_iter()).find(|(new_day, assoc_day)| *new_day && *assoc_day).is_none() {
                continue;
            }
            let new_begin = if rev_begin > assoc.validity[0].valid_begin {
                rev_begin.clone()
            }
            else {
                assoc.validity[0].valid_begin.clone()
            };
            let new_end = if rev_end < assoc.validity[0].valid_end {
                rev_end.clone()
            }
            else {
                assoc.validity[0].valid_end.clone()
            };
            if new_end < new_begin {
                continue;
            }
            let new_cancel = TrainValidityPeriod {
                valid_begin: new_begin,
                valid_end: new_end,
            };
            assoc.cancellations.push((new_cancel, rev_days_of_week.clone()))
        }
    }
}

fn replace_single_vec_assocs(assocs: &mut Vec<AssociationNode>, other_train_id: &str, other_train_location_suffix: &Option<String>, new_assoc: &AssociationNode) {
    for ref mut assoc in assocs.iter_mut() {
        if !(other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix) {
            // check for no overlapping days at all
            if new_assoc.days.into_iter().zip(assoc.days.into_iter()).find(|(new_day, assoc_day)| *new_day && *assoc_day).is_none() {
                continue;
            }
            let new_begin = if new_assoc.validity[0].valid_begin > assoc.validity[0].valid_begin {
                new_assoc.validity[0].valid_begin.clone()
            }
            else {
                assoc.validity[0].valid_begin.clone()
            };
            let new_end = if new_assoc.validity[0].valid_end < assoc.validity[0].valid_end {
                new_assoc.validity[0].valid_end.clone()
            }
            else {
                assoc.validity[0].valid_end.clone()
            };
            if new_end < new_begin {
                continue;
            }
            let new_assoc_fixed_date = AssociationNode {
                validity: vec![TrainValidityPeriod {
                    valid_begin: new_begin,
                    valid_end: new_end,
                }],
                ..new_assoc.clone()
            };
            assoc.replacements.push(new_assoc_fixed_date);
        }
    }
}

impl CifImporter {
    pub fn new() -> CifImporter {
        CifImporter { ..Default::default() }
    }

    fn trains_delete_assoc(&self, trains: &mut Vec<Train>, other_train_id: &str, begin: &DateTime::<Tz>, location: &str, location_suffix: &Option<String>, other_train_location_suffix: &Option<String>, stp_modification_type: &ModificationType, is_stp: bool) {
        for ref mut train in trains.iter_mut() {
            // recurse on replacements
            self.trains_delete_assoc(&mut train.replacements, &other_train_id, &begin, &location, &location_suffix, &other_train_location_suffix, &stp_modification_type, is_stp);

            for ref mut train_location in train.route.iter_mut() {
                if train_location.id == location && train_location.id_suffix == *location_suffix {
                    delete_single_vec_assocs(&mut train_location.divides_to_form, other_train_id, begin, other_train_location_suffix, stp_modification_type, is_stp, false);
                    delete_single_vec_assocs(&mut train_location.joins_to, other_train_id, begin, other_train_location_suffix, stp_modification_type, is_stp, false);
                    if let Some(ref mut assoc) = &mut train_location.becomes {
                        assoc.replacements.retain(|assoc| match &stp_modification_type {
                            ModificationType::Insert => true, // never delete from here for insertions
                            ModificationType::Amend => assoc.validity[0].valid_begin != *begin, // for deleted amendments we delete the actual replacement along with cleaning up the replacement list in the original
                            ModificationType::Delete => true, // for deleted cancellations we never delete an item here
                        } || other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix);
                        assoc.cancellations.retain(|(validity, _days_of_week)| match &stp_modification_type {
                            ModificationType::Insert => true, // never delete from here for insertions
                            ModificationType::Amend => true,
                            ModificationType::Delete => validity.valid_begin != *begin,
                        } || other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix);
                        if !(match (&stp_modification_type, &is_stp) {
                                (ModificationType::Insert, false) => assoc.source.unwrap() != TrainSource::LongTerm || assoc.validity[0].valid_begin != *begin, // delete the entire association for deleted inserts
                                (ModificationType::Insert, true) => assoc.source.unwrap() != TrainSource::ShortTerm || assoc.validity[0].valid_begin != *begin,
                                (ModificationType::Amend, _) => true, // for deleted amendments we never delete an item here
                                (ModificationType::Delete, _) => true, // for deleted cancellations we never delete an item here
                            } || other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix) {
                            train_location.becomes = None;
                        }
                    }
                }
            }
        }
    }

    fn trains_delete_rev_assoc(&self, trains: &mut Vec<Train>, other_train_id: &str, begin: &DateTime::<Tz>, location: &str, location_suffix: &Option<String>, other_train_location_suffix: &Option<String>, stp_modification_type: &ModificationType, is_stp: bool) {
        for ref mut train in trains.iter_mut() {
            // recurse on replacements
            self.trains_delete_rev_assoc(&mut train.replacements, &other_train_id, &begin, &location, &location_suffix, &other_train_location_suffix, &stp_modification_type, is_stp);

            for ref mut train_location in train.route.iter_mut() {
                if train_location.id == location && train_location.id_suffix == *location_suffix {
                    delete_single_vec_assocs(&mut train_location.divides_from, other_train_id, begin, other_train_location_suffix, stp_modification_type, is_stp, true);
                    delete_single_vec_assocs(&mut train_location.is_joined_to_by, other_train_id, begin, other_train_location_suffix, stp_modification_type, is_stp, true);
                    if let Some(ref mut assoc) = &mut train_location.forms_from {
                        let rev_begin = rev_date(&begin, assoc.day_diff);
                        assoc.replacements.retain(|assoc| match &stp_modification_type {
                            ModificationType::Insert => true, // never delete from here for insertions
                            ModificationType::Amend => assoc.validity[0].valid_begin != rev_begin, // for deleted amendments we delete the actual replacement along with cleaning up the replacement list in the original
                            ModificationType::Delete => true, // for deleted cancellations we never delete an item here
                        } || other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix);
                        assoc.cancellations.retain(|(validity, _days_of_week)| match &stp_modification_type {
                            ModificationType::Insert => true, // never delete from here for insertions
                            ModificationType::Amend => true,
                            ModificationType::Delete => validity.valid_begin != rev_begin,
                        } || other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix);
                        if !(match (&stp_modification_type, &is_stp) {
                                (ModificationType::Insert, false) => assoc.source.unwrap() != TrainSource::LongTerm || assoc.validity[0].valid_begin != rev_begin, // delete the entire association for deleted inserts
                                (ModificationType::Insert, true) => assoc.source.unwrap() != TrainSource::ShortTerm || assoc.validity[0].valid_begin != rev_begin,
                                (ModificationType::Amend, _) => true, // for deleted amendments we never delete an item here
                                (ModificationType::Delete, _) => true, // for deleted cancellations we never delete an item here
                            } || other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix) {
                            train_location.forms_from = None;
                        }
                    }
                }
            }
        }
    }

    fn trains_amend_assoc(&self, trains: &mut Vec<Train>, other_train_id: &str, begin: &DateTime::<Tz>, end: &DateTime::<Tz>, days_of_week: &DaysOfWeek, location: &str, location_suffix: &Option<String>, other_train_location_suffix: &Option<String>, stp_modification_type: &ModificationType, is_stp: bool, day_diff: i8, for_passengers: bool) {
        for ref mut train in trains.iter_mut() {
            // recurse on replacements
            self.trains_amend_assoc(&mut train.replacements, &other_train_id, &begin, &end, &days_of_week, &location, &location_suffix, &other_train_location_suffix, &stp_modification_type, is_stp, day_diff, for_passengers);

            for ref mut train_location in train.route.iter_mut() {
                if train_location.id == location && train_location.id_suffix == *location_suffix {
                    amend_single_vec_assocs(&mut train_location.divides_to_form, other_train_id, begin, end, days_of_week, other_train_location_suffix, stp_modification_type, is_stp, day_diff, for_passengers);
                    amend_single_vec_assocs(&mut train_location.joins_to, other_train_id, begin, end, days_of_week, other_train_location_suffix, stp_modification_type, is_stp, day_diff, for_passengers);
                    if let Some(ref mut assoc) = &mut train_location.becomes {
                        if !(match (&stp_modification_type, &is_stp) {
                                (ModificationType::Insert, false) => assoc.source.unwrap() != TrainSource::LongTerm || assoc.validity[0].valid_begin != *begin, // delete the entire association for deleted inserts
                                (ModificationType::Insert, true) => assoc.source.unwrap() != TrainSource::ShortTerm || assoc.validity[0].valid_begin != *begin,
                                (ModificationType::Amend, _) => true, // for deleted amendments we never delete an item here
                                (ModificationType::Delete, _) => true, // for deleted cancellations we never delete an item here
                            } || other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix) {
                            assoc.validity = vec![TrainValidityPeriod {
                                valid_begin: begin.clone(),
                                valid_end: end.clone(),
                            }];
                            assoc.days = days_of_week.clone();
                            assoc.day_diff = day_diff;
                            assoc.for_passengers = for_passengers;
                        }
                    }
                    if let Some(ref mut assoc) = &mut train_location.becomes {
                        if matches!(stp_modification_type, ModificationType::Amend) || matches!(stp_modification_type, ModificationType::Delete) {
                            if assoc.other_train_id == other_train_id && assoc.other_train_location_id_suffix == *other_train_location_suffix {
                                if matches!(stp_modification_type, ModificationType::Amend) {
                                    for replacement in assoc.replacements.iter_mut() {
                                        if replacement.validity[0].valid_begin == *begin {
                                            replacement.validity = vec![TrainValidityPeriod {
                                                valid_begin: begin.clone(),
                                                valid_end: end.clone(),
                                            }];
                                            replacement.days = days_of_week.clone();
                                            replacement.day_diff = day_diff;
                                            replacement.for_passengers = for_passengers;
                                        }
                                    }
                                }
                                else if matches!(stp_modification_type, ModificationType::Delete) {
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
                                else {
                                    panic!("Insert found where amend or cancel expected");
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn trains_amend_rev_assoc(&self, trains: &mut Vec<Train>, other_train_id: &str, begin: &DateTime::<Tz>, end: &DateTime::<Tz>, days_of_week: &DaysOfWeek, location: &str, location_suffix: &Option<String>, other_train_location_suffix: &Option<String>, stp_modification_type: &ModificationType, is_stp: bool, day_diff: i8, for_passengers: bool) {
        for ref mut train in trains.iter_mut() {
            // recurse on replacements
            self.trains_amend_assoc(&mut train.replacements, &other_train_id, &begin, &end, &days_of_week, &location, &location_suffix, &other_train_location_suffix, &stp_modification_type, is_stp, day_diff, for_passengers);

            for ref mut train_location in train.route.iter_mut() {
                if train_location.id == location && train_location.id_suffix == *location_suffix {
                    amend_single_vec_assocs(&mut train_location.divides_from, other_train_id, begin, end, days_of_week, other_train_location_suffix, stp_modification_type, is_stp, day_diff, for_passengers);
                    amend_single_vec_assocs(&mut train_location.is_joined_to_by, other_train_id, begin, end, days_of_week, other_train_location_suffix, stp_modification_type, is_stp, day_diff, for_passengers);
                    if let Some(ref mut assoc) = &mut train_location.forms_from {
                        if !(match (&stp_modification_type, &is_stp) {
                                (ModificationType::Insert, false) => assoc.source.unwrap() != TrainSource::LongTerm || assoc.validity[0].valid_begin != *begin, // delete the entire association for deleted inserts
                                (ModificationType::Insert, true) => assoc.source.unwrap() != TrainSource::ShortTerm || assoc.validity[0].valid_begin != *begin,
                                (ModificationType::Amend, _) => true, // for deleted amendments we never delete an item here
                                (ModificationType::Delete, _) => true, // for deleted cancellations we never delete an item here
                            } || other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix) {
                            assoc.validity = vec![TrainValidityPeriod {
                                valid_begin: begin.clone(),
                                valid_end: end.clone(),
                            }];
                            assoc.days = days_of_week.clone();
                            assoc.day_diff = day_diff;
                            assoc.for_passengers = for_passengers;
                        }
                    }
                    if let Some(ref mut assoc) = &mut train_location.forms_from {
                        if matches!(stp_modification_type, ModificationType::Amend) || matches!(stp_modification_type, ModificationType::Delete) {
                            if assoc.other_train_id == other_train_id && assoc.other_train_location_id_suffix == *other_train_location_suffix {
                                if matches!(stp_modification_type, ModificationType::Amend) {
                                    for replacement in assoc.replacements.iter_mut() {
                                        if replacement.validity[0].valid_begin == *begin {
                                            replacement.validity = vec![TrainValidityPeriod {
                                                valid_begin: begin.clone(),
                                                valid_end: end.clone(),
                                            }];
                                            replacement.days = days_of_week.clone();
                                            replacement.day_diff = day_diff;
                                            replacement.for_passengers = for_passengers;
                                        }
                                    }
                                }
                                else if matches!(stp_modification_type, ModificationType::Delete) {
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
                                else {
                                    panic!("Insert found where amend or cancel expected");
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn trains_cancel_assoc(&self, trains: &mut Vec<Train>, other_train_id: &str, begin: &DateTime::<Tz>, end: &DateTime::<Tz>, days_of_week: &DaysOfWeek, location: &str, location_suffix: &Option<String>, other_train_location_suffix: &Option<String>) {
        for ref mut train in trains.iter_mut() {
            // recurse on replacements
            self.trains_cancel_assoc(&mut train.replacements, &other_train_id, &begin, &end, &days_of_week, &location, &location_suffix, &other_train_location_suffix);

            for ref mut train_location in train.route.iter_mut() {
                if train_location.id == location && train_location.id_suffix == *location_suffix {
                    cancel_single_vec_assocs(&mut train_location.divides_to_form, other_train_id, begin, end, days_of_week, other_train_location_suffix, false);
                    cancel_single_vec_assocs(&mut train_location.joins_to, other_train_id, begin, end, days_of_week, other_train_location_suffix, false);
                    if let Some(assoc) = &mut train_location.becomes {
                        if !(other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix) {
                            // check for no overlapping days at all
                            if days_of_week.into_iter().zip(assoc.days.into_iter()).find(|(new_day, assoc_day)| *new_day && *assoc_day).is_none() {
                                continue;
                            }
                            let new_begin = if begin > &assoc.validity[0].valid_begin {
                                begin.clone()
                            }
                            else {
                                assoc.validity[0].valid_begin.clone()
                            };
                            let new_end = if end < &assoc.validity[0].valid_end {
                                end.clone()
                            }
                            else {
                                assoc.validity[0].valid_end.clone()
                            };
                            if new_end < new_begin {
                                continue;
                            }
                            let new_cancel = TrainValidityPeriod {
                                valid_begin: new_begin,
                                valid_end: new_end,
                            };
                            assoc.cancellations.push((new_cancel, days_of_week.clone()))
                        }
                    }
                }
            }
        }
    }

    fn trains_cancel_rev_assoc(&self, trains: &mut Vec<Train>, other_train_id: &str, begin: &DateTime::<Tz>, end: &DateTime::<Tz>, days_of_week: &DaysOfWeek, location: &str, location_suffix: &Option<String>, other_train_location_suffix: &Option<String>) {
        for ref mut train in trains.iter_mut() {
            // recurse on replacements
            self.trains_cancel_rev_assoc(&mut train.replacements, &other_train_id, &begin, &end, &days_of_week, &location, &location_suffix, &other_train_location_suffix);

            for ref mut train_location in train.route.iter_mut() {
                if train_location.id == location && train_location.id_suffix == *location_suffix {
                    cancel_single_vec_assocs(&mut train_location.divides_from, other_train_id, begin, end, days_of_week, other_train_location_suffix, true);
                    cancel_single_vec_assocs(&mut train_location.is_joined_to_by, other_train_id, begin, end, days_of_week, other_train_location_suffix, true);
                    if let Some(assoc) = &mut train_location.forms_from {
                        if !(other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix) {
                            let rev_begin = rev_date(&begin, assoc.day_diff);
                            let rev_end = rev_date(&end, assoc.day_diff);
                            let rev_days_of_week = rev_days(&days_of_week, assoc.day_diff);
                            // check for no overlapping days at all
                            if rev_days_of_week.into_iter().zip(assoc.days.into_iter()).find(|(new_day, assoc_day)| *new_day && *assoc_day).is_none() {
                                continue;
                            }
                            let new_begin = if rev_begin > assoc.validity[0].valid_begin {
                                rev_begin.clone()
                            }
                            else {
                                assoc.validity[0].valid_begin.clone()
                            };
                            let new_end = if rev_end < assoc.validity[0].valid_end {
                                rev_end.clone()
                            }
                            else {
                                assoc.validity[0].valid_end.clone()
                            };
                            if new_end < new_begin {
                                continue;
                            }
                            let new_cancel = TrainValidityPeriod {
                                valid_begin: new_begin,
                                valid_end: new_end,
                            };
                            assoc.cancellations.push((new_cancel, rev_days_of_week.clone()))
                        }
                    }
                }
            }
        }
    }

    fn trains_replace_assoc(&self, trains: &mut Vec<Train>, other_train_id: &str, location: &str, location_suffix: &Option<String>, other_train_location_suffix: &Option<String>, new_assoc: &AssociationNode) {
        for ref mut train in trains.iter_mut() {
            // recurse on replacements
            self.trains_replace_assoc(&mut train.replacements, &other_train_id, &location, &location_suffix, &other_train_location_suffix, &new_assoc);

            for ref mut train_location in train.route.iter_mut() {
                if train_location.id == location && train_location.id_suffix == *location_suffix {
                    replace_single_vec_assocs(&mut train_location.divides_to_form, other_train_id, other_train_location_suffix, new_assoc);
                    replace_single_vec_assocs(&mut train_location.joins_to, other_train_id, other_train_location_suffix, new_assoc);
                    if let Some(assoc) = &mut train_location.becomes {
                        if !(other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix) {
                            // check for no overlapping days at all
                            if new_assoc.days.into_iter().zip(assoc.days.into_iter()).find(|(new_day, assoc_day)| *new_day && *assoc_day).is_none() {
                                continue;
                            }
                            let new_begin = if new_assoc.validity[0].valid_begin > assoc.validity[0].valid_begin {
                                new_assoc.validity[0].valid_begin.clone()
                            }
                            else {
                                assoc.validity[0].valid_begin.clone()
                            };
                            let new_end = if new_assoc.validity[0].valid_end < assoc.validity[0].valid_end {
                                new_assoc.validity[0].valid_end.clone()
                            }
                            else {
                                assoc.validity[0].valid_end.clone()
                            };
                            if new_end < new_begin {
                                continue;
                            }
                            let new_assoc_fixed_date = AssociationNode {
                                validity: vec![TrainValidityPeriod {
                                    valid_begin: new_begin,
                                    valid_end: new_end,
                                }],
                                ..new_assoc.clone()
                            };
                            assoc.replacements.push(new_assoc_fixed_date);
                        }
                    }
                }
            }
        }
    }

    fn trains_replace_rev_assoc(&self, trains: &mut Vec<Train>, other_train_id: &str, location: &str, location_suffix: &Option<String>, other_train_location_suffix: &Option<String>, new_assoc: &AssociationNode) {
        for ref mut train in trains.iter_mut() {
            // recurse on replacements
            self.trains_replace_assoc(&mut train.replacements, &other_train_id, &location, &location_suffix, &other_train_location_suffix, &new_assoc);

            for ref mut train_location in train.route.iter_mut() {
                if train_location.id == location && train_location.id_suffix == *location_suffix {
                    replace_single_vec_assocs(&mut train_location.divides_from, other_train_id, other_train_location_suffix, new_assoc);
                    replace_single_vec_assocs(&mut train_location.is_joined_to_by, other_train_id, other_train_location_suffix, new_assoc);
                    if let Some(assoc) = &mut train_location.forms_from {
                        if !(other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix) {
                            // check for no overlapping days at all
                            if new_assoc.days.into_iter().zip(assoc.days.into_iter()).find(|(new_day, assoc_day)| *new_day && *assoc_day).is_none() {
                                continue;
                            }
                            let new_begin = if new_assoc.validity[0].valid_begin > assoc.validity[0].valid_begin {
                                new_assoc.validity[0].valid_begin.clone()
                            }
                            else {
                                assoc.validity[0].valid_begin.clone()
                            };
                            let new_end = if new_assoc.validity[0].valid_end < assoc.validity[0].valid_end {
                                new_assoc.validity[0].valid_end.clone()
                            }
                            else {
                                assoc.validity[0].valid_end.clone()
                            };
                            if new_end < new_begin {
                                continue;
                            }
                            let new_assoc_fixed_date = AssociationNode {
                                validity: vec![TrainValidityPeriod {
                                    valid_begin: new_begin,
                                    valid_end: new_end,
                                }],
                                ..new_assoc.clone()
                            };
                            assoc.replacements.push(new_assoc_fixed_date);
                        }
                    }
                }
            }
        }
    }

    fn read_association(&mut self, line: &str, mut schedule: Schedule, number: u64) -> Result<Schedule, CifError> {
        let modification_type = match &line[2..3] {
            "N" => ModificationType::Insert,
            "D" => ModificationType::Delete,
            "R" => ModificationType::Amend,
            x => return Err(CifError { error_type: CifErrorType::InvalidTransactionType(x.to_string()), line: number, column: 2 } ),
        };
        let stp_modification_type = match &line[79..80] {
            " " => ModificationType::Insert,
            "P" => ModificationType::Insert,
            "N" => ModificationType::Insert,
            "O" => ModificationType::Amend,
            "C" => ModificationType::Delete,
            x => return Err(CifError { error_type: CifErrorType::InvalidStpIndicator(x.to_string()), line: number, column: 79 } ),
        };
        let is_stp = match &line[79..80] {
            " " => false,
            "P" => false,
            "N" => true,
            "O" => true,
            "C" => true,
            x => return Err(CifError { error_type: CifErrorType::InvalidStpIndicator(x.to_string()), line: number, column: 79 } ),
        };

        let main_train_id = &line[3..9];
        let other_train_id = &line[9..15];
        let parsed_begin_date = NaiveDate::parse_from_str(&line[15..21], "%y%m%d");
        let parsed_begin_date = match parsed_begin_date {
            Ok(x) => x,
            Err(x) => return Err(CifError { error_type: CifErrorType::ChronoParseError(x), line: number, column: 15 }),
        };
        let begin = London.from_local_datetime(&parsed_begin_date.and_hms_opt(0, 0, 0).unwrap()).unwrap();
        let location = &line[37..44];
        let location_suffix = &line[44..45];
        let location_suffix = match location_suffix {
            " " => None,
            x => Some(x.to_string()),
        };
        let other_train_location_suffix = &line[45..46];
        let other_train_location_suffix = match other_train_location_suffix {
            " " => None,
            x => Some(x.to_string()),
        };

        // At this stage we have all the data we need for a simple delete, so handle this here
        //
        // Note these are NOT the same as STP cancels and indeed handled completely differently
        if matches!(modification_type, ModificationType::Delete) {
            // first find any committed associations and delete
            self.trains_delete_assoc(schedule.trains.get_mut(main_train_id).as_mut().unwrap_or(&mut &mut vec![]), &other_train_id, &begin, &location, &location_suffix, &other_train_location_suffix, &stp_modification_type, is_stp);
            self.trains_delete_rev_assoc(schedule.trains.get_mut(other_train_id).as_mut().unwrap_or(&mut &mut vec![]), &main_train_id, &begin, &location, &other_train_location_suffix, &location_suffix, &stp_modification_type, is_stp);

            // now delete from unwritten associations
            let old_assoc = self.unwritten_assocs.remove(&(main_train_id.to_string(), location.to_string(), location_suffix.clone()));
            let mut old_assoc = match old_assoc {
                None => return Ok(schedule),
                Some(x) => x,
            };

            // first we delete from the pending list
            old_assoc.retain(|(assoc, _category)| {
                if assoc.other_train_id == other_train_id && assoc.other_train_location_id_suffix == other_train_location_suffix {
                    match (&stp_modification_type, &is_stp) {
                        (ModificationType::Insert, false) => assoc.source.unwrap() != TrainSource::LongTerm || assoc.validity[0].valid_begin != begin, // delete the entire association for deleted inserts
                        (ModificationType::Insert, true) => assoc.source.unwrap() != TrainSource::ShortTerm || assoc.validity[0].valid_begin != begin,
                        (ModificationType::Amend, _) => true, // for deleted amendments we never delete an item here
                        (ModificationType::Delete, _) => true, // for deleted cancellations we never delete an item here
                    }
                }
                else {
                    true
                }
            });

            // now we clean up modifications/cancellations for the pending list
            if matches!(stp_modification_type, ModificationType::Amend) || matches!(stp_modification_type, ModificationType::Delete) {
                for (ref mut assoc, ref _category) in old_assoc.iter_mut() {
                    if assoc.other_train_id == other_train_id && assoc.other_train_location_id_suffix == other_train_location_suffix {
                        match stp_modification_type {
                            ModificationType::Insert => panic!("Insert found where Amend or Cancel expected"),
                            ModificationType::Amend => assoc.replacements.retain(|replacement| replacement.validity[0].valid_begin != begin),
                            ModificationType::Delete => assoc.cancellations.retain(|(cancellation, _days_of_week)| cancellation.valid_begin != begin),
                        }
                    }
                }
            }

            self.unwritten_assocs.insert((main_train_id.to_string(), location.to_string(), location_suffix), old_assoc);

            return Ok(schedule);
        }

        let parsed_end_date = NaiveDate::parse_from_str(&line[21..27], "%y%m%d");
        let parsed_end_date = match parsed_end_date {
            Ok(x) => x,
            Err(x) => return Err(CifError { error_type: CifErrorType::ChronoParseError(x), line: number, column: 21 }),
        };
        let end = London.from_local_datetime(&parsed_end_date.and_hms_opt(0, 0, 0).unwrap()).unwrap();
        let days_of_week = DaysOfWeek {
            monday: &line[27..28] == "1",
            tuesday: &line[28..29] == "1",
            wednesday: &line[29..30] == "1",
            thursday: &line[30..31] == "1",
            friday: &line[31..32] == "1",
            saturday: &line[32..33] == "1",
            sunday: &line[33..34] == "1",
        };

        // Now we handle STP cancellations; these are where long-running
        // associations are deleted as a one-off
        if matches!(stp_modification_type, ModificationType::Delete) && matches!(modification_type, ModificationType::Insert) {
            // modify written ones
            self.trains_cancel_assoc(schedule.trains.get_mut(main_train_id).as_mut().unwrap_or(&mut &mut vec![]), &other_train_id, &begin, &end, &days_of_week, &location, &location_suffix, &other_train_location_suffix);
            self.trains_cancel_rev_assoc(schedule.trains.get_mut(other_train_id).as_mut().unwrap_or(&mut &mut vec![]), &main_train_id, &begin, &end, &days_of_week, &location, &other_train_location_suffix, &location_suffix);

            // now modify in unwritten associations
            let old_assoc = self.unwritten_assocs.remove(&(main_train_id.to_string(), location.to_string(), location_suffix.clone()));
            let mut old_assoc = match old_assoc {
                None => return Ok(schedule),
                Some(x) => x,
            };

            // we cancel in the pending list
            for (assoc, _category) in old_assoc.iter_mut() {
                if assoc.other_train_id == other_train_id && assoc.other_train_location_id_suffix == other_train_location_suffix {
                    // check for no overlapping days at all
                    if days_of_week.into_iter().zip(assoc.days.into_iter()).find(|(new_day, assoc_day)| *new_day && *assoc_day).is_none() {
                        continue;
                    }
                    let new_begin = if begin > assoc.validity[0].valid_begin {
                        begin.clone()
                    }
                    else {
                        assoc.validity[0].valid_begin.clone()
                    };
                    let new_end = if end < assoc.validity[0].valid_end {
                        end.clone()
                    }
                    else {
                        assoc.validity[0].valid_end.clone()
                    };
                    if new_end < new_begin {
                        continue;
                    }
                    let new_cancel = TrainValidityPeriod {
                        valid_begin: new_begin,
                        valid_end: new_end,
                    };
                    assoc.cancellations.push((new_cancel, days_of_week.clone()))
                }
            }

            self.unwritten_assocs.insert((main_train_id.to_string(), location.to_string(), location_suffix), old_assoc);

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

        if matches!(modification_type, ModificationType::Insert) && matches!(stp_modification_type, ModificationType::Insert) {
            // As trains might not all have appeared yet, we temporarily add to unwritten_assocs
            let new_assoc = AssociationNode {
                other_train_id: other_train_id.to_string(),
                other_train_location_id_suffix: other_train_location_suffix,
                validity: vec![TrainValidityPeriod {
                    valid_begin: begin,
                    valid_end: end,
                }],
                cancellations: vec![],
                replacements: vec![],
                days: days_of_week,
                day_diff,
                for_passengers,
                source: Some(if is_stp { TrainSource::LongTerm } else { TrainSource::ShortTerm }),
            };

            self.unwritten_assocs.entry((main_train_id.to_string(), location.to_string(), location_suffix)).or_insert(vec![]).push((new_assoc, category));

            return Ok(schedule);
        }

        if matches!(modification_type, ModificationType::Amend) {
            // first find any committed associations and modify
            self.trains_amend_assoc(schedule.trains.get_mut(main_train_id).as_mut().unwrap_or(&mut &mut vec![]), &other_train_id, &begin, &end, &days_of_week, &location, &location_suffix, &other_train_location_suffix, &stp_modification_type, is_stp, day_diff, for_passengers);

            let rev_days_of_week = rev_days(&days_of_week, day_diff);
            let rev_begin = rev_date(&begin, day_diff);
            let rev_end = rev_date(&end, day_diff);
            self.trains_amend_rev_assoc(schedule.trains.get_mut(other_train_id).as_mut().unwrap_or(&mut &mut vec![]), &main_train_id, &rev_begin, &rev_end, &rev_days_of_week, &location, &other_train_location_suffix, &location_suffix, &stp_modification_type, is_stp, -day_diff, for_passengers);

            // now amend unwritten associations
            let old_assoc = self.unwritten_assocs.remove(&(main_train_id.to_string(), location.to_string(), location_suffix.clone()));
            let mut old_assoc = match old_assoc {
                None => return Ok(schedule),
                Some(x) => x,
            };

            // first we amend the pending list
            for (ref mut assoc, ref mut assoc_category) in old_assoc.iter_mut() {
                if assoc.other_train_id == other_train_id && assoc.other_train_location_id_suffix == other_train_location_suffix {
                    if match (&stp_modification_type, &is_stp) {
                            (ModificationType::Insert, false) => assoc.source.unwrap() == TrainSource::LongTerm && assoc.validity[0].valid_begin == begin,
                            (ModificationType::Insert, true) => assoc.source.unwrap() == TrainSource::ShortTerm && assoc.validity[0].valid_begin == begin,
                            (ModificationType::Amend, _) => false,
                            (ModificationType::Delete, _) => false,
                        } {
                        assoc.validity = vec![TrainValidityPeriod {
                            valid_begin: begin,
                            valid_end: end,
                        }];
                        assoc.days = days_of_week.clone();
                        assoc.day_diff = day_diff;
                        assoc.for_passengers = for_passengers;
                        *assoc_category = category.clone();
                    }
                }
            }

            // now we clean up modifications/cancellations for the pending list
            if matches!(stp_modification_type, ModificationType::Amend) || matches!(stp_modification_type, ModificationType::Delete) {
                for (ref mut assoc, ref _category) in old_assoc.iter_mut() {
                    if assoc.other_train_id == other_train_id && assoc.other_train_location_id_suffix == other_train_location_suffix {
                        if matches!(stp_modification_type, ModificationType::Amend) {
                            for replacement in assoc.replacements.iter_mut() {
                                if replacement.validity[0].valid_begin == begin {
                                    replacement.validity = vec![TrainValidityPeriod {
                                        valid_begin: begin,
                                        valid_end: end,
                                    }];
                                    replacement.days = days_of_week.clone();
                                    replacement.day_diff = day_diff;
                                    replacement.for_passengers = for_passengers;
                                }
                            }
                        }
                        else if matches!(stp_modification_type, ModificationType::Delete) {
                            for (cancellation, old_days_of_week) in assoc.cancellations.iter_mut() {
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
            }

            self.unwritten_assocs.insert((main_train_id.to_string(), location.to_string(), location_suffix), old_assoc);

            return Ok(schedule);
        }

        if matches!(stp_modification_type, ModificationType::Amend) {
            let new_assoc = AssociationNode {
                other_train_id: other_train_id.to_string(),
                other_train_location_id_suffix: other_train_location_suffix.clone(),
                validity: vec![TrainValidityPeriod {
                    valid_begin: begin.clone(),
                    valid_end: end.clone(),
                }],
                cancellations: vec![],
                replacements: vec![],
                days: days_of_week,
                day_diff,
                for_passengers,
                source: Some(TrainSource::ShortTerm),
            };

            let rev_days_of_week = rev_days(&days_of_week, day_diff);
            let rev_begin = rev_date(&begin, day_diff);
            let rev_end = rev_date(&end, day_diff);
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
                source: Some(TrainSource::ShortTerm),
            };

            // first find any committed associations and modify
            self.trains_replace_assoc(schedule.trains.get_mut(main_train_id).as_mut().unwrap_or(&mut &mut vec![]), &other_train_id, &location, &location_suffix, &other_train_location_suffix, &new_assoc);
            self.trains_replace_rev_assoc(schedule.trains.get_mut(other_train_id).as_mut().unwrap_or(&mut &mut vec![]), &main_train_id, &location, &other_train_location_suffix, &location_suffix, &new_rev_assoc);

            // now amend unwritten associations
            let old_assoc = self.unwritten_assocs.remove(&(main_train_id.to_string(), location.to_string(), location_suffix.clone()));
            let mut old_assoc = match old_assoc {
                None => return Ok(schedule),
                Some(x) => x,
            };

            // we amend the pending list
            for (ref mut assoc, _assoc_category) in old_assoc.iter_mut() {
                if assoc.other_train_id == other_train_id && assoc.other_train_location_id_suffix == other_train_location_suffix {
                    if days_of_week.into_iter().zip(assoc.days.into_iter()).find(|(new_day, assoc_day)| *new_day && *assoc_day).is_none() {
                        continue;
                    }
                    let new_begin = if begin > assoc.validity[0].valid_begin {
                        begin.clone()
                    }
                    else {
                        assoc.validity[0].valid_begin.clone()
                    };
                    let new_end = if end < assoc.validity[0].valid_end {
                        end.clone()
                    }
                    else {
                        assoc.validity[0].valid_end.clone()
                    };
                    if new_end < new_begin {
                        continue;
                    }
                    let new_assoc_fixed_date = AssociationNode {
                        validity: vec![TrainValidityPeriod {
                            valid_begin: new_begin,
                            valid_end: new_end,
                        }],
                        ..new_assoc.clone()
                    };
                    assoc.replacements.push(new_assoc_fixed_date);
                }
            }

            self.unwritten_assocs.insert((main_train_id.to_string(), location.to_string(), location_suffix), old_assoc);

            return Ok(schedule);
        }

        Ok(schedule)
    }

    fn read_basic_schedule(&mut self, line: &str, mut schedule: Schedule, number: u64) -> Result<Schedule, CifError> {
        print!("{}\n", line);
        let modification_type = match &line[2..3] {
            "N" => ModificationType::Insert,
            "D" => ModificationType::Delete,
            "R" => ModificationType::Amend,
            x => return Err(CifError { error_type: CifErrorType::InvalidTransactionType(x.to_string()), line: number, column: 2 } ),
        };
        let stp_modification_type = match &line[79..80] {
            " " => ModificationType::Insert,
            "P" => ModificationType::Insert,
            "N" => ModificationType::Insert,
            "O" => ModificationType::Amend,
            "C" => ModificationType::Delete,
            x => return Err(CifError { error_type: CifErrorType::InvalidStpIndicator(x.to_string()), line: number, column: 79 } ),
        };
        let is_stp = match &line[79..80] {
            " " => false,
            "P" => false,
            "N" => true,
            "O" => true,
            "C" => true,
            x => return Err(CifError { error_type: CifErrorType::InvalidStpIndicator(x.to_string()), line: number, column: 79 } ),
        };

        let main_train_id = &line[3..9];
        let parsed_begin_date = NaiveDate::parse_from_str(&line[9..15], "%y%m%d");
        let parsed_begin_date = match parsed_begin_date {
            Ok(x) => x,
            Err(x) => return Err(CifError { error_type: CifErrorType::ChronoParseError(x), line: number, column: 9 }),
        };
        let begin = London.from_local_datetime(&parsed_begin_date.and_hms_opt(0, 0, 0).unwrap()).unwrap();

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
            "D  " => TrainPower::DieselLocomotive,
            "DEM" => TrainPower::DieselElectricMultipleUnit,
            "DMU" => match &line[53..54] {
                "D" => TrainPower::DieselMechanicalMultipleUnit,
                _   => TrainPower::DieselHydraulicMultipleUnit,
            },
            "E  " => TrainPower::ElectricLocomotive,
            "ED " => TrainPower::ElectricAndDieselLocomotive,
            "EML" => TrainPower::ElectricMultipleUnitWithLocomotive,
            "EMU" => TrainPower::ElectricMultipleUnit,
            "HST" => TrainPower::DieselElectricMultipleUnit,
            x => return Err(CifError { error_type: CifErrorType::InvalidTrainPower(x.to_string()), line: number, column: 50 } ),
        };

        let speed_mph = match line[57..60].parse::<u16>() {
            Ok(speed) => speed,
            Err(_) => return Err(CifError { error_type: CifErrorType::InvalidSpeed(line[57..60].to_string()), line: number, column: 57 } ),
        };

        let speed_m_per_s = f64::from(speed_mph) * (1609.344 / (60. * 60.));

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
                wheelchairs: if wheelchair_reservations { ReservationField::Possible } else { ReservationField::Impossible },
            },
            x => return Err(CifError { error_type: CifErrorType::InvalidReservationType(x.to_string()), line: number, column: 68 } ),
        };

        let mut brand = None;
        for chr in line[74..78].chars() {
            match chr {
                'E' => brand = Some("Eurostar".to_string()),
                ' ' => (),
                x => return Err(CifError { error_type: CifErrorType::InvalidBrand(x.to_string()), line: number, column: 74 } ),
            }
        }

        if matches!(modification_type, ModificationType::Insert) && matches!(stp_modification_type, ModificationType::Insert) {
            // we can write a (partial) train now, and continue updating it later.
            self.last_train = Some(main_train_id.to_string());

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
                    power_type: Some(power_type),
                    timing_allocation: match timing_load_str {
                        None => None,
                        Some(x) => Some(TrainAllocation {
                            id: line[50..57].to_string(),
                            description: x,
                            vehicles: None,
                        }),
                    },
                    actual_allocation: None,
                    timing_speed_m_per_s: Some(speed_m_per_s),
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
                    operator: "".to_string(),
                },
                source: Some(if is_stp { TrainSource::LongTerm } else { TrainSource::ShortTerm }),
                runs_as_required,
                performance_monitoring: None,
                route: vec![],
            };

            schedule.trains.entry(main_train_id.to_string()).or_insert(vec![]).push(new_train);

            return Ok(schedule);
        }
        
        if matches!(modification_type, ModificationType::Amend) {
            // we can write a (partial) train now, and continue updating it later.
            self.last_train = Some(main_train_id.to_string());

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
                        power_type: Some(power_type),
                        timing_allocation: match timing_load_str {
                            None => None,
                            Some(ref x) => Some(TrainAllocation {
                                id: line[50..57].to_string(),
                                description: x.clone(),
                                vehicles: None,
                            }),
                        },
                        actual_allocation: None,
                        timing_speed_m_per_s: Some(speed_m_per_s),
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
                        operator: "".to_string(),
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
                                    power_type: Some(power_type),
                                    timing_allocation: match timing_load_str {
                                        None => None,
                                        Some(ref x) => Some(TrainAllocation {
                                            id: line[50..57].to_string(),
                                            description: x.clone(),
                                            vehicles: None,
                                        }),
                                    },
                                    actual_allocation: None,
                                    timing_speed_m_per_s: Some(speed_m_per_s),
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
                                    operator: "".to_string(),
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
            self.last_train = Some(main_train_id.to_string());

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
                        power_type: Some(power_type),
                        timing_allocation: match timing_load_str {
                            None => None,
                            Some(ref x) => Some(TrainAllocation {
                                id: line[50..57].to_string(),
                                description: x.clone(),
                                vehicles: None,
                            }),
                        },
                        actual_allocation: None,
                        timing_speed_m_per_s: Some(speed_m_per_s),
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
                        operator: "".to_string(),
                    },
                    source: Some(if is_stp { TrainSource::LongTerm } else { TrainSource::ShortTerm }),
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
