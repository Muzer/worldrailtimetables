use crate::schedule::{AssociationNode, DaysOfWeek, Location, Schedule, Train, TrainSource, TrainValidityPeriod};
use crate::importer::Importer;
use crate::error::Error;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone};
use chrono::format::ParseError;
use chrono_tz::Tz;
use chrono_tz::Europe::London;

use std::collections::HashMap;
use std::fmt;
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

enum ModificationType {
    Insert,
    Amend,
    Delete,
}

enum AssociationCategory {
    Join,
    Divide,
    Next,
}

impl CifImporter {
    pub fn new() -> CifImporter {
        CifImporter { ..Default::default() }
    }

    fn delete_single_vec_assocs(&self, assocs: &mut Vec<AssociationNode>, other_train_id: &str, begin: &DateTime::<Tz>, other_train_location_suffix: &Option<String>, stp_modification_type: &ModificationType, is_stp: bool) {
        assocs.retain(|assoc| match (&stp_modification_type, &is_stp) {
            (ModificationType::Insert, false) => assoc.source != TrainSource::LongTerm || assoc.validity[0].valid_begin != *begin, // delete the entire association for deleted inserts
            (ModificationType::Insert, true) => assoc.source != TrainSource::ShortTerm || assoc.validity[0].valid_begin != *begin,
            (ModificationType::Amend, _) => true, // for deleted amendments we never delete an item here
            (ModificationType::Delete, _) => true, // for deleted cancellations we never delete an item here
        } || other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix);
        for ref mut assoc in assocs.iter_mut() {
            assoc.replacements.retain(|assoc| match &stp_modification_type {
                ModificationType::Insert => true, // never delete from here for insertions
                ModificationType::Amend => assoc.validity[0].valid_begin != *begin, // for deleted amendments we delete the actual replacement along with cleaning up the replacement list in the original
                ModificationType::Delete => true, // for deleted cancellations we never delete an item here
            } || other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix);
            assoc.cancellations.retain(|(validity, _days_of_week)| match &stp_modification_type {
                ModificationType::Insert => true, // never delete from here for insertions
                ModificationType::Amend => true,
                ModificationType::Delete => validity.valid_begin != *begin,
            } || other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix)
        }
    }

    fn cancel_single_vec_assocs(&self, assocs: &mut Vec<AssociationNode>, other_train_id: &str, begin: &DateTime::<Tz>, end: &DateTime::<Tz>, days_of_week: &DaysOfWeek, other_train_location_suffix: &Option<String>) {
        for ref mut assoc in assocs.iter_mut() {
            if !(other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix) {
                // check for no overlapping days at all
                if days_of_week.into_iter().zip(assoc.days.into_iter()).find(|(new_day, assoc_day)| *new_day && *assoc_day).is_none() {
                    continue;
                }
                let new_begin = if begin > &assoc.validity.iter().min_by(|x, y| x.valid_begin.cmp(&y.valid_begin)).unwrap().valid_begin {
                    begin.clone()
                }
                else {
                    assoc.validity.iter().min_by(|x, y| x.valid_begin.cmp(&y.valid_begin)).unwrap().valid_begin.clone()
                };
                let new_end = if end < &assoc.validity.iter().max_by(|x, y| x.valid_end.cmp(&y.valid_end)).unwrap().valid_end {
                    end.clone()
                }
                else {
                    assoc.validity.iter().max_by(|x, y| x.valid_end.cmp(&y.valid_end)).unwrap().valid_end.clone()
                };
                if end < begin {
                    continue;
                }
                let new_cancel = TrainValidityPeriod {
                    valid_begin: new_begin,
                    valid_end: new_end
                };
                assoc.cancellations.push((new_cancel, days_of_week.clone()))
            }
        }
    }

    fn trains_delete_assoc(&self, trains: &mut Vec<Train>, other_train_id: &str, begin: &DateTime::<Tz>, location: &str, location_suffix: &Option<String>, other_train_location_suffix: &Option<String>, stp_modification_type: &ModificationType, is_stp: bool) {
        for ref mut train in trains.iter_mut() {
            // recurse on replacements
            self.trains_delete_assoc(&mut train.replacements, &other_train_id, &begin, &location, &location_suffix, &other_train_location_suffix, &stp_modification_type, is_stp);

            for ref mut train_location in train.route.iter_mut() {
                if train_location.id == location && train_location.id_suffix == *location_suffix {
                    self.delete_single_vec_assocs(&mut train_location.divides_to_form, other_train_id, begin, other_train_location_suffix, stp_modification_type, is_stp);
                    self.delete_single_vec_assocs(&mut train_location.joins_to, other_train_id, begin, other_train_location_suffix, stp_modification_type, is_stp);
                    if let Some(assoc) = &train_location.becomes {
                        if !(match (&stp_modification_type, &is_stp) {
                                (ModificationType::Insert, false) => assoc.source != TrainSource::LongTerm || assoc.validity[0].valid_begin != *begin, // delete the entire association for deleted inserts
                                (ModificationType::Insert, true) => assoc.source != TrainSource::ShortTerm || assoc.validity[0].valid_begin != *begin,
                                (ModificationType::Amend, _) => true, // for deleted amendments we never delete an item here
                                (ModificationType::Delete, _) => true, // for deleted cancellations we never delete an item here
                            } || other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix) {
                            train_location.becomes = None;
                        }
                    }
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
                        } || other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix)
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
                    self.delete_single_vec_assocs(&mut train_location.divides_from, other_train_id, begin, other_train_location_suffix, stp_modification_type, is_stp);
                    self.delete_single_vec_assocs(&mut train_location.is_joined_to_by, other_train_id, begin, other_train_location_suffix, stp_modification_type, is_stp);
                    if let Some(assoc) = &train_location.forms_from {
                        if !(match (&stp_modification_type, &is_stp) {
                                (ModificationType::Insert, false) => assoc.source != TrainSource::LongTerm || assoc.validity[0].valid_begin != *begin, // delete the entire association for deleted inserts
                                (ModificationType::Insert, true) => assoc.source != TrainSource::ShortTerm || assoc.validity[0].valid_begin != *begin,
                                (ModificationType::Amend, _) => true, // for deleted amendments we never delete an item here
                                (ModificationType::Delete, _) => true, // for deleted cancellations we never delete an item here
                            } || other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix) {
                            train_location.forms_from = None;
                        }
                    }
                    if let Some(ref mut assoc) = &mut train_location.forms_from {
                        assoc.replacements.retain(|assoc| match &stp_modification_type {
                            ModificationType::Insert => true, // never delete from here for insertions
                            ModificationType::Amend => assoc.validity[0].valid_begin != *begin, // for deleted amendments we delete the actual replacement along with cleaning up the replacement list in the original
                            ModificationType::Delete => true, // for deleted cancellations we never delete an item here
                        } || other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix);
                        assoc.cancellations.retain(|(validity, _days_of_week)| match &stp_modification_type {
                            ModificationType::Insert => true, // never delete from here for insertions
                            ModificationType::Amend => true,
                            ModificationType::Delete => validity.valid_begin != *begin,
                        } || other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix)
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
                    self.cancel_single_vec_assocs(&mut train_location.divides_to_form, other_train_id, begin, end, days_of_week, other_train_location_suffix);
                    self.cancel_single_vec_assocs(&mut train_location.joins_to, other_train_id, begin, end, days_of_week, other_train_location_suffix);
                    if let Some(assoc) = &mut train_location.becomes {
                        if !(other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix) {
                            // check for no overlapping days at all
                            if days_of_week.into_iter().zip(assoc.days.into_iter()).find(|(new_day, assoc_day)| *new_day && *assoc_day).is_none() {
                                continue;
                            }
                            let new_begin = if begin > &assoc.validity.iter().min_by(|x, y| x.valid_begin.cmp(&y.valid_begin)).unwrap().valid_begin {
                                begin.clone()
                            }
                            else {
                                assoc.validity.iter().min_by(|x, y| x.valid_begin.cmp(&y.valid_begin)).unwrap().valid_begin.clone()
                            };
                            let new_end = if end < &assoc.validity.iter().max_by(|x, y| x.valid_end.cmp(&y.valid_end)).unwrap().valid_end {
                                end.clone()
                            }
                            else {
                                assoc.validity.iter().max_by(|x, y| x.valid_end.cmp(&y.valid_end)).unwrap().valid_end.clone()
                            };
                            if end < begin {
                                continue;
                            }
                            let new_cancel = TrainValidityPeriod {
                                valid_begin: new_begin,
                                valid_end: new_end
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
                    self.cancel_single_vec_assocs(&mut train_location.divides_from, other_train_id, begin, end, days_of_week, other_train_location_suffix);
                    self.cancel_single_vec_assocs(&mut train_location.is_joined_to_by, other_train_id, begin, end, days_of_week, other_train_location_suffix);
                    if let Some(assoc) = &mut train_location.forms_from {
                        if !(other_train_id != assoc.other_train_id || *other_train_location_suffix != assoc.other_train_location_id_suffix) {
                            // check for no overlapping days at all
                            if days_of_week.into_iter().zip(assoc.days.into_iter()).find(|(new_day, assoc_day)| *new_day && *assoc_day).is_none() {
                                continue;
                            }
                            let new_begin = if begin > &assoc.validity.iter().min_by(|x, y| x.valid_begin.cmp(&y.valid_begin)).unwrap().valid_begin {
                                begin.clone()
                            }
                            else {
                                assoc.validity.iter().min_by(|x, y| x.valid_begin.cmp(&y.valid_begin)).unwrap().valid_begin.clone()
                            };
                            let new_end = if end < &assoc.validity.iter().max_by(|x, y| x.valid_end.cmp(&y.valid_end)).unwrap().valid_end {
                                end.clone()
                            }
                            else {
                                assoc.validity.iter().max_by(|x, y| x.valid_end.cmp(&y.valid_end)).unwrap().valid_end.clone()
                            };
                            if end < begin {
                                continue;
                            }
                            let new_cancel = TrainValidityPeriod {
                                valid_begin: new_begin,
                                valid_end: new_end
                            };
                            assoc.cancellations.push((new_cancel, days_of_week.clone()))
                        }
                    }
                }
            }
        }
    }

    fn read_association(&mut self, line: &str, mut schedule: Schedule, number: u64) -> Result<Schedule, CifError> {
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
                        (ModificationType::Insert, false) => assoc.source != TrainSource::LongTerm || assoc.validity[0].valid_begin != begin, // delete the entire association for deleted inserts
                        (ModificationType::Insert, true) => assoc.source != TrainSource::ShortTerm || assoc.validity[0].valid_begin != begin,
                        (ModificationType::Amend, _) => true, // for deleted amendments we never delete an item here
                        (ModificationType::Delete, _) => true, // for deleted cancellations we never delete an item here
                    }
                }
                else {
                    true
                }
            });

            // now we clean up validities for the pending list
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
        if matches!(stp_modification_type, ModificationType::Delete) {
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
                    let new_begin = if begin > assoc.validity.iter().min_by(|x, y| x.valid_begin.cmp(&y.valid_begin)).unwrap().valid_begin {
                        begin.clone()
                    }
                    else {
                        assoc.validity.iter().min_by(|x, y| x.valid_begin.cmp(&y.valid_begin)).unwrap().valid_begin.clone()
                    };
                    let new_end = if end < assoc.validity.iter().max_by(|x, y| x.valid_end.cmp(&y.valid_end)).unwrap().valid_end {
                        end.clone()
                    }
                    else {
                        assoc.validity.iter().max_by(|x, y| x.valid_end.cmp(&y.valid_end)).unwrap().valid_end.clone()
                    };
                    if end < begin {
                        continue;
                    }
                    let new_cancel = TrainValidityPeriod {
                        valid_begin: new_begin,
                        valid_end: new_end
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
            x => return Err(CifError { error_type: CifErrorType::InvalidAssociationDateIndicator(x.to_string()), line: number, column: 37 } ),
        };
        let for_passengers = match &line[47..48] {
            "P" => true,
            "O" => false,
            x => return Err(CifError { error_type: CifErrorType::InvalidAssociationType(x.to_string()), line: number, column: 49 } ),
        };

        let category = match &line[34..36] {
            "JJ" => AssociationCategory::Join,
            "VV" => AssociationCategory::Divide,
            "NP" => AssociationCategory::Next,
            x => return Err(CifError { error_type: CifErrorType::InvalidAssociationCategory(x.to_string()), line: number, column: 35 } ),
        };

        if matches!(modification_type, ModificationType::Insert) && matches!(stp_modification_type, ModificationType::Insert) {
            // As trains might not all have appeared yet, we temporarily add to unwritten_assocs
            let new_assoc = AssociationNode {
                other_train_id: other_train_id.to_string(),
                other_train_location_id_suffix: other_train_location_suffix,
                validity: vec![TrainValidityPeriod {
                    valid_begin: begin,
                    valid_end: end
                }],
                cancellations: vec![],
                replacements: vec![],
                days: days_of_week,
                day_diff,
                for_passengers,
                source: if is_stp { TrainSource::LongTerm } else { TrainSource::ShortTerm }
            };

            self.unwritten_assocs.entry((main_train_id.to_string(), location.to_string(), location_suffix)).or_insert(vec![]).push((new_assoc, category));

            return Ok(schedule);
        }

        if matches!(modification_type, ModificationType::Amend) {
            // first find any committed associations and modify
            self.trains_amend_assoc(schedule.trains.get_mut(main_train_id).as_mut().unwrap_or(&mut &mut vec![]), &other_train_id, &begin, &end, &days_of_week, &location, &location_suffix, &other_train_location_suffix, &stp_modification_type, is_stp, &day_diff, for_passengers);
            self.trains_amend_rev_assoc(schedule.trains.get_mut(other_train_id).as_mut().unwrap_or(&mut &mut vec![]), &main_train_id, &begin, &end, &days_of_week, &location, &other_train_location_suffix, &location_suffix, &stp_modification_type, is_stp, &day_diff, for_passengers);

            // now amend unwritten associations
            let old_assoc = self.unwritten_assocs.remove(&(main_train_id.to_string(), location.to_string(), location_suffix.clone()));
            let mut old_assoc = match old_assoc {
                None => return Ok(schedule),
                Some(x) => x,
            };

            // first we amend the pending list
            for (ref mut assoc, ref mut category) in old_assoc.iter_mut() {
                if assoc.other_train_id == other_train_id && assoc.other_train_location_id_suffix == other_train_location_suffix {
                    if match (&stp_modification_type, &is_stp) {
                            (ModificationType::Insert, false) => assoc.source == TrainSource::LongTerm && assoc.validity[0].valid_begin == begin,
                            (ModificationType::Insert, true) => assoc.source == TrainSource::ShortTerm && assoc.validity[0].valid_begin == begin,
                            (ModificationType::Amend, _) => false,
                            (ModificationType::Delete, _) => false,
                        } {
                        assoc.validity = vec![TrainValidityPeriod {
                            valid_begin: begin,
                            valid_end: end
                        }];
                        assoc.days = days_of_week;
                        assoc.day_diff = day_diff;
                        assoc.for_passengers = for_passengers;
                    }
                }
            }

            // now we clean up validities for the pending list TODO finish this
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

        // TODO STP modification

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
