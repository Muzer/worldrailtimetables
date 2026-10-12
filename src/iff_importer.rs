use crate::error::Error;
use crate::importer::SlowMultiFileImporter;
use crate::schedule::{
    AccommodationClass, ReservationField, TrainSource, TrainType, accommodation_types, brand,
    location, schedule, train, train_location, train_validity_period, train_variant, train_operator,
    variable_train,
};

use async_trait::async_trait;

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, Timelike};
use chrono::format::ParseError;
use chrono::naive::Days;
use chrono_tz::Europe::{Amsterdam, London};

use encoding_rs::{Decoder, Encoding};

use sea_orm::DatabaseTransaction;

use tokio::io::{AsyncBufRead, AsyncBufReadExt};

use sea_orm::EntityTrait;
use sea_orm::entity::{ActiveBelongsTo, ActiveHasMany, ActiveHasOne, ActiveValue};

use std::borrow::BorrowMut;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::io::{Error as IoError};

#[derive(Clone, Debug)]
pub enum IffErrorType {
    ChronoParseError(ParseError),
    DayBitsDontMatchPeriodLength(i64),
    FootnoteNotFound(String),
    InvalidStopIndex(String),
    MissingFile,
    MissingFootnoteDetail,
    MissingFootnoteId,
    MissingTimezoneDetail,
    MissingTimezoneId,
    MissingTripNumberRecord,
    MissingValidity,
    NotEnoughFields,
    PassWithoutStop,
    TimeParseError(String),
    TimezoneNotFound(String),
    TrackWithoutLocation,
    UnexpectedFootnote(String),
    UnexpectedRecordType(String),
    UnknownAttribute(String),
    UnknownMode(String),
    UnknownTimezone(String),
}

impl Display for IffErrorType {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            IffErrorType::ChronoParseError(x)
                => write!(f, "Failed to parse date and/or time: {}", x),
            IffErrorType::DayBitsDontMatchPeriodLength(x)
                => write!(f, "Number of day bits does not match period length {}", x),
            IffErrorType::FootnoteNotFound(x) => write!(f, "Footnote {} not found", x),
            IffErrorType::InvalidStopIndex(x) => write!(f, "Invalid stop index {}", x),
            IffErrorType::MissingFile => write!(f, "Missing file"),
            IffErrorType::MissingFootnoteDetail => write!(f, "Missing footnote detail"),
            IffErrorType::MissingFootnoteId => write!(f, "Missing footnote ID"),
            IffErrorType::MissingTimezoneDetail => write!(f, "Missing timezone detail"),
            IffErrorType::MissingTimezoneId => write!(f, "Missing timezone ID"),
            IffErrorType::MissingTripNumberRecord => write!(f, "Missing trip number record"),
            IffErrorType::MissingValidity => write!(f, "Validity period not found in file"),
            IffErrorType::NotEnoughFields => write!(f, "Not enough fields"),
            IffErrorType::PassWithoutStop => write!(f, "Pass record without stop record"),
            IffErrorType::TimeParseError(x) => write!(f, "Failed to parse time {}", x),
            IffErrorType::TimezoneNotFound(x) => write!(f, "Timezone {} not found", x),
            IffErrorType::TrackWithoutLocation => write!(f, "Track record without location record"),
            IffErrorType::UnexpectedFootnote(x) => write!(f, "Unexpected footnote {}", x),
            IffErrorType::UnexpectedRecordType(x) => write!(f, "Unexpected record type {}", x),
            IffErrorType::UnknownAttribute(x) => write!(f, "Unknown attribute {}", x),
            IffErrorType::UnknownMode(x) => write!(f, "Unknown mode of transport {}", x),
            IffErrorType::UnknownTimezone(x) => write!(f, "Unknown timezone offset {}", x),
        }
    }
}

#[derive(Debug)]
pub struct IffError {
    error_type: IffErrorType,
    filename: String,
    line: usize,
}

impl Display for IffError {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "Error reading NS file {} line {}: {}", self.filename, self.line, self.error_type)
    }
}
#[derive(Default)]
pub struct IffImporter {
    timezone_by_id: HashMap<String, String>,
    day_bits_by_id: HashMap<String, String>,
    current_train_variant: Option<train_variant::ActiveModelEx>,
    changes_en_route: HashMap<usize, variable_train::ActiveModelEx>,
    no_boarding_stops: HashSet<usize>,
    no_alighting_stops: HashSet<usize>,
    cur_footnote_id: String,
}

fn read_date(date_slice: &str, filename: &str, line: usize) -> Result<NaiveDateTime, IffError> {
    let parsed_date = NaiveDate::parse_from_str(date_slice, "%d%m%Y");
    let parsed_date = match parsed_date {
        Ok(x) => x,
        Err(x) => return Err(IffError {
            error_type: IffErrorType::ChronoParseError(x),
            filename: filename.to_string(),
            line: line,
        }),
    };
    Ok(parsed_date.and_hms_opt(0, 0, 0).unwrap())
}

fn read_time(time_slice: &str, filename: &str, line: usize) -> Result<(u8, NaiveTime), IffError> {
    let hours: u8 = match time_slice[0..2].parse() {
        Ok(x) => x,
        Err(_) => return Err(IffError {
            error_type: IffErrorType::TimeParseError(time_slice.to_string()),
            filename: filename.to_string(),
            line: line,
        }),
    };
    let minutes: u8 = match time_slice[2..4].parse() {
        Ok(x) => x,
        Err(_) => return Err(IffError {
            error_type: IffErrorType::TimeParseError(time_slice.to_string()),
            filename: filename.to_string(),
            line: line,
        }),
    };
    let days = hours / 24;
    let hours = hours % 24;
    let time = NaiveTime::from_hms_opt(hours.into(), minutes.into(), 0);
    let time = match time {
        Some(x) => x,
        None => return Err(IffError {
            error_type: IffErrorType::TimeParseError(time_slice.to_string()),
            filename: filename.to_string(),
            line: line,
        }),
    };
    Ok((days, time))
}

fn try_overlay_days(
    mut original: [Option::<bool>; 7],
    overlay: [Option::<bool>; 7],
) -> Option<[Option<bool>; 7]> {
    for i in 0..7 {
        if original[i] != overlay[i] {
            match original[i] {
                Some(_) => {
                    match overlay[i] {
                        Some(_) => { return None; },
                        None => (),
                    }
                },
                None => {
                    original[i] = overlay[i];
                },
            }
        }
    }

    Some(original)
}

fn populate_variable_train_attribute(
    attribute: &str,
    variable_train: &mut variable_train::ActiveModelEx,
    no_boarding_stops: &mut HashSet<usize>,
    no_alighting_stops: &mut HashSet<usize>,
    from_stop: usize,
    to_stop: usize,
    i: usize,
) -> Result<(), IffError> {
    match attribute {
        "TAR " => variable_train.non_standard_fares = ActiveValue::Set(Some(true)),
        "BAR " => variable_train.bar = ActiveValue::Set(Some(true)),
        "RESV" => {
            variable_train.seats = ActiveValue::Set(Some(ReservationField::Mandatory));
            variable_train.wheelchairs = ActiveValue::Set(Some(ReservationField::Mandatory));
            if variable_train.sleepers != ActiveValue::Set(Some(ReservationField::NotApplicable)) {
                variable_train.sleepers = ActiveValue::Set(Some(ReservationField::Mandatory));
            }
        },
        "SPEC" => variable_train.special_ticket_required = ActiveValue::Set(Some(true)),
        "T   " => variable_train.supplement_required = ActiveValue::Set(Some(true)),
        "RESA" => variable_train.international
            = ActiveValue::Set(Some(ReservationField::Recommended)),
        "TWEE" => variable_train.accommodation.as_mut_vec().retain(
                |x| x.class == ActiveValue::Set(AccommodationClass::Second)
            ),
        "REST" => variable_train.restaurant = ActiveValue::Set(Some(true)),
        "GAVR" => variable_train.national_rights_apply = ActiveValue::Set(Some(false)),
        "FIET" => variable_train.luggage_bicycles = ActiveValue::Set(Some(true)),
        "BIST" => variable_train.bistro = ActiveValue::Set(Some(true)),
        "NIIN" => for i in from_stop..to_stop + 1 {
            no_boarding_stops.insert(i);
        },
        "NUIT" => for i in from_stop..to_stop + 1 {
            no_alighting_stops.insert(i);
        },
        "KOPS" => variable_train.drink_available = ActiveValue::Set(Some(true)),
        "LIGW" => {
            for accommodation_types in variable_train.accommodation.as_mut_vec() {
                accommodation_types.couchette = ActiveValue::Set(Some(true));
            }
            variable_train.sleepers = variable_train.seats.clone();
        },
        "SLP " => {
            for accommodation_types in variable_train.accommodation.as_mut_vec() {
                accommodation_types.sleeper = ActiveValue::Set(Some(true));
            }
            variable_train.sleepers = variable_train.seats.clone();
        },
        "TELE" => variable_train.telephone = ActiveValue::Set(Some(true)),
        "EEN " => variable_train.accommodation.as_mut_vec().retain(
                |x| x.class == ActiveValue::Set(AccommodationClass::First)
            ),
        // I don't thiiink these are actionable?
        "XXX1" => (),
        "XXX2" => (),
        "XXX3" => (),
        "FINI" => variable_train.luggage_bicycles = ActiveValue::Set(Some(false)),
        "RESM" => {
            variable_train.seats = ActiveValue::Set(Some(ReservationField::Possible));
            variable_train.wheelchairs = ActiveValue::Set(Some(ReservationField::Possible));
            if variable_train.sleepers != ActiveValue::Set(Some(ReservationField::NotApplicable)) {
                variable_train.sleepers = ActiveValue::Set(Some(ReservationField::Possible));
            }
        },
        "FIVE" => {
            variable_train.luggage_bicycles = ActiveValue::Set(Some(true));
            variable_train.reservations_bicycles
                = ActiveValue::Set(Some(ReservationField::Mandatory));
        },
        "OO  " => variable_train.very_busy = ActiveValue::Set(Some(true)),
        "SPRZ" => variable_train.toilet = ActiveValue::Set(Some(false)),
        "TSR " => variable_train.supplement_required_part = ActiveValue::Set(Some(true)),
        "IRES" => variable_train.international
            = ActiveValue::Set(Some(ReservationField::Mandatory)),
        "BEMU" => variable_train.attraction_visitors_only = ActiveValue::Set(Some(true)),
        "TVIE" => variable_train.supplement_required_rail_passes = ActiveValue::Set(Some(true)),
        "ROL " => {
            variable_train.wheelchair_accessible = ActiveValue::Set(Some(true));
            for accommodation_types in variable_train.accommodation.as_mut_vec() {
                accommodation_types.wheelchair = ActiveValue::Set(Some(true));
            }
        },
        "FMBM" => variable_train.limited_bicycles = ActiveValue::Set(Some(true)),
        "UKT " => variable_train.international_border_check = ActiveValue::Set(Some(true)),
        // We already assume both of these are present unless specified so these are no-ops
        "TKL " => (),
        "EKL " => (),
        "ZORG" => variable_train.baby_changing = ActiveValue::Set(Some(true)),
        "WIFI" => variable_train.wifi = ActiveValue::Set(Some(true)),
        "STOP" => variable_train.mains_sockets = ActiveValue::Set(Some(true)),
        "STIL" => {
            for accommodation_types in variable_train.accommodation.as_mut_vec() {
                accommodation_types.quiet_zone = ActiveValue::Set(Some(true));
            }
        },
        "NROL" => {
            variable_train.wheelchair_accessible = ActiveValue::Set(Some(false));
            for accommodation_types in variable_train.accommodation.as_mut_vec() {
                accommodation_types.wheelchair = ActiveValue::Set(Some(false));
            }
        },
        _ => return Err(IffError {
            error_type: IffErrorType::UnknownAttribute(attribute.to_string()),
            filename: "timetbls_new.dat".to_string(),
            line: i,
        })
    }
    Ok(())
}

fn interpolate_pass_times(
    train_variant: &mut train_variant::ActiveModelEx,
    arr_time: &NaiveTime,
    arr_day: &u8,
) {
    let mut num_divisions = 0;
    let mut dep_time = arr_time.clone();
    let mut dep_day = *arr_day;
    // Count the number of passing stops and the time between here and the previous stop
    for train_location in train_variant.route.as_mut_vec().iter().rev() {
        num_divisions += 1;
        match train_location.working_dep {
            ActiveValue::Set(Some(x)) => {
                dep_time = x.clone();
                dep_day = train_location.working_dep_day.clone().unwrap().unwrap();
                break;
            },
            ActiveValue::Set(None) | ActiveValue::NotSet | ActiveValue::Unchanged(_) => (),
        };
    }

    // Not found/no time difference, either way we can early return
    if dep_time == *arr_time {
        return;
    }

    let second_diff
        = (arr_time.num_seconds_from_midnight() + u32::from(*arr_day) * 86_400)
        - (dep_time.num_seconds_from_midnight() + u32::from(dep_day) * 86_400);

    for (i, train_location) in train_variant.route.as_mut_vec().iter_mut().rev().enumerate() {
        match (
            train_location.working_pass.borrow_mut(),
            train_location.working_pass_day.borrow_mut()
        ) {
            (ActiveValue::Set(Some(working_pass)), ActiveValue::Set(Some(working_pass_day))) => {
                let second_offset = (u32::try_from(i).unwrap() + 1) * (second_diff / num_divisions);
                *working_pass = *arr_time - TimeDelta::seconds(second_offset.into());
                *working_pass_day = (u32::from(*arr_day)
                    - (second_offset + (86_400 - arr_time.num_seconds_from_midnight())) / 86_400)
                .try_into().unwrap();
            },
            _ => break,
        }
    }
}

impl IffImporter {
    pub fn new() -> IffImporter {
        IffImporter {
            ..Default::default()
        }
    }

    async fn read_delivery(
        &self,
        mut lines: LegacyLines<impl AsyncBufRead + Unpin>,
        schedule: &schedule::ModelEx,
        transaction: &DatabaseTransaction,
    ) -> Result<(NaiveDateTime, NaiveDateTime), Error> {
        let mut i: usize = 0;
        let namespace = &schedule.namespace;
        let mut valid_begin = None;
        let mut valid_end = None;
        while let Some(line) = lines.next_line().await? {
            i += 1;
            if line.len() == 0 {
                continue;
            }

            if line[0..1] != *"@" {
                return Err(IffError {
                    error_type: IffErrorType::UnexpectedRecordType(line[0..1].to_string()),
                    filename: "delivery.dat".to_string(),
                    line: i,
                }.into());
            }

            let fields: Vec<String> = line[1..].split(",").map(|x| x.to_string()).collect();
            // Technically we should read company.dat first, but we don't actually care about the
            // company here so whatever.
            // Company,Start date,End date,Version,Description

            if fields.len() != 5 {
                return Err(IffError {
                    error_type: IffErrorType::NotEnoughFields,
                    filename: "delivery.dat".to_string(),
                    line: i,
                }.into());
            }

            let mut schedule: schedule::ActiveModelEx = schedule.clone().into();

            valid_begin = Some(read_date(&fields[1], "delivery.dat", i)?);
            valid_end = Some(read_date(&fields[2], "delivery.dat", i)?);

            schedule.valid_begin = ActiveValue::Set(valid_begin.clone());
            schedule.valid_end = ActiveValue::Set(valid_end.clone());
            schedule.their_id = ActiveValue::Set(Some(fields[3].trim().to_string()));
            schedule.timezone = ActiveValue::Set(Some(Amsterdam.name().to_string()));

            println!("[{}] Updating root schedule...", namespace);
            schedule.update(transaction).await?;
            println!("[{}] Updated root schedule", namespace);
        }
        let (valid_begin, valid_end) = match (valid_begin, valid_end) {
            (Some(x), Some(y)) => (x, y),
            _ => return Err(IffError {
                error_type: IffErrorType::MissingValidity,
                filename: "delivery.dat".to_string(),
                line: i,
            }.into()),
        };
        Ok((valid_begin, valid_end))
    }

    async fn read_company(
        &self,
        mut lines: LegacyLines<impl AsyncBufRead + Unpin>,
        namespace: &str,
        transaction: &DatabaseTransaction,
    ) -> Result<(), Error> {
        let mut i: usize = 0;
        let mut operator_count = 0;
        println!("[{}] Loading operators...", namespace);
        while let Some(line) = lines.next_line().await? {
            i += 1;
            if line.len() == 0 {
                continue;
            }

            if line[0..1] == *"@" {
                // We have already read the header from delivery.dat, so ignore it
                continue;
            }

            let fields: Vec<String> = line.split(",").map(|x| x.to_string()).collect();
            // ID,public ID,name,timezone
            // What is timezone actually used for here? I don't know...

            if fields.len() != 4 {
                return Err(IffError {
                    error_type: IffErrorType::NotEnoughFields,
                    filename: "company.dat".to_string(),
                    line: i,
                }.into());
            }

            let operator = train_operator::ActiveModelEx {
                id: ActiveValue::Set(fields[0].trim().to_string()),
                namespace: ActiveValue::Set(namespace.to_owned()),
                public_id: ActiveValue::Set(Some(fields[1].trim().to_string())),
                description: ActiveValue::Set(Some(fields[2].trim().to_string())),
                ..Default::default()
            };

            operator.insert(transaction).await?;
            operator_count += 1;
        }
        println!("[{}] Persisted {} operators", namespace, operator_count);
        Ok(())
    }

    async fn read_trnsmode(
        &self,
        mut lines: LegacyLines<impl AsyncBufRead + Unpin>,
        namespace: &str,
        transaction: &DatabaseTransaction,
    ) -> Result<(), Error> {
        let mut i: usize = 0;
        let mut brand_count = 0;
        println!("[{}] Loading brands...", namespace);
        while let Some(line) = lines.next_line().await? {
            i += 1;
            if line.len() == 0 {
                continue;
            }

            if line[0..1] == *"@" {
                // We have already read the header from delivery.dat, so ignore it
                continue;
            }

            let fields: Vec<String> = line.split(",").map(|x| x.to_string()).collect();
            // ID,description

            if fields.len() != 2 {
                return Err(IffError {
                    error_type: IffErrorType::NotEnoughFields,
                    filename: "trnsmode.dat".to_string(),
                    line: i,
                }.into());
            }

            let brand = brand::ActiveModelEx {
                id: ActiveValue::Set(fields[0].trim().to_string()),
                namespace: ActiveValue::Set(namespace.to_owned()),
                description: ActiveValue::Set(Some(fields[1].trim().to_string())),
                ..Default::default()
            };

            brand.insert(transaction).await?;
            brand_count += 1;
        }
        println!("[{}] Persisted {} brands", namespace, brand_count);
        Ok(())
    }

    async fn read_timezone(
        &mut self,
        mut lines: LegacyLines<impl AsyncBufRead + Unpin>,
        _namespace: &str,
    ) -> Result<(), Error> {
        let mut i: usize = 0;
        let mut tz_id = None;
        while let Some(line) = lines.next_line().await? {
            i += 1;
            if line.len() == 0 {
                continue;
            }

            if line[0..1] == *"@" {
                // We have already read the header from delivery.dat, so ignore it
                continue;
            }

            if line[0..1] == *"#" {
                if tz_id.is_some() {
                    return Err(IffError {
                        error_type: IffErrorType::MissingTimezoneDetail,
                        filename: "timezone.dat".to_string(),
                        line: i,
                    }.into());
                }

                tz_id = Some(line[1..].trim().to_string());
                continue;
            }

            let id = match tz_id {
                Some(x) => x,
                None => return Err(IffError {
                    error_type: IffErrorType::MissingTimezoneId,
                    filename: "timezone.dat".to_string(),
                    line: i,
                }.into()),
            };

            let fields: Vec<String> = line.split(",").map(|x| x.to_string()).collect();
            // offset,valid_begin,valid_end

            // If DST changes are happening we've got bigger problems so let's ignore the validity
            // period for now

            if fields.len() != 3 {
                return Err(IffError {
                    error_type: IffErrorType::NotEnoughFields,
                    filename: "timezone.dat".to_string(),
                    line: i,
                }.into());
            }

            let timezone = match fields[0].as_str() {
                "-01" => London.name().to_string(),
                "+00" => Amsterdam.name().to_string(),
                _ => return Err(IffError {
                    error_type: IffErrorType::UnknownTimezone(fields[0].to_string()),
                    filename: "timezone.dat".to_string(),
                    line: i,
                }.into()),
            };

            self.timezone_by_id.insert(id, timezone);

            tz_id = None;
        }
        Ok(())
    }

    async fn read_stations(
        &self,
        mut lines: LegacyLines<impl AsyncBufRead + Unpin>,
        namespace: &str,
        transaction: &DatabaseTransaction,
    ) -> Result<(), Error> {
        let mut i: usize = 0;
        let mut location_count = 0;
        println!("[{}] Loading locations...", namespace);
        while let Some(line) = lines.next_line().await? {
            i += 1;
            if line.len() == 0 {
                continue;
            }

            if line[0..1] == *"@" {
                // We have already read the header from delivery.dat, so ignore it
                continue;
            }

            let fields: Vec<String> = line.split(",").map(|x| x.to_string()).collect();
            // interchange,id,connection_time,max_connection_time,country,timezone,spare,x-coord,
            // y-coord,name

            if fields.len() != 10 {
                return Err(IffError {
                    error_type: IffErrorType::NotEnoughFields,
                    filename: "stations.dat".to_string(),
                    line: i,
                }.into());
            }

            let timezone = match self.timezone_by_id.get(fields[5].trim()) {
                Some(x) => x,
                None => return Err(IffError {
                    error_type: IffErrorType::TimezoneNotFound(fields[5].trim().to_string()),
                    filename: "stations.dat".to_string(),
                    line: i,
                }.into()),
            };

            let location = location::ActiveModelEx {
                id: ActiveValue::Set(fields[1].trim().to_string()),
                namespace: ActiveValue::Set(namespace.to_owned()),
                name: ActiveValue::Set(fields[9].trim().to_string()),
                timezone: ActiveValue::Set(timezone.clone()),
                ..Default::default()
            };

            location.insert(transaction).await?;
            location_count += 1;
        }
        println!("[{}] Persisted {} locations", namespace, location_count);
        Ok(())
    }

    async fn read_footnote(
        &mut self,
        mut lines: LegacyLines<impl AsyncBufRead + Unpin>,
        _namespace: &str,
        valid_begin: NaiveDateTime,
        valid_end: NaiveDateTime,
    ) -> Result<(), Error> {
        let mut i: usize = 0;
        let mut footnote_id = None;
        while let Some(line) = lines.next_line().await? {
            i += 1;
            if line.len() == 0 {
                continue;
            }

            if line[0..1] == *"@" {
                // We have already read the header from delivery.dat, so ignore it
                continue;
            }

            if line[0..1] == *"#" {
                if footnote_id.is_some() {
                    return Err(IffError {
                        error_type: IffErrorType::MissingFootnoteDetail,
                        filename: "footnote.dat".to_string(),
                        line: i,
                    }.into());
                }

                footnote_id = Some(line[1..].trim().to_string());
                continue;
            }

            let id = match footnote_id {
                Some(x) => x,
                None => return Err(IffError {
                    error_type: IffErrorType::MissingFootnoteId,
                    filename: "footnote.dat".to_string(),
                    line: i,
                }.into()),
            };

            // valid_day_bits should be the same length as the period between the two dates plus one
            // (valid days are inclusive)
            let days = (valid_end.date() - valid_begin.date()).num_days() + 1;
            if line.len() != usize::try_from(days).unwrap() {
                return Err(
                    IffError {
                        error_type: IffErrorType::DayBitsDontMatchPeriodLength(days),
                        filename: "footnote.dat".to_string(),
                        line: i,
                    }.into()
                );
            }

            self.day_bits_by_id.insert(id, line.trim().to_string());

            footnote_id = None;
        }
        Ok(())
    }

    async fn read_timetbls_write_train(
        &self, train_variant: &train_variant::ActiveModelEx, transaction: &DatabaseTransaction
    ) -> Result<(), Error> {
        let mut train_variant = train_variant.clone();
        let mut route = train_variant.route.into_vec();
        train_variant.route = ActiveHasMany::NotSet;
        let inserted = train_variant.insert(transaction).await?;
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
        Ok(())
    }

    async fn read_timetbls_trip_number(
        &mut self,
        line: &str,
        namespace: &str,
        _i: usize,
        transaction: &DatabaseTransaction
    ) -> Result<usize, Error> {
        let mut train_count: usize = 0;
        match &self.current_train_variant {
            None => (),
            Some(train_variant) => {
                self.read_timetbls_write_train(train_variant, transaction).await?;

                train_count += 1;
            },
        };

        self.changes_en_route.clear();
        self.no_boarding_stops.clear();
        self.no_alighting_stops.clear();
        // This is only for validation so isn't too important that it's super rigorous
        self.cur_footnote_id = "".to_string();

        self.current_train_variant = Some(
            train_variant::ActiveModelEx {
                namespace: ActiveValue::Set(namespace.to_string()),
                train: ActiveBelongsTo::Set(Some(Box::new(train::ActiveModelEx {
                    id: ActiveValue::Set(line.trim().to_string()),
                    namespace: ActiveValue::Set(namespace.to_string()),
                    ..Default::default()
                }))),
                // no distinction between long and short in IFF
                source: ActiveValue::Set(Some(TrainSource::LongTerm)),
                runs_as_required: ActiveValue::Set(false),
                ..Default::default()
            }
        );
        Ok(train_count)
    }

    async fn read_timetbls_operator(
        &mut self,
        line: &str,
        namespace: &str,
        i: usize,
    ) -> Result<(), Error> {
        let train_variant = match self.current_train_variant.as_mut() {
            None => return Err(IffError {
                error_type: IffErrorType::MissingTripNumberRecord,
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into()),
            Some(train_variant) => train_variant,
        };

        let fields: Vec<String> = line.split(",").map(|x| x.to_string()).collect();
        // operator,train_number,line_number,first_stop,last_stop,name
        if fields.len() != 6 {
            return Err(IffError {
                error_type: IffErrorType::NotEnoughFields,
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into());
        }

        // We determine if we are setting this now as the first variable_train, or storing it as a
        // change en route
        let from_stop_index: usize = match fields[3].trim().parse() {
            Ok(x) => x,
            Err(_) => return Err(IffError {
                error_type: IffErrorType::InvalidStopIndex(fields[3].trim().to_string()),
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into()),
        };
        // Ignore the to_stop_index as we can assume there will only be one operator at a time with
        // no gaps. If this turns out to be wrong we can always implement that logic...

        // The line number field is nowadays just used for Rail Replacement Bus number, so we can
        // stick that in the headcode field.
        let headcode = fields[2].trim();
        let headcode = if headcode.len() == 0 {
            None
        } else {
            Some(headcode.to_string())
        };

        let name = fields[5].trim();
        let name = if name.len() == 0 {
            None
        } else {
            Some(name.to_string())
        };

        let variable_train = variable_train::ActiveModelEx {
            namespace: ActiveValue::Set(namespace.to_string()),
            operator_id: ActiveValue::Set(Some(fields[0].trim().to_string())),
            public_id: ActiveValue::Set(Some(fields[1].trim().to_string())),
            headcode: ActiveValue::Set(headcode),
            name: ActiveValue::Set(name),
            // We set some defaults as the available attributes imply we will get this data
            has_operating_characteristics: ActiveValue::Set(false),
            has_reservations: ActiveValue::Set(true),
            seats: ActiveValue::Set(Some(ReservationField::NotMandatory)),
            groups: ActiveValue::Set(Some(ReservationField::Unknown)),
            first_class: ActiveValue::Set(Some(ReservationField::Unknown)),
            second_class: ActiveValue::Set(Some(ReservationField::Unknown)),
            not_every_class: ActiveValue::Set(Some(ReservationField::Unknown)),
            reservations_bicycles: ActiveValue::Set(Some(ReservationField::Unknown)),
            sleepers: ActiveValue::Set(Some(ReservationField::NotApplicable)),
            reservations_vehicles: ActiveValue::Set(Some(ReservationField::NotApplicable)),
            wheelchairs: ActiveValue::Set(Some(ReservationField::NotMandatory)),
            international: ActiveValue::Set(Some(ReservationField::NotMandatory)),
            has_catering: ActiveValue::Set(true),
            bar: ActiveValue::Set(Some(false)),
            bistro: ActiveValue::Set(Some(false)),
            restaurant: ActiveValue::Set(Some(false)),
            has_toilets: ActiveValue::Set(true),
            has_luggage: ActiveValue::Set(true),
            limited_bicycles: ActiveValue::Set(Some(false)),
            has_families: ActiveValue::Set(false),
            has_passenger_communications: ActiveValue::Set(true),
            telephone: ActiveValue::Set(Some(false)),
            has_assistance: ActiveValue::Set(false),
            has_passenger_information: ActiveValue::Set(false),
            has_fares: ActiveValue::Set(true),
            non_standard_fares: ActiveValue::Set(Some(false)),
            special_ticket_required: ActiveValue::Set(Some(false)),
            supplement_required: ActiveValue::Set(Some(false)),
            supplement_required_part: ActiveValue::Set(Some(false)),
            attraction_visitors_only: ActiveValue::Set(Some(false)),
            supplement_required_rail_passes: ActiveValue::Set(Some(false)),
            national_rights_apply: ActiveValue::Set(Some(true)),
            has_occupancy: ActiveValue::Set(true),
            very_busy: ActiveValue::Set(Some(false)),
            has_boarding_information: ActiveValue::Set(true),
            international_border_check: ActiveValue::Set(Some(false)),
            accommodation: ActiveHasMany::Append(
                vec![
                    accommodation_types::ActiveModelEx {
                        class: ActiveValue::Set(AccommodationClass::First),
                        seating: ActiveValue::Set(Some(true)),
                        sleeper: ActiveValue::Set(Some(false)),
                        couchette: ActiveValue::Set(Some(false)),
                        quiet_zone: ActiveValue::Set(Some(false)),
                        ..Default::default()
                    },
                    accommodation_types::ActiveModelEx {
                        class: ActiveValue::Set(AccommodationClass::Second),
                        seating: ActiveValue::Set(Some(true)),
                        sleeper: ActiveValue::Set(Some(false)),
                        couchette: ActiveValue::Set(Some(false)),
                        quiet_zone: ActiveValue::Set(Some(false)),
                        ..Default::default()
                    },
                ]
            ),
            ..Default::default()
        };

        if from_stop_index == 1 {
            train_variant.variable_train = ActiveHasOne::Set(Some(Box::new(variable_train)));
        } else {
            self.changes_en_route.insert(from_stop_index, variable_train);
        }
        Ok(())
    }

    async fn read_timetbls_footnote(
        &mut self,
        line: &str,
        _namespace: &str,
        i: usize,
        valid_begin: &NaiveDateTime,
        valid_end: &NaiveDateTime,
    ) -> Result<(), Error> {
        let train_variant = match self.current_train_variant.as_mut() {
            None => return Err(IffError {
                error_type: IffErrorType::MissingTripNumberRecord,
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into()),
            Some(train_variant) => train_variant,
        };

        let fields: Vec<String> = line.split(",").map(|x| x.to_string()).collect();
        // footnote,first_stop,last_stop
        // first_stop and last_stop don't appear to be used here
        if fields.len() != 3 {
            return Err(IffError {
                error_type: IffErrorType::NotEnoughFields,
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into());
        }

        // For validation later
        self.cur_footnote_id = fields[0].trim().to_string();

        let mut train_validity_periods = vec![];

        let day_bits = match self.day_bits_by_id.get(fields[0].trim()) {
            Some(x) => x,
            None => return Err(IffError {
                error_type: IffErrorType::FootnoteNotFound(fields[0].trim().to_string()),
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into()),
        };

        // We've actually already done this check but whatever
        let days = (valid_end.date() - valid_begin.date()).num_days() + 1;
        if day_bits.len() != usize::try_from(days).unwrap() {
            return Err(
                IffError {
                    error_type: IffErrorType::DayBitsDontMatchPeriodLength(days),
                    filename: "timetbls_new.dat".to_string(),
                    line: i,
                }.into()
            );
        }

        let first_day_offset = usize::try_from(
            valid_begin.date().weekday().num_days_from_monday()
        ).unwrap();

        let first_full_week_index = (7 - first_day_offset) % 7;

        // First extract the weeks into vectors
        let mut weeks = vec![];
        let mut idx: usize = 0;

        if first_full_week_index > 0 {
            let mut first_week = [None; 7];
            while idx < first_full_week_index && idx < day_bits.len() {
                first_week[idx + first_day_offset] = Some(day_bits.as_bytes()[idx] == b'1');
                idx += 1;
            }
            weeks.push(first_week);
        }

        while idx < day_bits.len() {
            let mut week = [None; 7];
            loop {
                week[(idx - first_full_week_index) % 7] = Some(
                    day_bits.as_bytes()[idx] == b'1'
                );
                idx += 1;
                if (idx - first_full_week_index) % 7 == 0 || idx >= day_bits.len() {
                    break
                }
            }
            weeks.push(week);
        }

        // now we have a vector of each week, we can check each's consistency against the last
        let mut maybe_days_of_week = [None; 7];
        let mut cur_start = valid_begin.date().clone();
        let mut cur_end = cur_start.clone();
        for week in weeks {
            maybe_days_of_week = match try_overlay_days(
                maybe_days_of_week.clone(), week.clone()
            ) {
                Some(new_days_of_week) => {
                    cur_end = cur_end + Days::new(
                        u64::try_from(week.iter().filter(|x| { x.is_some() }).count()).unwrap()
                    );
                    new_days_of_week
                },
                None => {
                    let train_validity_period = train_validity_period::ActiveModelEx {
                        valid_begin:
                            ActiveValue::Set(cur_start.and_hms_opt(0, 0, 0).unwrap()),
                        valid_end: ActiveValue::Set(
                                (cur_end - Days::new(1)).and_hms_opt(0, 0, 0).unwrap()
                            ),
                        timezone: ActiveValue::Set(Amsterdam.name().to_string()),
                        monday: ActiveValue::Set(maybe_days_of_week[0].unwrap_or(false)),
                        tuesday: ActiveValue::Set(maybe_days_of_week[1].unwrap_or(false)),
                        wednesday: ActiveValue::Set(maybe_days_of_week[2].unwrap_or(false)),
                        thursday: ActiveValue::Set(maybe_days_of_week[3].unwrap_or(false)),
                        friday: ActiveValue::Set(maybe_days_of_week[4].unwrap_or(false)),
                        saturday: ActiveValue::Set(maybe_days_of_week[5].unwrap_or(false)),
                        sunday: ActiveValue::Set(maybe_days_of_week[6].unwrap_or(false)),
                        ..Default::default()
                    };
                    if maybe_days_of_week.iter().filter(
                        |x| { x.unwrap_or(false) }
                    ).count() != 0 {
                        train_validity_periods.push(train_validity_period);
                    }
                    cur_start = cur_end.clone();
                    cur_end = cur_end + Days::new(
                        u64::try_from(week.iter().filter(|x| { x.is_some() }).count()).unwrap()
                    );
                    week
                },
            };
        };

        train_variant.validity = ActiveHasMany::Append(train_validity_periods);
        Ok(())
    }

    async fn read_timetbls_mode(
        &mut self,
        line: &str,
        _namespace: &str,
        i: usize,
    ) -> Result<(), Error> {
        let train_variant = match self.current_train_variant.as_mut() {
            None => return Err(IffError {
                error_type: IffErrorType::MissingTripNumberRecord,
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into()),
            Some(train_variant) => train_variant,
        };

        let fields: Vec<String> = line.split(",").map(|x| x.to_string()).collect();
        // trnsmode_id,first_stop,last_stop
        if fields.len() != 3 {
            return Err(IffError {
                error_type: IffErrorType::NotEnoughFields,
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into());
        }

        // As these are a mix of brands and modes, we define a best-effort mapping to modes here
        let mode = match fields[0].as_str() {
            "B   " => TrainType::ReplacementBus,
            "BBE " => TrainType::OnDemandReplacementBus,
            "BBS " => TrainType::OnDemandReplacementBus,
            "BMI " => TrainType::StoppingReplacementBus,
            "BMS " => TrainType::ExpressReplacementBus,
            "BSN " => TrainType::ExpressReplacementBus,
            "BST " => TrainType::StoppingReplacementBus,
            "BTA " => TrainType::ReplacementTaxi,
            "BTS " => TrainType::ExpressReplacementBus,
            "BUS " => TrainType::StoppingReplacementBus,
            "DSN " => TrainType::ExpressReplacementBus,
            "DST " => TrainType::StoppingReplacementBus,
            "ECC " => TrainType::InternationalPassenger,
            "ECD " => TrainType::InternationalPassenger,
            "ES  " => TrainType::InternationalSleeperPassenger,
            "EST " => TrainType::InternationalHighSpeedPassenger,
            "GV  " => TrainType::InternationalPassenger,
            "HSN " => TrainType::InternationalHighSpeedPassenger,
            "IC  " => TrainType::IntercityPassenger,
            "ICD " => TrainType::HighSpeedPassenger,
            "ICE " => TrainType::InternationalHighSpeedPassenger,
            "INT " => TrainType::InternationalPassenger,
            "M   " => TrainType::Metro,
            "MBS " => TrainType::ExpressReplacementBus,
            "MST " => TrainType::StoppingReplacementBus,
            "MTR " => TrainType::ReplacementMetro,
            "MTS " => TrainType::ReplacementMetro,
            "NJ  " => TrainType::InternationalSleeperPassenger,
            "NSM " => TrainType::ReplacementMetro,
            "NSS " => TrainType::ExpressReplacementBus,
            "NST " => TrainType::ReplacementTram,
            "NT  " => TrainType::NightPassenger,
            "OSB " => TrainType::ExpressReplacementBus,
            "S   " => TrainType::ExpressPassenger,
            "SPC " => TrainType::SpecialPassenger,
            "SPR " => TrainType::LocalPassenger,
            "ST  " => TrainType::LocalPassenger,
            "STO " => TrainType::AccessibleBus,
            "THA " => TrainType::InternationalHighSpeedPassenger,
            _ => return Err(IffError {
                error_type: IffErrorType::UnknownMode(fields[0].to_string()),
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into()),
        };

        // We determine if we are setting this now as the first variable_train, or storing it as a
        // change en route
        let from_stop_index: usize = match fields[1].trim().parse() {
            Ok(x) => x,
            Err(_) => return Err(IffError {
                error_type: IffErrorType::InvalidStopIndex(fields[1].trim().to_string()),
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into()),
        };
        let to_stop_index: usize = match fields[2].trim().parse() {
            Ok(x) => x,
            Err(_) => return Err(IffError {
                error_type: IffErrorType::InvalidStopIndex(fields[2].trim().to_string()),
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into()),
        };

        {
            let variable_train = if from_stop_index == 1 {
                train_variant.variable_train.as_mut().unwrap()
            } else {
                match self.changes_en_route.get_mut(&from_stop_index) {
                    Some(x) => x,
                    None => {
                        let mut found = false;
                        for stop_index in (2..from_stop_index).rev() {
                            match self.changes_en_route.get(&stop_index) {
                                None => (),
                                Some(old_change) => {
                                    found = true;
                                    self.changes_en_route.insert(
                                        from_stop_index, old_change.clone()
                                    );
                                },
                            };
                        }
                        if !found {
                            self.changes_en_route.insert(
                                from_stop_index,
                                train_variant.variable_train.as_ref().unwrap().clone(),
                            );
                        }
                        self.changes_en_route.get_mut(&from_stop_index).unwrap()
                    },
                }
            };
            variable_train.brand_id = ActiveValue::Set(Some(fields[0].trim().to_string()));
            variable_train.train_type = ActiveValue::Set(mode.clone());
        }

        // Now we backfill any other changes during this period
        for stop_index in from_stop_index..to_stop_index + 1 {
            match self.changes_en_route.get_mut(&stop_index) {
                Some(change_en_route) => {
                    change_en_route.brand_id = ActiveValue::Set(Some(fields[0].trim().to_string()));
                    change_en_route.train_type = ActiveValue::Set(mode.clone());
                },
                None => (),
            }
        }

        Ok(())
    }

    async fn read_timetbls_attribute(
        &mut self,
        line: &str,
        _namespace: &str,
        i: usize,
    ) -> Result<(), Error> {
        let train_variant = match self.current_train_variant.as_mut() {
            None => return Err(IffError {
                error_type: IffErrorType::MissingTripNumberRecord,
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into()),
            Some(train_variant) => train_variant,
        };

        let fields: Vec<String> = line.split(",").map(|x| x.to_string()).collect();
        // attribute,first_stop,last_stop,footnote

        if fields.len() != 4 {
            return Err(IffError {
                error_type: IffErrorType::NotEnoughFields,
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into());
        }

        if fields[3].trim() != "00000" && fields[3].trim() != self.cur_footnote_id {
            return Err(IffError {
                error_type: IffErrorType::UnexpectedFootnote(fields[3].trim().to_string()),
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into());
        }

        // We determine if we are setting this now as the first variable_train, or storing it as a
        // change en route
        let from_stop_index: usize = match fields[1].trim().parse() {
            Ok(x) => x,
            Err(_) => return Err(IffError {
                error_type: IffErrorType::InvalidStopIndex(fields[1].trim().to_string()),
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into()),
        };
        let to_stop_index: usize = match fields[2].trim().parse() {
            Ok(x) => x,
            Err(_) => return Err(IffError {
                error_type: IffErrorType::InvalidStopIndex(fields[2].trim().to_string()),
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into()),
        };

        {
            let variable_train = if from_stop_index == 1 {
                train_variant.variable_train.as_mut().unwrap()
            } else {
                match self.changes_en_route.get_mut(&from_stop_index) {
                    Some(x) => x,
                    None => {
                        let mut found = false;
                        for stop_index in (2..from_stop_index).rev() {
                            match self.changes_en_route.get(&stop_index) {
                                None => (),
                                Some(old_change) => {
                                    found = true;
                                    self.changes_en_route.insert(
                                        from_stop_index, old_change.clone()
                                    );
                                },
                            };
                        }
                        if !found {
                            self.changes_en_route.insert(
                                from_stop_index,
                                train_variant.variable_train.as_ref().unwrap().clone(),
                            );
                        }
                        self.changes_en_route.get_mut(&from_stop_index).unwrap()
                    },
                }
            };

            populate_variable_train_attribute(
                &fields[0],
                variable_train,
                &mut self.no_boarding_stops,
                &mut self.no_alighting_stops,
                from_stop_index,
                to_stop_index,
                i,
            )?;
        }

        // Now we backfill any other changes during this period
        for stop_index in from_stop_index..to_stop_index + 1 {
            match self.changes_en_route.get_mut(&stop_index) {
                Some(change_en_route) => {
                    populate_variable_train_attribute(
                        &fields[0],
                        &mut *change_en_route,
                        &mut self.no_boarding_stops,
                        &mut self.no_alighting_stops,
                        from_stop_index,
                        to_stop_index,
                        i,
                    )?;
                },
                None => (),
            }
        }

        Ok(())
    }

    async fn read_timetbls_begin(
        &mut self,
        line: &str,
        namespace: &str,
        i: usize,
        stop_index: usize,
        location_index: usize,
    ) -> Result<(), Error> {
        let train_variant = match self.current_train_variant.as_mut() {
            None => return Err(IffError {
                error_type: IffErrorType::MissingTripNumberRecord,
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into()),
            Some(train_variant) => train_variant,
        };

        let fields: Vec<String> = line.split(",").map(|x| x.to_string()).collect();
        // station,departure_time
        if fields.len() != 2 {
            return Err(IffError {
                error_type: IffErrorType::NotEnoughFields,
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into());
        }

        let (departure_day, departure_time) = read_time(&fields[1], "timetbls_new.dat", i)?;

        // We know there won't be any change en route for the first stop
        // However I guess it's possible there could be a no-boarding prohibition on the first stop
        // so let's check for that.

        let (public_dep_day, public_dep) = if self.no_boarding_stops.contains(&stop_index) {
            (None, None)
        } else {
            (Some(departure_day), Some(departure_time.clone()))
        };

        train_variant.route = ActiveHasMany::Append(vec![
            train_location::ActiveModelEx {
                index: ActiveValue::Set(location_index.try_into().unwrap()),
                location_id: ActiveValue::Set(fields[0].trim().to_string()),
                namespace: ActiveValue::Set(namespace.to_string()),
                id_suffix: ActiveValue::Set(Some(stop_index.to_string())),
                working_dep: ActiveValue::Set(Some(departure_time)),
                working_dep_day: ActiveValue::Set(Some(departure_day)),
                public_dep: ActiveValue::Set(public_dep),
                public_dep_day: ActiveValue::Set(public_dep_day),
                detach: ActiveValue::Set(false),
                attach: ActiveValue::Set(false),
                other_trains_pass: ActiveValue::Set(false),
                attach_or_detach_assisting_loco: ActiveValue::Set(false),
                x_on_arrival: ActiveValue::Set(false),
                banking_loco: ActiveValue::Set(false),
                crew_change: ActiveValue::Set(false),
                set_down_only: ActiveValue::Set(false),
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
                unadvertised_stop: ActiveValue::Set(public_dep.is_none()),
                operational_stop: ActiveValue::Set(false),
                train_locomotive_on_rear: ActiveValue::Set(false),
                propelling: ActiveValue::Set(false),
                request_pick_up: ActiveValue::Set(false),
                request_set_down: ActiveValue::Set(false),
                reversing_move: ActiveValue::Set(false),
                run_round: ActiveValue::Set(false),
                staff_stop: ActiveValue::Set(false),
                normal_passenger_stop: ActiveValue::Set(false),
                train_begins: ActiveValue::Set(true),
                train_finishes: ActiveValue::Set(false),
                tops_reporting: ActiveValue::Set(false),
                token_etc: ActiveValue::Set(false),
                pick_up_only: ActiveValue::Set(public_dep.is_some()),
                watering_stock: ActiveValue::Set(false),
                cross_at_passing_point: ActiveValue::Set(false),
                request_pick_up_by_telephone: ActiveValue::Set(false),
                request_set_down_by_telephone: ActiveValue::Set(false),
                times_approximate: ActiveValue::Set(false),
                ..Default::default()
            }
        ]);

        Ok(())
    }

    async fn read_timetbls_pass(
        &mut self,
        line: &str,
        namespace: &str,
        i: usize,
        location_index: usize,
    ) -> Result<(), Error> {
        let train_variant = match self.current_train_variant.as_mut() {
            None => return Err(IffError {
                error_type: IffErrorType::MissingTripNumberRecord,
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into()),
            Some(train_variant) => train_variant,
        };

        if train_variant.route.as_mut_vec().len() < 1 {
            return Err(IffError {
                error_type: IffErrorType::PassWithoutStop,
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into());
        }

        let last_location = train_variant.route.as_mut_vec().last().unwrap();
        // No passing times are provided in this file, so for now duplicate the previous departure
        // or passing time. There will always be one of these unless we somehow get a pass after a
        // train finish. Later we will go back and update these to be approximately interpolated.
        let working_pass
            = last_location.working_dep.clone().into_option().unwrap_or_else(
                || last_location.working_pass.clone().into_option().unwrap()
            );
        let working_pass_day
            = last_location.working_dep_day.clone().into_option().unwrap_or_else(
                || last_location.working_pass_day.clone().into_option().unwrap()
            );

        train_variant.route.as_mut_vec().push(
            train_location::ActiveModelEx {
                index: ActiveValue::Set(location_index.try_into().unwrap()),
                location_id: ActiveValue::Set(line.trim().to_string()),
                namespace: ActiveValue::Set(namespace.to_string()),
                working_pass: ActiveValue::Set(Some(working_pass)),
                working_pass_day: ActiveValue::Set(Some(working_pass_day)),
                detach: ActiveValue::Set(false),
                attach: ActiveValue::Set(false),
                other_trains_pass: ActiveValue::Set(false),
                attach_or_detach_assisting_loco: ActiveValue::Set(false),
                x_on_arrival: ActiveValue::Set(false),
                banking_loco: ActiveValue::Set(false),
                crew_change: ActiveValue::Set(false),
                set_down_only: ActiveValue::Set(false),
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
                unadvertised_stop: ActiveValue::Set(false),
                operational_stop: ActiveValue::Set(false),
                train_locomotive_on_rear: ActiveValue::Set(false),
                propelling: ActiveValue::Set(false),
                request_pick_up: ActiveValue::Set(false),
                request_set_down: ActiveValue::Set(false),
                reversing_move: ActiveValue::Set(false),
                run_round: ActiveValue::Set(false),
                staff_stop: ActiveValue::Set(false),
                normal_passenger_stop: ActiveValue::Set(false),
                train_begins: ActiveValue::Set(false),
                train_finishes: ActiveValue::Set(false),
                tops_reporting: ActiveValue::Set(false),
                token_etc: ActiveValue::Set(false),
                pick_up_only: ActiveValue::Set(false),
                watering_stock: ActiveValue::Set(false),
                cross_at_passing_point: ActiveValue::Set(false),
                request_pick_up_by_telephone: ActiveValue::Set(false),
                request_set_down_by_telephone: ActiveValue::Set(false),
                times_approximate: ActiveValue::Set(true),
                ..Default::default()
            }
        );

        Ok(())
    }

    async fn read_timetbls_short_stop(
        &mut self,
        line: &str,
        namespace: &str,
        i: usize,
        stop_index: usize,
        location_index: usize,
    ) -> Result<(), Error> {
        let train_variant = match self.current_train_variant.as_mut() {
            None => return Err(IffError {
                error_type: IffErrorType::MissingTripNumberRecord,
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into()),
            Some(train_variant) => train_variant,
        };

        let fields: Vec<String> = line.split(",").map(|x| x.to_string()).collect();
        // station,stop_time
        if fields.len() != 2 {
            return Err(IffError {
                error_type: IffErrorType::NotEnoughFields,
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into());
        }

        let (day, time) = read_time(&fields[1], "timetbls_new.dat", i)?;

        let (public_dep_day, public_dep) = if self.no_boarding_stops.contains(&stop_index) {
            (None, None)
        } else {
            (Some(day), Some(time.clone()))
        };

        let (public_arr_day, public_arr) = if self.no_alighting_stops.contains(&stop_index) {
            (None, None)
        } else {
            (Some(day), Some(time.clone()))
        };

        let change_en_route = match self.changes_en_route.get(&stop_index) {
            Some(x) => Some(Box::new(x.clone())),
            None => None,
        };

        interpolate_pass_times(&mut *train_variant, &time, &day);

        train_variant.route.as_mut_vec().push(
            train_location::ActiveModelEx {
                index: ActiveValue::Set(location_index.try_into().unwrap()),
                location_id: ActiveValue::Set(fields[0].trim().to_string()),
                namespace: ActiveValue::Set(namespace.to_string()),
                working_arr: ActiveValue::Set(Some(time.clone())),
                working_arr_day: ActiveValue::Set(Some(day)),
                public_arr: ActiveValue::Set(public_arr.clone()),
                public_arr_day: ActiveValue::Set(public_arr_day.clone()),
                working_dep: ActiveValue::Set(Some(time.clone())),
                working_dep_day: ActiveValue::Set(Some(day)),
                public_dep: ActiveValue::Set(public_dep.clone()),
                public_dep_day: ActiveValue::Set(public_dep_day.clone()),
                detach: ActiveValue::Set(false),
                attach: ActiveValue::Set(false),
                other_trains_pass: ActiveValue::Set(false),
                attach_or_detach_assisting_loco: ActiveValue::Set(false),
                x_on_arrival: ActiveValue::Set(false),
                banking_loco: ActiveValue::Set(false),
                crew_change: ActiveValue::Set(false),
                set_down_only: ActiveValue::Set(public_arr.is_some() && public_dep.is_none()),
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
                unadvertised_stop: ActiveValue::Set(public_arr.is_none() && public_dep.is_none()),
                operational_stop: ActiveValue::Set(false),
                train_locomotive_on_rear: ActiveValue::Set(false),
                propelling: ActiveValue::Set(false),
                request_pick_up: ActiveValue::Set(false),
                request_set_down: ActiveValue::Set(false),
                reversing_move: ActiveValue::Set(false),
                run_round: ActiveValue::Set(false),
                staff_stop: ActiveValue::Set(false),
                normal_passenger_stop:
                    ActiveValue::Set(public_arr.is_some() && public_dep.is_some()),
                train_begins: ActiveValue::Set(false),
                train_finishes: ActiveValue::Set(false),
                tops_reporting: ActiveValue::Set(false),
                token_etc: ActiveValue::Set(false),
                pick_up_only: ActiveValue::Set(public_arr.is_none() && public_dep.is_some()),
                watering_stock: ActiveValue::Set(false),
                cross_at_passing_point: ActiveValue::Set(false),
                request_pick_up_by_telephone: ActiveValue::Set(false),
                request_set_down_by_telephone: ActiveValue::Set(false),
                times_approximate: ActiveValue::Set(false),
                change_en_route: ActiveHasOne::Set(change_en_route),
                ..Default::default()
            }
        );

        Ok(())
    }

    async fn read_timetbls_stop(
        &mut self,
        line: &str,
        namespace: &str,
        i: usize,
        stop_index: usize,
        location_index: usize,
    ) -> Result<(), Error> {
        let train_variant = match self.current_train_variant.as_mut() {
            None => return Err(IffError {
                error_type: IffErrorType::MissingTripNumberRecord,
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into()),
            Some(train_variant) => train_variant,
        };

        let fields: Vec<String> = line.split(",").map(|x| x.to_string()).collect();
        // station,arr_time,dep_time
        if fields.len() != 3 {
            return Err(IffError {
                error_type: IffErrorType::NotEnoughFields,
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into());
        }

        let (arr_day, arr_time) = read_time(&fields[1], "timetbls_new.dat", i)?;
        let (dep_day, dep_time) = read_time(&fields[2], "timetbls_new.dat", i)?;

        let (public_dep_day, public_dep) = if self.no_boarding_stops.contains(&stop_index) {
            (None, None)
        } else {
            (Some(dep_day), Some(dep_time.clone()))
        };

        let (public_arr_day, public_arr) = if self.no_alighting_stops.contains(&stop_index) {
            (None, None)
        } else {
            (Some(arr_day), Some(arr_time.clone()))
        };

        let change_en_route = match self.changes_en_route.get(&stop_index) {
            Some(x) => Some(Box::new(x.clone())),
            None => None,
        };

        interpolate_pass_times(&mut *train_variant, &arr_time, &arr_day);

        train_variant.route.as_mut_vec().push(
            train_location::ActiveModelEx {
                index: ActiveValue::Set(location_index.try_into().unwrap()),
                location_id: ActiveValue::Set(fields[0].trim().to_string()),
                namespace: ActiveValue::Set(namespace.to_string()),
                working_arr: ActiveValue::Set(Some(arr_time)),
                working_arr_day: ActiveValue::Set(Some(arr_day)),
                public_arr: ActiveValue::Set(public_arr.clone()),
                public_arr_day: ActiveValue::Set(public_arr_day.clone()),
                working_dep: ActiveValue::Set(Some(dep_time)),
                working_dep_day: ActiveValue::Set(Some(dep_day)),
                public_dep: ActiveValue::Set(public_dep.clone()),
                public_dep_day: ActiveValue::Set(public_dep_day.clone()),
                detach: ActiveValue::Set(false),
                attach: ActiveValue::Set(false),
                other_trains_pass: ActiveValue::Set(false),
                attach_or_detach_assisting_loco: ActiveValue::Set(false),
                x_on_arrival: ActiveValue::Set(false),
                banking_loco: ActiveValue::Set(false),
                crew_change: ActiveValue::Set(false),
                set_down_only: ActiveValue::Set(public_arr.is_some() && public_dep.is_none()),
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
                unadvertised_stop: ActiveValue::Set(public_arr.is_none() && public_dep.is_none()),
                operational_stop: ActiveValue::Set(false),
                train_locomotive_on_rear: ActiveValue::Set(false),
                propelling: ActiveValue::Set(false),
                request_pick_up: ActiveValue::Set(false),
                request_set_down: ActiveValue::Set(false),
                reversing_move: ActiveValue::Set(false),
                run_round: ActiveValue::Set(false),
                staff_stop: ActiveValue::Set(false),
                normal_passenger_stop:
                    ActiveValue::Set(public_arr.is_some() && public_dep.is_some()),
                train_begins: ActiveValue::Set(false),
                train_finishes: ActiveValue::Set(false),
                tops_reporting: ActiveValue::Set(false),
                token_etc: ActiveValue::Set(false),
                pick_up_only: ActiveValue::Set(public_arr.is_none() && public_dep.is_some()),
                watering_stock: ActiveValue::Set(false),
                cross_at_passing_point: ActiveValue::Set(false),
                request_pick_up_by_telephone: ActiveValue::Set(false),
                request_set_down_by_telephone: ActiveValue::Set(false),
                times_approximate: ActiveValue::Set(false),
                change_en_route: ActiveHasOne::Set(change_en_route),
                ..Default::default()
            }
        );

        Ok(())
    }

    async fn read_timetbls_end(
        &mut self,
        line: &str,
        namespace: &str,
        i: usize,
        stop_index: usize,
        location_index: usize,
    ) -> Result<(), Error> {
        let train_variant = match self.current_train_variant.as_mut() {
            None => return Err(IffError {
                error_type: IffErrorType::MissingTripNumberRecord,
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into()),
            Some(train_variant) => train_variant,
        };

        let fields: Vec<String> = line.split(",").map(|x| x.to_string()).collect();
        // station,arr_time
        if fields.len() != 2 {
            return Err(IffError {
                error_type: IffErrorType::NotEnoughFields,
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into());
        }

        let (arr_day, arr_time) = read_time(&fields[1], "timetbls_new.dat", i)?;

        let (public_arr_day, public_arr) = if self.no_alighting_stops.contains(&stop_index) {
            (None, None)
        } else {
            (Some(arr_day), Some(arr_time.clone()))
        };

        let change_en_route = match self.changes_en_route.get(&stop_index) {
            Some(x) => Some(Box::new(x.clone())),
            None => None,
        };

        interpolate_pass_times(&mut *train_variant, &arr_time, &arr_day);

        train_variant.route.as_mut_vec().push(
            train_location::ActiveModelEx {
                index: ActiveValue::Set(location_index.try_into().unwrap()),
                location_id: ActiveValue::Set(fields[0].trim().to_string()),
                namespace: ActiveValue::Set(namespace.to_string()),
                working_arr: ActiveValue::Set(Some(arr_time)),
                working_arr_day: ActiveValue::Set(Some(arr_day)),
                public_arr: ActiveValue::Set(public_arr.clone()),
                public_arr_day: ActiveValue::Set(public_arr_day.clone()),
                detach: ActiveValue::Set(false),
                attach: ActiveValue::Set(false),
                other_trains_pass: ActiveValue::Set(false),
                attach_or_detach_assisting_loco: ActiveValue::Set(false),
                x_on_arrival: ActiveValue::Set(false),
                banking_loco: ActiveValue::Set(false),
                crew_change: ActiveValue::Set(false),
                set_down_only: ActiveValue::Set(public_arr.is_some()),
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
                unadvertised_stop: ActiveValue::Set(public_arr.is_none()),
                operational_stop: ActiveValue::Set(false),
                train_locomotive_on_rear: ActiveValue::Set(false),
                propelling: ActiveValue::Set(false),
                request_pick_up: ActiveValue::Set(false),
                request_set_down: ActiveValue::Set(false),
                reversing_move: ActiveValue::Set(false),
                run_round: ActiveValue::Set(false),
                staff_stop: ActiveValue::Set(false),
                normal_passenger_stop: ActiveValue::Set(false),
                train_begins: ActiveValue::Set(false),
                train_finishes: ActiveValue::Set(true),
                tops_reporting: ActiveValue::Set(false),
                token_etc: ActiveValue::Set(false),
                pick_up_only: ActiveValue::Set(false),
                watering_stock: ActiveValue::Set(false),
                cross_at_passing_point: ActiveValue::Set(false),
                request_pick_up_by_telephone: ActiveValue::Set(false),
                request_set_down_by_telephone: ActiveValue::Set(false),
                times_approximate: ActiveValue::Set(false),
                change_en_route: ActiveHasOne::Set(change_en_route),
                ..Default::default()
            }
        );

        Ok(())
    }

    async fn read_timetbls_track(
        &mut self,
        line: &str,
        _namespace: &str,
        i: usize,
    ) -> Result<(), Error> {
        let train_variant = match self.current_train_variant.as_mut() {
            None => return Err(IffError {
                error_type: IffErrorType::MissingTripNumberRecord,
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into()),
            Some(train_variant) => train_variant,
        };

        let fields: Vec<String> = line.split(",").map(|x| x.to_string()).collect();
        // arrival_track,departure_track,footnote

        if fields.len() != 3 {
            return Err(IffError {
                error_type: IffErrorType::NotEnoughFields,
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into());
        }

        // Believe footnote is not likely to be used here, but we can check it for sanity
        if fields[2].trim() != "00000" && fields[2].trim() != self.cur_footnote_id {
            return Err(IffError {
                error_type: IffErrorType::UnexpectedFootnote(fields[2].trim().to_string()),
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into());
        }

        if train_variant.route.as_mut_vec().len() < 1 {
            return Err(IffError {
                error_type: IffErrorType::TrackWithoutLocation,
                filename: "timetbls_new.dat".to_string(),
                line: i,
            }.into());
        }

        let last_location = train_variant.route.as_mut_vec().last_mut().unwrap();

        last_location.arr_platform = ActiveValue::Set(Some(fields[0].trim().to_string()));
        last_location.platform = ActiveValue::Set(Some(fields[1].trim().to_string()));

        Ok(())
    }

    async fn read_timetbls_finish(
        &mut self, transaction: &DatabaseTransaction
    ) -> Result<usize, Error> {
        let mut train_count: usize = 0;
        match &self.current_train_variant {
            None => (),
            Some(train_variant) => {
                self.read_timetbls_write_train(train_variant, transaction).await?;

                train_count += 1;
            },
        };

        // Reset the state ready for the next import
        self.timezone_by_id.clear();
        self.day_bits_by_id.clear();
        self.current_train_variant = None;
        self.changes_en_route.clear();
        self.no_boarding_stops.clear();
        self.no_alighting_stops.clear();
        // This is only for validation so isn't too important that it's super rigorous
        self.cur_footnote_id = "".to_string();

        Ok(train_count)
    }

    async fn read_timetbls_new(
        &mut self,
        mut lines: LegacyLines<impl AsyncBufRead + Unpin>,
        namespace: &str,
        valid_begin: &NaiveDateTime,
        valid_end: &NaiveDateTime,
        transaction: &DatabaseTransaction,
    ) -> Result<(), Error> {
        let mut i: usize = 0;
        let mut train_count = 0;
        let mut cur_stop = 1;
        let mut cur_location = 0;
        println!("[{}] Loading trains...", namespace);
        while let Some(line) = lines.next_line().await? {
            i += 1;
            if line.len() == 0 {
                continue;
            }

            match &line[0..1] {
                // We have already read the header from delivery.dat, so ignore it
                "@" => continue,
                "#" => train_count
                    += self.read_timetbls_trip_number(&line[1..], namespace, i, transaction).await?,
                "%" => self.read_timetbls_operator(&line[1..], namespace, i).await?,
                "-" =>
                   self.read_timetbls_footnote(&line[1..], namespace, i, valid_begin, valid_end)
                   .await?,
                "&" => self.read_timetbls_mode(&line[1..], namespace, i).await?,
                "*" => self.read_timetbls_attribute(&line[1..], namespace, i).await?,
                ">" => {
                    self.read_timetbls_begin(
                        &line[1..], namespace, i, cur_stop, cur_location
                    ).await?;
                    cur_stop += 1;
                    cur_location += 1;
                },
                "?" => self.read_timetbls_track(&line[1..], namespace, i).await?,
                ";" => {
                    self.read_timetbls_pass(&line[1..], namespace, i, cur_location).await?;
                    cur_location += 1;
                },
                "." => {
                    self.read_timetbls_short_stop(
                        &line[1..], namespace, i, cur_stop, cur_location
                    ).await?;
                    cur_stop += 1;
                    cur_location += 1;
                },
                "+" => {
                    self.read_timetbls_stop(
                        &line[1..], namespace, i, cur_stop, cur_location
                    ).await?;
                    cur_stop += 1;
                    cur_location += 1;
                },
                "<" => {
                    self.read_timetbls_end(&line[1..], namespace, i, cur_stop, cur_location).await?;
                    cur_stop += 1;
                    cur_location += 1;
                },
                _ => return Err(IffError {
                    error_type: IffErrorType::UnexpectedRecordType(line[0..1].to_string()),
                    filename: "timetbls_new.dat".to_string(),
                    line: i,
                }.into()),
            }
        }
        train_count += self.read_timetbls_finish(transaction).await?;
        println!("[{}] Persisted {} trains", namespace, train_count);
        Ok(())
    }
}

#[async_trait]
impl SlowMultiFileImporter for IffImporter {
    async fn overlay(
        &mut self,
        mut readers: HashMap<String, Box<dyn AsyncBufRead + Unpin + Send>>,
        schedule: &schedule::ModelEx,
        transaction: &DatabaseTransaction,
    ) -> Result<(), Error> {
        let (valid_begin, valid_end) = {
            let delivery = match readers.get_mut("delivery.dat") {
                Some(x) => x,
                None => return Err(IffError {
                    error_type: IffErrorType::MissingFile,
                    filename: "delivery.dat".to_string(),
                    line: 0,
                }.into()),
            };
            self.read_delivery(
                LegacyLines::new(delivery, encoding_rs::WINDOWS_1252),
                schedule,
                transaction,
            ).await?
        };

        {
            let company = match readers.get_mut("company.dat") {
                Some(x) => x,
                None => return Err(IffError {
                    error_type: IffErrorType::MissingFile,
                    filename: "company.dat".to_string(),
                    line: 0,
                }.into()),
            };
            self.read_company(
                LegacyLines::new(company, encoding_rs::WINDOWS_1252),
                &schedule.namespace,
                transaction,
            ).await?;
        }

        {
            let trnsmode = match readers.get_mut("trnsmode.dat") {
                Some(x) => x,
                None => return Err(IffError {
                    error_type: IffErrorType::MissingFile,
                    filename: "trnsmode.dat".to_string(),
                    line: 0,
                }.into()),
            };
            self.read_trnsmode(
                LegacyLines::new(trnsmode, encoding_rs::WINDOWS_1252),
                &schedule.namespace,
                transaction,
            ).await?;
        }

        {
            let timezone = match readers.get_mut("timezone.dat") {
                Some(x) => x,
                None => return Err(IffError {
                    error_type: IffErrorType::MissingFile,
                    filename: "timezone.dat".to_string(),
                    line: 0,
                }.into()),
            };
            self.read_timezone(
                LegacyLines::new(timezone, encoding_rs::WINDOWS_1252),
                &schedule.namespace,
            ).await?;
        }

        {
            let stations = match readers.get_mut("stations.dat") {
                Some(x) => x,
                None => return Err(IffError {
                    error_type: IffErrorType::MissingFile,
                    filename: "stations.dat".to_string(),
                    line: 0,
                }.into()),
            };
            self.read_stations(
                LegacyLines::new(stations, encoding_rs::WINDOWS_1252),
                &schedule.namespace,
                transaction,
            ).await?;
        }

        {
            let footnote = match readers.get_mut("footnote.dat") {
                Some(x) => x,
                None => return Err(IffError {
                    error_type: IffErrorType::MissingFile,
                    filename: "footnote.dat".to_string(),
                    line: 0,
                }.into()),
            };
            self.read_footnote(
                LegacyLines::new(footnote, encoding_rs::WINDOWS_1252),
                &schedule.namespace,
                valid_begin.clone(),
                valid_end.clone(),
            ).await?;
        }

        {
            let timetbls_new = match readers.get_mut("timetbls_new.dat") {
                Some(x) => x,
                None => return Err(IffError {
                    error_type: IffErrorType::MissingFile,
                    filename: "timetbls_new.dat".to_string(),
                    line: 0,
                }.into()),
            };
            self.read_timetbls_new(
                LegacyLines::new(timetbls_new, encoding_rs::WINDOWS_1252),
                &schedule.namespace,
                &valid_begin,
                &valid_end,
                transaction,
            ).await?;
        }

        println!("[{}] Successfully loaded trains from NS", schedule.namespace);

        Ok(())
    }
}

pub struct LegacyLines<R> {
    reader: R,
    decoder: Decoder,
    decoded: String,
    eof: bool,
}

impl<R: AsyncBufRead + Unpin> LegacyLines<R> {
    pub fn new(reader: R, encoding: &'static Encoding) -> Self {
        Self {
            reader,
            decoder: encoding.new_decoder(),
            decoded: String::new(),
            eof: false,
        }
    }

    pub async fn next_line(&mut self) -> Result<Option<String>, IoError> {
        loop {
            if let Some(pos) = self.decoded.find('\n') {
                let mut line = self.decoded.drain(..=pos).collect::<String>();
                line.pop(); // Remove LF.
                if line.ends_with('\r') {
                    line.pop();
                }
                return Ok(Some(line));
            }

            if self.eof {
                if self.decoded.is_empty() {
                    return Ok(None);
                }

                return Ok(Some(std::mem::take(&mut self.decoded)));
            }

            let buf = self.reader.fill_buf().await?;

            if buf.is_empty() {
                let capacity = self
                    .decoder
                    .max_utf8_buffer_length(buf.len())
                    .unwrap_or(buf.len() * 3);
                let mut output = String::with_capacity(capacity);
                let (_result, _read, _written)
                    = self.decoder.decode_to_string(buf, &mut output, true);

                self.decoded.push_str(&output);
                self.eof = true;
                continue;
            }

            let capacity = self
                .decoder
                .max_utf8_buffer_length(buf.len())
                .unwrap_or(buf.len() * 3);
            let mut output = String::with_capacity(capacity);

            let (result, read, _written) =
                self.decoder.decode_to_string(buf, &mut output, false);

            self.decoded.push_str(&output);

            self.reader.consume(read);

            if result == encoding_rs::CoderResult::OutputFull {
                continue;
            }
        }
    }
}
