use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, ParseError, TimeZone, Utc};
use chrono::naive::Days;
use chrono::offset::LocalResult;
use chrono_tz::Tz;

use crate::error::Error;
use crate::schedule::{
    association_node, AssociationType, train, train_location, train_operator, TrainSource,
    train_validity_period, train_variant
};
use crate::schedule_manager::ScheduleManager;

use rocket::{get, routes, State};
use rocket::request::FromParam;

use rocket_tera::{context, Template};

use sea_orm::prelude::{BelongsTo, HasMany};

use serde::Serialize;

use tera;

use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops::{Add, Sub};
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug)]
pub struct WebUiError {
    what: String,
}

impl fmt::Display for WebUiError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error in web UI: {}", self.what)
    }
}

fn hex_colour(value: i64, _: tera::Kwargs, _: &tera::State) -> String {
    format!("{value:06x}")
}

#[get("/")]
async fn index(schedule_manager: &State<Arc<ScheduleManager>>) -> Option<Template> {
    let namespaces = {
        let schedules = schedule_manager.get_all_schedules().await.ok()?;
        let mut map = HashMap::new();
        for schedule in schedules {
            map.insert(schedule.namespace.clone(), schedule.description.clone());
        }
        map
    };

    let context = context! {
        namespaces,
    };

    Some(Template::render("index.tera.html", &context))
}

pub struct NaiveDateRocket(NaiveDate);

impl<'a> FromParam<'a> for NaiveDateRocket {
    type Error = ParseError;

    fn from_param(param: &'a str) -> Result<Self, Self::Error> {
        match NaiveDate::parse_from_str(&param, "%Y-%m-%d") {
            Ok(date) => Ok(NaiveDateRocket(date)),
            Err(e) => Err(e),
        }
    }
}

pub struct NaiveTimeRocket(NaiveTime);

impl<'a> FromParam<'a> for NaiveTimeRocket {
    type Error = ParseError;

    fn from_param(param: &'a str) -> Result<Self, Self::Error> {
        match NaiveTime::parse_from_str(&param, "%H:%M") {
            Ok(date) => Ok(NaiveTimeRocket(date)),
            Err(e) => Err(e),
        }
    }
}

fn convert_tz(
    date: &NaiveDate,
    day_diff: &Option<u8>,
    time: &Option<NaiveTime>,
    time_tz: &Option<String>,
    target_tz: &String,
) -> Result<Option<(NaiveTime, u8)>, Error> {
    let target_tz = match Tz::from_str(target_tz) {
        Ok(x) => x,
        Err(_) => return Err(Error::WebUiError(WebUiError {
            what: "Invalid target timezone".to_string(),
        })),
    };
    let (time, day_diff) = match time {
        None => return Ok(None),
        Some(x) => (x, day_diff.unwrap()),
    };
    let time_tz = match time_tz {
        None => return Ok(Some((time.clone(), day_diff))),
        Some(x) => x,
    };
    let time_tz = match Tz::from_str(time_tz) {
        Ok(x) => x,
        Err(_) => return Err(Error::WebUiError(WebUiError {
            what: "Invalid source timezone".to_string(),
        })),
    };
    let date_time = date.add(Days::new(day_diff.into())).and_time(*time);

    let date_time_with_tz = match time_tz.from_local_datetime(&date_time) {
        LocalResult::None => {
            return Err(Error::WebUiError(WebUiError {
                what: "Invalid datetime".to_string(),
            }))
        }
        LocalResult::Single(x) => x,
        LocalResult::Ambiguous(x, _) => x, // TODO?
    };

    let output_time_tz = date_time_with_tz.with_timezone(&target_tz);

    Ok(Some((
        output_time_tz.time(),
        date_time_with_tz
        .date_naive()
        .signed_duration_since(output_time_tz.date_naive())
        .num_days()
        .try_into()
        .unwrap()
    )))
}

fn get_train_instance(train: &train::ModelEx, date: NaiveDate)
    -> (Option<train_variant::ModelEx>, bool, bool) {
    // let's make life easy and find the right train
    let mut final_train = None;
    let mut cancelled = false;
    let mut modified = false;
    for train_variant in &train.train_variants {
        for validity in &train_variant.validity {
            if validity.valid_begin.date() <= date
                && validity.valid_end.date() >= date
                && train_validity_period::DaysOfWeek::get_from_model(validity)
                .get_by_weekday(date.weekday())
            {
                cancelled = false;
                modified = false;
                'replacement: for replacement in &train_variant.replacements {
                    for validity in &replacement.validity {
                        if validity.valid_begin.date() <= date
                            && validity.valid_end.date() >= date
                            && train_validity_period::DaysOfWeek::get_from_model(validity)
                            .get_by_weekday(date.weekday())
                        {
                            final_train = Some(replacement.clone());
                            modified = true;
                            break 'replacement;
                        }
                    }
                }
                if final_train.is_none() {
                    final_train = Some(train_variant.clone());
                }
                for cancellation in &train_variant.cancellations {
                    for validity in &cancellation.validity {
                        if validity.valid_begin.date() <= date
                            && validity.valid_end.date() >= date
                            && train_validity_period::DaysOfWeek::get_from_model(validity)
                            .get_by_weekday(date.weekday())
                        {
                            cancelled = true;
                        }
                    }
                }
            }
        }
    }

    return (final_train, cancelled, modified);
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
enum AssociationCategory {
    Join,
    Divide,
    Next,
    IsJoinedToBy,
    DividesFrom,
    FormsFrom,
    Duplicate,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
struct BasicAssocTrainDetails {
    id: String,
    public_id: Option<String>,
    origin_id: String,
    destination_id: String,
    date: NaiveDate,
    namespace: String,
    is_public: bool,
    category: AssociationCategory,
    name: Option<String>,
    dep_time: NaiveTime,
}

fn get_association(
    assoc: &association_node::ModelEx, date: NaiveDate
) -> Option<association_node::ModelEx> {
    let mut final_assoc = None;
    let mut cancelled = false;
    for validity in &assoc.validity {
        if validity.valid_begin.date() <= date
            && validity.valid_end.date() >= date
            && train_validity_period::DaysOfWeek::get_from_model(validity)
            .get_by_weekday(date.weekday())
        {
            cancelled = false;
            'replacement: for replacement in &assoc.replacements {
                for validity in &replacement.validity {
                    if validity.valid_begin.date() <= date
                        && validity.valid_end.date() >= date
                        && train_validity_period::DaysOfWeek::get_from_model(validity)
                        .get_by_weekday(date.weekday())
                    {
                        final_assoc = Some(replacement.clone());
                        break 'replacement;
                    }
                }
            }
            if final_assoc.is_none() {
                final_assoc = Some(assoc.clone());
            }
            for cancellation in &assoc.cancellations {
                for validity in &cancellation.validity {
                    if validity.valid_begin.date() <= date
                        && validity.valid_end.date() >= date
                        && train_validity_period::DaysOfWeek::get_from_model(validity)
                        .get_by_weekday(date.weekday())
                    {
                        cancelled = true;
                    }
                }
            }
        }
    }

    if final_assoc.is_none() || cancelled {
        None
    } else {
        final_assoc
    }
}

fn add_associated_train(
    associations: &mut Vec<(
        String,
        i8,
        bool,
        String,
        Option<String>,
        AssociationCategory,
    )>,
    assoc: &association_node::ModelEx,
    date: NaiveDate,
    location: &String,
    location_suffix: &Option<String>,
) -> () {
    let final_assoc = match get_association(assoc, date) {
        Some(x) => x,
        None => return,
    };

    associations.push((
        final_assoc.other_train_id,
        final_assoc.day_diff,
        final_assoc.for_passengers,
        location.clone(),
        location_suffix.clone(),
        match &assoc.association_type {
            AssociationType::MainDividesToFormOther => AssociationCategory::Divide,
            AssociationType::MainDividesFromOther => AssociationCategory::DividesFrom,
            AssociationType::MainJoinsToOther => AssociationCategory::Join,
            AssociationType::MainIsJoinedToByOther => AssociationCategory::IsJoinedToBy,
            AssociationType::MainBecomesOther => AssociationCategory::Next,
            AssociationType::MainFormsFromOther => AssociationCategory::FormsFrom,
        },
    ));
}

fn add_associated_trains(
    associations: &mut Vec<(
        String,
        i8,
        bool,
        String,
        Option<String>,
        AssociationCategory,
    )>,
    assoc_vec: &Vec<association_node::ModelEx>,
    date: NaiveDate,
    location: &String,
    location_suffix: &Option<String>,
) -> () {
    for assoc in assoc_vec {
        add_associated_train(
            associations,
            &assoc,
            date,
            location,
            location_suffix,
        );
    }
}

#[get("/train/<namespace>/<train_id>/<date>")]
async fn train_on_date(
    namespace: &str,
    train_id: &str,
    date: NaiveDateRocket,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let date = date.0;

    let mut locations_by_namespace = HashMap::new();

    let (train, schedule_desc, duplicate_trains) = {
        let train = schedule_manager.get_train_by_id(train_id, namespace).await.ok()??;
        let schedule = schedule_manager.get_schedule_by_id(namespace).await.ok()?;
        let mut duplicate_trains_out = HashSet::new();
        for train_variant in &train.train_variants {
            for (duplicate_train_id, duplicate_namespace)
                in schedule_manager.get_duplicate_trains(namespace, &train_variant).await.ok()? {
                let duplicate_train = schedule_manager.get_train_by_id(
                    &duplicate_train_id, &duplicate_namespace
                ).await.ok()?;
                let duplicate_train = match duplicate_train {
                    Some(duplicate_train) => duplicate_train,
                    None => continue,
                };
                // TODO there's a bug here around midnight where the timing TZ of the duplicate
                // train differs from the timing TZ of the current train.
                let (duplicate_final_train, _, _) = get_train_instance(&duplicate_train, date);
                match duplicate_final_train {
                    Some(duplicate_train) => {
                        let route = match &duplicate_train.route {
                            HasMany::Loaded(x) => x,
                            HasMany::Unloaded => return None,
                        };
                        locations_by_namespace
                            .entry(duplicate_namespace.clone())
                            .or_insert(HashMap::new())
                            .insert(
                                route.first().unwrap().location_id.clone(),
                                schedule_manager.get_location_by_id(
                                    &route.first().unwrap().location_id, &duplicate_namespace
                                ).await.ok()?
                            );
                        locations_by_namespace
                            .entry(duplicate_namespace.clone())
                            .or_insert(HashMap::new())
                            .insert(
                                route.last().unwrap().location_id.clone(),
                                schedule_manager.get_location_by_id(
                                    &route.last().unwrap().location_id, &duplicate_namespace
                                ).await.ok()?
                            );
                        let variable_train = duplicate_train.variable_train.as_ref()?;
                        duplicate_trains_out.insert(BasicAssocTrainDetails {
                            id: duplicate_train_id.clone(),
                            public_id: variable_train.public_id.clone(),
                            origin_id: route.first().unwrap().location_id.clone(),
                            destination_id: route.last().unwrap().location_id.clone(),
                            date: date,
                            namespace: duplicate_namespace.clone(),
                            is_public: true,
                            category: AssociationCategory::Duplicate,
                            name: variable_train.name.clone(),
                            dep_time: if route[0].public_dep.is_none() {
                                convert_tz(
                                    &date,
                                    &Some(0),
                                    &route[0].working_dep,
                                    &route[0].timing_tz,
                                    &schedule_manager
                                        .get_location_by_id(
                                            &route[0].location_id, &duplicate_namespace
                                        )
                                        .await.ok()?
                                        .unwrap()
                                        .timezone,
                                )
                                .ok()?
                                .unwrap().0
                            } else {
                                convert_tz(
                                    &date,
                                    &Some(0),
                                    &route[0].public_dep,
                                    &route[0].timing_tz,
                                    &schedule_manager
                                        .get_location_by_id(
                                            &route[0].location_id, &duplicate_namespace
                                        )
                                        .await.ok()?
                                        .unwrap()
                                        .timezone,
                                )
                                .ok()?
                                .unwrap().0
                            },
                        });
                    },
                    None => (),
                }
            }
        }
        (
            train.clone(),
            schedule?.description.clone(),
            duplicate_trains_out,
        )
    };

    let train_id = train.id.clone(); // We have a train ID but get it from the schedule just to be
                                     // sure I guess
    let (final_train, cancelled, modified) = get_train_instance(&train, date);

    let mut train = final_train?;
    let mut associations: Vec<(
        String,
        i8,
        bool,
        String,
        Option<String>,
        AssociationCategory,
    )> = Vec::new();
    for location in &train.route {
        {
            locations_by_namespace
                .entry(namespace.to_string())
                .or_insert(HashMap::new())
                .insert(
                    location.location_id.clone(),
                    schedule_manager.get_location_by_id(
                        &location.location_id, namespace
                    ).await.ok()?
                );
        };
        let association_nodes = match &location.association_nodes {
            HasMany::Loaded(x) => x,
            HasMany::Unloaded => return None,
        };
        add_associated_trains(
            &mut associations,
            association_nodes,
            date,
            &location.location_id,
            &location.id_suffix,
        );
    }

    let mut assoc_train_details: HashMap<String, Vec<BasicAssocTrainDetails>> = HashMap::new();
    for (train_id, day_diff, is_public, location_id, location_suffix, category) in &associations {
        let train = schedule_manager.get_train_by_id(train_id, namespace).await.ok()??;
        let other_date = if *day_diff >= 0 {
            date.add(Days::new(u64::try_from(*day_diff).unwrap()))
        } else {
            date.sub(Days::new(u64::try_from(-*day_diff).unwrap()))
        };
        let train = get_train_instance(&train, other_date).0;
        // No association? No problem, must just not be running this day...
        match train {
            Some(train) => {
                {
                    let route = match &train.route {
                        HasMany::Loaded(x) => x,
                        HasMany::Unloaded => return None,
                    };
                    locations_by_namespace
                        .entry(namespace.to_string())
                        .or_insert(HashMap::new())
                        .insert(
                            route.first().unwrap().location_id.clone(),
                            schedule_manager.get_location_by_id(
                                &route.first().unwrap().location_id, namespace
                            ).await.ok()?
                        );
                    locations_by_namespace
                        .entry(namespace.to_string())
                        .or_insert(HashMap::new())
                        .insert(
                            route.last().unwrap().location_id.clone(),
                            schedule_manager.get_location_by_id(
                                &route.last().unwrap().location_id, namespace
                            ).await.ok()?
                        );
                    let variable_train = train.variable_train.as_ref()?;
                    assoc_train_details
                        .entry(location_id.clone() + "|" +
                            &location_suffix.as_ref().unwrap_or(&"".to_string()))
                        .or_insert(vec![])
                        .push(BasicAssocTrainDetails {
                            id: train_id.clone(),
                            public_id: variable_train.public_id.clone(),
                            origin_id: route.first().unwrap().location_id.clone(),
                            destination_id: route.last().unwrap().location_id.clone(),
                            date: other_date.clone(),
                            namespace: namespace.to_string(),
                            is_public: *is_public,
                            category: *category,
                            name: variable_train.name.clone(),
                            dep_time: if route[0].public_dep.is_none() {
                                convert_tz(
                                    &other_date,
                                    &Some(0),
                                    &route[0].working_dep,
                                    &route[0].timing_tz,
                                    &locations_by_namespace[namespace][location_id]
                                        .clone().unwrap().timezone,
                                )
                                .ok()?
                                .unwrap().0
                            } else {
                                convert_tz(
                                    &other_date,
                                    &Some(0),
                                    &route[0].public_dep,
                                    &route[0].timing_tz,
                                    &locations_by_namespace[namespace][location_id]
                                        .clone().unwrap().timezone,
                                )
                                .ok()?
                                .unwrap().0
                            },
                        });
                };
            },
            None => (),
        };
    }

    let route = match &mut train.route {
        HasMany::Loaded(x) => x,
        HasMany::Unloaded => return None,
    };
    let mut dates = vec![];
    for extra_days in 0..(max(
        route.last().unwrap().working_arr_day,
        route.last().unwrap().public_arr_day,
    )
    .unwrap()
        + 1)
    {
        dates.push(date.add(Days::new(extra_days.into())));
    }

    // now convert all the timezones of all the stops
    for location in route.iter_mut() {
        (location.working_arr, location.working_arr_day) = match convert_tz(
            &date,
            &location.working_arr_day,
            &location.working_arr,
            &location.timing_tz,
            &locations_by_namespace[namespace][&location.location_id]
                .clone().unwrap().timezone,
        )
        .ok()? {
            Some((x, y)) => (Some(x), Some(y)),
            None => (None, None),
        };
        (location.working_dep, location.working_dep_day) = match convert_tz(
            &date,
            &location.working_dep_day,
            &location.working_dep,
            &location.timing_tz,
            &locations_by_namespace[namespace][&location.location_id].clone().unwrap().timezone,
        )
        .ok()? {
            Some((x, y)) => (Some(x), Some(y)),
            None => (None, None),
        };
        (location.working_pass, location.working_pass_day) = match convert_tz(
            &date,
            &location.working_pass_day,
            &location.working_pass,
            &location.timing_tz,
            &locations_by_namespace[namespace][&location.location_id].clone().unwrap().timezone,
        )
        .ok()? {
            Some((x, y)) => (Some(x), Some(y)),
            None => (None, None),
        };
        (location.public_arr, location.public_arr_day) = match convert_tz(
            &date,
            &location.public_arr_day,
            &location.public_arr,
            &location.timing_tz,
            &locations_by_namespace[namespace][&location.location_id].clone().unwrap().timezone,
        )
        .ok()? {
            Some((x, y)) => (Some(x), Some(y)),
            None => (None, None),
        };
        (location.public_dep, location.public_dep_day) = match convert_tz(
            &date,
            &location.public_dep_day,
            &location.public_dep,
            &location.timing_tz,
            &locations_by_namespace[namespace][&location.location_id].clone().unwrap().timezone,
        )
        .ok()? {
            Some((x, y)) => (Some(x), Some(y)),
            None => (None, None),
        };
    }

    let context = context! {
        train_id,
        train,
        locations_by_namespace,
        cancelled,
        modified,
        namespace: namespace.to_string(),
        dates,
        schedule_desc,
        assoc_train_details,
        duplicate_trains,
    };

    Some(Template::render("train.tera.html", &context))
}

#[derive(Clone, Debug, Serialize)]
struct BasicTrainForLocation {
    id: String,
    public_id: Option<String>,
    origins: Vec<String>,
    destinations: Vec<String>,
    working_arr: Option<NaiveDateTime>,
    working_dep: Option<NaiveDateTime>,
    working_pass: Option<NaiveDateTime>,
    public_arr: Option<NaiveDateTime>,
    public_dep: Option<NaiveDateTime>,
    platform: Option<String>,
    platform_zone: Option<String>,
    modified: bool,
    cancelled: bool,
    source: Option<TrainSource>,
    runs_as_required: bool,
    operator: Option<train_operator::ModelEx>,
    name: Option<String>,
    namespace: String,
    date: NaiveDate,
    is_first: bool,
    is_last: bool,
    cur_found_tos: usize,
}

async fn get_origins(
    i: usize,
    length: usize,
    location: &train_location::ModelEx,
    schedule_manager: Arc<ScheduleManager>,
    date: NaiveDate,
    namespace: &str,
) -> Vec<String> {
    let mut origins = vec![];

    let association_nodes = match &location.association_nodes {
        HasMany::Loaded(x) => x,
        HasMany::Unloaded => return vec![],
    };

    if i == 0 {
        let mut found_origin = false;
        // This is irrelevant for European-style divides so we only need to check the asymmetric
        // version
        for assoc in association_nodes.iter()
            .filter(|node| node.association_type == AssociationType::MainDividesFromOther) { 
            let final_assoc = match get_association(assoc, date) {
                Some(x) => x,
                None => continue,
            };

            let train = {
                match schedule_manager
                    .get_trains_for_location_lineup_by_ids(
                        &vec![(final_assoc.other_train_id.clone(), namespace.to_string())]
                        .into_iter().collect(),
                        &HashSet::new(),
                    )
                    .await
                    .ok()
                {
                    Some(mut x) if x.len() > 0 => x.remove(0),
                    Some(_) => continue,
                    None => continue,
                }
            };

            let other_date = if final_assoc.day_diff >= 0 {
                date.add(Days::new(u64::try_from(final_assoc.day_diff).unwrap()))
            } else {
                date.sub(Days::new(u64::try_from(-final_assoc.day_diff).unwrap()))
            };
            let (train, cancelled, _modified) = get_train_instance(&train, other_date);

            if cancelled || train.is_none() {
                continue;
            }

            found_origin = true;

            let route = match train.unwrap().route {
                HasMany::Loaded(x) => x,
                HasMany::Unloaded => continue,
            };
            for (i, other_location) in route.iter().enumerate() {
                if location.location_id == other_location.location_id
                    && assoc.other_train_location_id_suffix == other_location.id_suffix
                {
                    break;
                }

                origins.append(&mut Box::pin(get_origins(
                    i,
                    route.len(),
                    other_location,
                    schedule_manager.clone(),
                    other_date,
                    namespace,
                )).await);
            }
        }
        if !found_origin {
            origins.push(location.location_id.clone());
        }
    }

    let joins_to_check: Vec<&association_node::ModelEx> = if i == length - 1 {
        association_nodes.iter().filter(
            |x| x.association_type == AssociationType::MainJoinsToOther
        ).collect()
    } else {
        // If we are not the last location, this is likely a European-style join, in which case we
        // need to treat it symmetrically
        association_nodes.iter().filter(
            |x| x.association_type == AssociationType::MainJoinsToOther
                || x.association_type == AssociationType::MainIsJoinedToByOther
        ).collect()
    };

    for assoc in joins_to_check {
        let final_assoc = match get_association(&assoc, date) {
            Some(x) => x,
            None => continue,
        };

        let train = {
            match schedule_manager
                .get_trains_for_location_lineup_by_ids(
                    &vec![(final_assoc.other_train_id.clone(), namespace.to_string())]
                    .into_iter().collect(),
                    &HashSet::new(),
                )
                .await
                .ok()
            {
                Some(mut x) if x.len() > 0 => x.remove(0),
                Some(_) => continue,
                None => continue,
            }
        };

        let other_date = if final_assoc.day_diff >= 0 {
            date.add(Days::new(u64::try_from(final_assoc.day_diff).unwrap()))
        } else {
            date.sub(Days::new(u64::try_from(-final_assoc.day_diff).unwrap()))
        };
        let (train, cancelled, _modified) = get_train_instance(&train, other_date);

        if cancelled || train.is_none() {
            continue;
        }

        let route = match train.unwrap().route {
            HasMany::Loaded(x) => x,
            HasMany::Unloaded => continue,
        };
        for (i, other_location) in route.iter().enumerate() {
            if location.location_id == other_location.location_id
                && assoc.other_train_location_id_suffix == other_location.id_suffix
            {
                break;
            }

            origins.append(&mut Box::pin(get_origins(
                i,
                route.len(),
                other_location,
                schedule_manager.clone(),
                other_date,
                namespace,
            )).await);
        }
    }

    origins
}

async fn get_destinations(
    i: usize,
    length: usize,
    location: &train_location::ModelEx,
    schedule_manager: Arc<ScheduleManager>,
    date: NaiveDate,
    namespace: &str,
) -> Vec<String> {
    let mut destinations = vec![];

    let association_nodes = match &location.association_nodes {
        HasMany::Loaded(x) => x,
        HasMany::Unloaded => return vec![],
    };

    if i == length - 1 {
        let mut found_destination = false;
        // This is irrelevant for European-style joins so we only need to check the asymmetric
        // version
        for assoc in association_nodes.iter()
            .filter(|node| node.association_type == AssociationType::MainIsJoinedToByOther) { 
            let final_assoc = match get_association(assoc, date) {
                Some(x) => x,
                None => continue,
            };

            let train = {
                match schedule_manager
                    .get_trains_for_location_lineup_by_ids(
                        &vec![(final_assoc.other_train_id.clone(), namespace.to_string())]
                        .into_iter().collect(),
                        &HashSet::new(),
                    )
                    .await
                    .ok()
                {
                    Some(mut x) if x.len() > 0 => x.remove(0),
                    Some(_) => continue,
                    None => continue,
                }
            };

            let other_date = if final_assoc.day_diff >= 0 {
                date.add(Days::new(u64::try_from(final_assoc.day_diff).unwrap()))
            } else {
                date.sub(Days::new(u64::try_from(-final_assoc.day_diff).unwrap()))
            };
            let (train, cancelled, _modified) = get_train_instance(&train, other_date);

            if cancelled || train.is_none() {
                continue;
            }

            found_destination = true;

            let mut found = false;

            let route = match train.unwrap().route {
                HasMany::Loaded(x) => x,
                HasMany::Unloaded => continue,
            };
            for (i, other_location) in route.iter().enumerate() {
                if location.location_id == other_location.location_id
                    && assoc.other_train_location_id_suffix == other_location.id_suffix
                {
                    found = true;
                    continue;
                }

                if !found {
                    continue;
                }

                destinations.splice(0..0, Box::pin(get_destinations(
                    i,
                    route.len(),
                    other_location,
                    schedule_manager.clone(),
                    other_date,
                    namespace,
                )).await);
            }
        }
        if !found_destination {
            destinations.insert(0, location.location_id.clone());
        }
    }

    let divides_to_check: Vec<&association_node::ModelEx> = if i == 0 {
        association_nodes.iter().filter(
            |x| x.association_type == AssociationType::MainDividesToFormOther
        ).collect()
    } else {
        // If we are not the first location, this is likely a European-style divide, in which case
        // need to treat it symmetrically
        association_nodes.iter().filter(
            |x| x.association_type == AssociationType::MainDividesToFormOther
                || x.association_type == AssociationType::MainDividesFromOther
        ).collect()
    };

    for assoc in &divides_to_check {
        let final_assoc = match get_association(assoc, date) {
            Some(x) => x,
            None => continue,
        };

        let train = {
            match schedule_manager
                .get_trains_for_location_lineup_by_ids(
                    &vec![(final_assoc.other_train_id.clone(), namespace.to_string())]
                    .into_iter().collect(),
                    &HashSet::new(),
                )
                .await
                .ok()
            {
                Some(mut x) if x.len() > 0 => x.remove(0),
                Some(_) => continue,
                None => continue,
            }
        };

        let other_date = if final_assoc.day_diff >= 0 {
            date.add(Days::new(u64::try_from(final_assoc.day_diff).unwrap()))
        } else {
            date.sub(Days::new(u64::try_from(-final_assoc.day_diff).unwrap()))
        };
        let (train, cancelled, _modified) = get_train_instance(&train, other_date);

        if cancelled || train.is_none() {
            continue;
        }

        let mut found = false;
        let route = match train.unwrap().route {
            HasMany::Loaded(x) => x,
            HasMany::Unloaded => continue,
        };
        for (i, other_location) in route.iter().enumerate() {
            if location.location_id == other_location.location_id
                && assoc.other_train_location_id_suffix == other_location.id_suffix
            {
                found = true;
                continue;
            }

            if !found {
                continue;
            }

            destinations.splice(0..0, Box::pin(get_destinations(
                i,
                route.len(),
                other_location,
                schedule_manager.clone(),
                other_date,
                namespace,
            )).await);
        }
    }

    destinations
}

async fn location_line_up(
    namespace: &str,
    location_ids: &HashSet<(String, String)>,
    start_datetime: NaiveDateTime,
    end_datetime: NaiveDateTime,
    from_station: Option<HashSet<(String, String)>>,
    to_station: Option<HashSet<(String, String)>>,
    schedule_manager: Arc<ScheduleManager>,
) -> Option<Template> {
    let (trains, mut locations_by_namespace) = {
        let mut out_trains = vec![];
        let mut seen_ids = HashSet::new();
        let mut locations_by_namespace = HashMap::new();
        for (location_id, location_namespace) in location_ids {
            if location_namespace != namespace {
                match schedule_manager.get_schedule_by_id(location_namespace).await.ok().unwrap() {
                    Some(_) => (),
                    // If the namespace for this location isn't loaded, that's fine, we just
                    // continue, it's probably just disabled.
                    None => continue,
                };
            }
            let (train_ids, excluded_variants) = schedule_manager
                .get_train_ids_with_excluded_variants_by_location_id_for_dates(
                    location_id, location_namespace, start_datetime.date(), end_datetime.date()
                ).await.unwrap();
            let trains = schedule_manager.get_trains_for_location_lineup_by_ids(
                &train_ids, &excluded_variants
            ).await.unwrap();
            for train in trains.into_iter() {
                // OK, we have a train, but now we want to check if it has a duplicate in the
                // current namespace. We prefer to show trains in the current namespace over an
                // alternative, and we hide duplicates.
                if location_namespace != namespace {
                    if futures::future::join_all(train
                        .train_variants
                        .iter()
                        .map(
                            async |train| schedule_manager
                                .get_duplicate_trains(&train.namespace, train)
                                .await
                                .unwrap()
                                .iter()
                                .any(|(_, duplicate_namespace)| duplicate_namespace == namespace)
                        ))
                        .await
                        .iter()
                        .any(|x| *x) {
                        continue;
                    };
                }
                if !seen_ids.contains(&(train.id.clone(), train.namespace.clone())) {
                    seen_ids.insert((train.id.clone(), train.namespace.clone()));
                    let namespace = train.namespace.clone();
                    out_trains.push((train, namespace));
                }
            }
            if !locations_by_namespace
                .entry(location_namespace.clone())
                .or_insert(HashMap::new())
                .contains_key(location_id) {
                locations_by_namespace
                    .entry(location_namespace.clone())
                    .or_insert(HashMap::new())
                    .insert(
                        location_id.clone(),
                        schedule_manager
                            .get_location_by_id(&location_id, &location_namespace).await.unwrap()
                            .clone(),
                    );
            }
        }
        (out_trains, locations_by_namespace)
    };

    let mut actual_trains = vec![];
    for (train, train_namespace) in trains {
        // OK, this is somewhat hacky but I haven't yet thought of a better way.
        if train.train_variants.len() == 0 {
            // deleted trains remain in database
            continue;
        }
        let mut max_day_offset = 1;
        for train_variant in &train.train_variants {
            let route = match &train_variant.route {
                HasMany::Loaded(x) => x,
                HasMany::Unloaded => return None,
            };
            let last_location = &route.last().unwrap();
            // We add one to allow for differences between timing timezone and location timezone
            let day_offset = if last_location.working_arr_day.is_none() {
                last_location.public_arr_day.unwrap()
            } else {
                last_location.working_arr_day.unwrap()
            } + 1;
            max_day_offset = max(max_day_offset, day_offset);
        }

        let first_date = start_datetime.date().sub(Days::new(max_day_offset.into()));
        let end_date = end_datetime.date().add(Days::new(1)); // one past the end
        let mut cur_date = first_date;

        while cur_date != end_date {
            let train_id = train.id.clone();
            let (train, cancelled, modified) = match get_train_instance(&train, cur_date) {
                (Some(x), y, z) => (x, y, z),
                _ => {
                    cur_date = cur_date.add(Days::new(1));
                    continue;
                }
            };

            let route = match &train.route {
                HasMany::Loaded(x) => x,
                HasMany::Unloaded => return None,
            };

            let mut additions_for_this_train: Vec<BasicTrainForLocation> = vec![];
            let mut origins_so_far = vec![];
            let mut variable_train = train.variable_train.as_ref().unwrap();
            let mut found_from = match from_station {
                Some(_) => false,
                None => true,
            };
            let mut just_found_from = false;
            let mut cur_found_tos = 0;
            let mut at_least_one_stop = false;
            for location in route.iter() {
                if !locations_by_namespace
                    .entry(train_namespace.clone())
                    .or_insert(HashMap::new())
                    .contains_key(&location.location_id) {
                    locations_by_namespace
                        .entry(train_namespace.clone())
                        .or_insert(HashMap::new())
                        .insert(
                            location.location_id.clone(),
                            schedule_manager
                                .get_location_by_id(
                                    &location.location_id, &train_namespace
                                ).await.unwrap()
                                .clone(),
                        );
                }
                let location_detail
                    = locations_by_namespace[&train_namespace][&location.location_id]
                    .clone()
                    .unwrap();

                if !location_ids.contains(
                    &(location.location_id.clone(), train_namespace.clone())
                ) {
                    continue;
                }

                let (best_time, best_offset) = {
                    if location.working_dep.is_some() {
                        (
                            location.working_dep.unwrap(),
                            location.working_dep_day.unwrap(),
                        )
                    } else if location.public_dep.is_some() {
                        (
                            location.public_dep.unwrap(),
                            location.public_dep_day.unwrap(),
                        )
                    } else if location.working_pass.is_some() {
                        (
                            location.working_pass.unwrap(),
                            location.working_pass_day.unwrap(),
                        )
                    } else if location.working_arr.is_some() {
                        (
                            location.working_arr.unwrap(),
                            location.working_arr_day.unwrap(),
                        )
                    } else if location.public_arr.is_some() {
                        (
                            location.public_arr.unwrap(),
                            location.public_arr_day.unwrap(),
                        )
                    } else {
                        return None;
                    }
                };
                let (best_time, best_offset) = convert_tz(
                    &cur_date,
                    &Some(best_offset),
                    &Some(best_time),
                    &location.timing_tz,
                    &location_detail.timezone,
                ).ok()?.unwrap();
                let time_from_cur_date = cur_date
                    .add(Days::new(best_offset.into()))
                    .and_time(best_time);
                if time_from_cur_date < start_datetime || time_from_cur_date > end_datetime {
                    continue;
                }
                at_least_one_stop = true;
            }
            if at_least_one_stop {
                for (i, location) in route.iter().enumerate() {
                    let location_detail
                        = locations_by_namespace[&train_namespace][&location.location_id]
                        .clone()
                        .unwrap();
                    if just_found_from {
                        found_from = true;
                        just_found_from = false;
                    }

                    if !location.change_en_route.is_none() {
                        variable_train = &location.change_en_route.as_ref().unwrap();
                    }

                    if !found_from {
                        just_found_from = from_station.as_ref().unwrap().contains(
                            &(location.location_id.clone(), train_namespace.clone())
                        );
                    }
                    if to_station.is_some() {
                        if to_station.as_ref().unwrap().contains(
                            &(location.location_id.clone(), train_namespace.clone())
                        ) {
                            cur_found_tos += 1;
                        }
                    }

                    let mut origins = get_origins(
                        i,
                        route.len(),
                        &location,
                        schedule_manager.clone(),
                        cur_date,
                        &train_namespace,
                    ).await;

                    for origin in &origins {
                        if !locations_by_namespace
                            .entry(train_namespace.clone())
                            .or_insert(HashMap::new())
                            .contains_key(origin) {
                            locations_by_namespace
                                .entry(train_namespace.clone())
                                .or_insert(HashMap::new())
                                .insert(
                                    origin.clone(),
                                    schedule_manager
                                        .get_location_by_id(origin, &train_namespace).await.unwrap()
                                        .clone(),
                                );
                        }
                    }

                    origins_so_far.append(&mut origins);

                    let destinations = get_destinations(
                        i,
                        route.len(),
                        &location,
                        schedule_manager.clone(),
                        cur_date,
                        &train_namespace,
                    ).await;

                    for destination in &destinations {
                        if !locations_by_namespace
                            .entry(train_namespace.clone())
                            .or_insert(HashMap::new())
                            .contains_key(destination) {
                            locations_by_namespace
                                .entry(train_namespace.clone())
                                .or_insert(HashMap::new())
                                .insert(
                                    destination.clone(),
                                    schedule_manager
                                        .get_location_by_id(destination, &train_namespace).await.unwrap()
                                        .clone(),
                                );
                        }
                    }

                    for addition in &mut additions_for_this_train {
                        addition.destinations.splice(0..0, destinations.clone());
                    }

                    if !location_ids.contains(
                        &(location.location_id.clone(), train_namespace.clone())
                    ) {
                        continue;
                    }

                    if from_station.is_some() && !found_from {
                        continue;
                    }

                    let (best_time, best_offset) = {
                        if location.working_dep.is_some() {
                            (
                                location.working_dep.unwrap(),
                                location.working_dep_day.unwrap(),
                            )
                        } else if location.public_dep.is_some() {
                            (
                                location.public_dep.unwrap(),
                                location.public_dep_day.unwrap(),
                            )
                        } else if location.working_pass.is_some() {
                            (
                                location.working_pass.unwrap(),
                                location.working_pass_day.unwrap(),
                            )
                        } else if location.working_arr.is_some() {
                            (
                                location.working_arr.unwrap(),
                                location.working_arr_day.unwrap(),
                            )
                        } else if location.public_arr.is_some() {
                            (
                                location.public_arr.unwrap(),
                                location.public_arr_day.unwrap(),
                            )
                        } else {
                            return None;
                        }
                    };
                    let (best_time, best_offset) = convert_tz(
                        &cur_date,
                        &Some(best_offset),
                        &Some(best_time),
                        &location.timing_tz,
                        &location_detail.timezone,
                    ).ok()?.unwrap();
                    let time_from_cur_date = cur_date
                        .add(Days::new(best_offset.into()))
                        .and_time(best_time);
                    if time_from_cur_date < start_datetime || time_from_cur_date > end_datetime {
                        continue;
                    }

                    // special case: add this station as destination if we are in the last iteration
                    let starting_destinations = if i == route.len() - 1 {
                        let mut dests = vec![];
                        dests.push(location.location_id.clone());
                        dests
                    } else {
                        vec![]
                    };
                    
                    let operator = match &variable_train.operator {
                        BelongsTo::Loaded(x) => x,
                        BelongsTo::Unloaded => return None,
                    };

                    let operator = match &operator {
                        Some(x) => Some((**x).clone()),
                        None => None,
                    };

                    // Convert all the timezones
                    let (working_arr, working_arr_day) = match convert_tz(
                        &cur_date,
                        &location.working_arr_day,
                        &location.working_arr,
                        &location.timing_tz,
                        &location_detail.timezone,
                    )
                    .ok()? {
                        Some((x, y)) => (Some(x), Some(y)),
                        None => (None, None),
                    };
                    let (working_dep, working_dep_day) = match convert_tz(
                        &cur_date,
                        &location.working_dep_day,
                        &location.working_dep,
                        &location.timing_tz,
                        &location_detail.timezone,
                    )
                    .ok()? {
                        Some((x, y)) => (Some(x), Some(y)),
                        None => (None, None),
                    };
                    let (working_pass, working_pass_day) = match convert_tz(
                        &cur_date,
                        &location.working_pass_day,
                        &location.working_pass,
                        &location.timing_tz,
                        &location_detail.timezone,
                    )
                    .ok()? {
                        Some((x, y)) => (Some(x), Some(y)),
                        None => (None, None),
                    };
                    let (public_arr, public_arr_day) = match convert_tz(
                        &cur_date,
                        &location.public_arr_day,
                        &location.public_arr,
                        &location.timing_tz,
                        &location_detail.timezone,
                    )
                    .ok()? {
                        Some((x, y)) => (Some(x), Some(y)),
                        None => (None, None),
                    };
                    let (public_dep, public_dep_day) = match convert_tz(
                        &cur_date,
                        &location.public_dep_day,
                        &location.public_dep,
                        &location.timing_tz,
                        &location_detail.timezone,
                    )
                    .ok()? {
                        Some((x, y)) => (Some(x), Some(y)),
                        None => (None, None),
                    };

                    additions_for_this_train.push(BasicTrainForLocation {
                        id: train_id.clone(),
                        public_id: variable_train.public_id.clone(),
                        origins: origins_so_far.clone(),
                        destinations: starting_destinations,
                        working_arr: match working_arr {
                            None => None,
                            Some(x) => Some(
                                cur_date
                                    .add(Days::new(working_arr_day.unwrap().into()))
                                    .and_time(x),
                            ),
                        },
                        working_dep: match working_dep {
                            None => None,
                            Some(x) => Some(
                                cur_date
                                    .add(Days::new(working_dep_day.unwrap().into()))
                                    .and_time(x),
                            ),
                        },
                        working_pass: match working_pass {
                            None => None,
                            Some(x) => Some(
                                cur_date
                                    .add(Days::new(working_pass_day.unwrap().into()))
                                    .and_time(x),
                            ),
                        },
                        public_arr: match public_arr {
                            None => None,
                            Some(x) => Some(
                                cur_date
                                    .add(Days::new(public_arr_day.unwrap().into()))
                                    .and_time(x),
                            ),
                        },
                        public_dep: match public_dep {
                            None => None,
                            Some(x) => Some(
                                cur_date
                                    .add(Days::new(public_dep_day.unwrap().into()))
                                    .and_time(x),
                            ),
                        },
                        platform: location.platform.clone(),
                        platform_zone: location.platform_zone.clone(),
                        modified,
                        cancelled,
                        source: train.source,
                        runs_as_required: train.runs_as_required,
                        operator: operator,
                        name: variable_train.name.clone(),
                        namespace: train_namespace.clone(),
                        date: cur_date,
                        is_first: i == 0,
                        is_last: i == route.len() - 1,
                        cur_found_tos,
                    });
                }
            }

            cur_date = cur_date.add(Days::new(1));

            if to_station.is_some() {
                for addition in additions_for_this_train {
                    if cur_found_tos > addition.cur_found_tos {
                        actual_trains.push(addition.clone());
                    }
                }
            } else {
                actual_trains.append(&mut additions_for_this_train);
            }
        }
    }

    actual_trains.sort_by_key(|train| {
        // Sort secondarily by ID as a simple tiebreaker to give a stable order
        if train.working_dep.is_some() {
            (train.working_dep, train.id.clone())
        } else if train.public_dep.is_some() {
            (train.public_dep, train.id.clone())
        } else if train.working_pass.is_some() {
            (train.working_pass, train.id.clone())
        } else if train.working_arr.is_some() {
            (train.working_arr, train.id.clone())
        } else if train.public_arr.is_some() {
            (train.public_arr, train.id.clone())
        } else {
            (None, train.id.clone())
        }
    });

    let context = context! {
        actual_trains,
        locations_by_namespace,
        location_id: &location_ids
            .iter()
            .filter(|x| x.1 == namespace)
            .next()
            .unwrap()
            .0,
        namespace: namespace.to_string(),
    };

    Some(Template::render("location.tera.html", &context))
}

struct Namespace {
    namespace: String,
    is_public_id: bool,
}

impl<'a> FromParam<'a> for Namespace {
    type Error = WebUiError;

    fn from_param(param: &'a str) -> Result<Self, Self::Error> {
        let parts = param.split("-").collect::<Vec<&str>>();
        if parts.len() != 2 {
            return Err(WebUiError {
                what: "Invalid namespace string".to_string(),
            });
        }

        match parts[1] {
            "public" => Ok(Namespace {
                namespace: parts[0].to_string(),
                is_public_id: true,
            }),
            "internal" => Ok(Namespace {
                namespace: parts[0].to_string(),
                is_public_id: false,
            }),
            _ => {
                return Err(WebUiError {
                    what: "Invalid ID type".to_string(),
                })
            }
        }
    }
}

async fn get_location_ids_and_first_tz(
    location_id: &str,
    namespace: &Namespace,
    schedule_manager: Arc<ScheduleManager>,
) -> Option<(HashSet<(String, String)>, String)> {
    match namespace.is_public_id {
        true => {
            let locations = schedule_manager
                .get_locations_by_public_id(location_id, &namespace.namespace).await.ok()?;
            if locations.len() == 0 {
                return None;
            }
            let mut all_locations: HashSet<(String, String)> = locations.iter().map(
                |location| (location.id.clone(), namespace.namespace.clone())
            ).collect();
            for location in locations {
                match schedule_manager.location_associations_by_id.get(&location.id) {
                    Some(location_association) => {
                        for associated_location in &location_association.associated_locations {
                            match schedule_manager
                                .get_schedule_by_id(&associated_location.namespace).await.ok()? {
                                Some(_) => (),
                                // If the namespace for this location isn't loaded, that's fine, we
                                // just continue, it's probably just disabled.
                                None => continue,
                            };
                            match &associated_location.id {
                                Some(id) => {
                                    all_locations.insert(
                                        (id.to_string(), associated_location.namespace.clone())
                                    );
                                },
                                None => (),
                            };
                            match &associated_location.public_id {
                                Some(public_id) => {
                                    all_locations = all_locations.union(
                                        &schedule_manager.get_locations_by_public_id(
                                            public_id, &associated_location.namespace
                                        ).await.ok()?.into_iter().map(
                                            |x| (
                                                x.id.clone(),
                                                associated_location.namespace.clone()
                                            )
                                        ).collect()
                                    ).cloned().collect();
                                },
                                None => (),
                            };
                        }
                    },
                    None => (),
                };
                match schedule_manager.location_associations_by_public_id.get(location_id) {
                    Some(location_association) => {
                        for associated_location in &location_association.associated_locations {
                            match schedule_manager
                                .get_schedule_by_id(&associated_location.namespace).await.ok()? {
                                Some(_) => (),
                                // If the namespace for this location isn't loaded, that's fine, we
                                // just continue, it's probably just disabled.
                                None => continue,
                            };
                            match &associated_location.id {
                                Some(id) => {
                                    all_locations.insert(
                                        (id.to_string(), associated_location.namespace.clone())
                                    );
                                },
                                None => (),
                            };
                            match &associated_location.public_id {
                                Some(public_id) => {
                                    all_locations = all_locations.union(
                                        &schedule_manager.get_locations_by_public_id(
                                            public_id, &associated_location.namespace
                                        ).await.ok()?.into_iter().map(
                                            |x| (
                                                x.id.clone(),
                                                associated_location.namespace.clone()
                                            )
                                        ).collect()
                                    ).cloned().collect();
                                },
                                None => (),
                            };
                        }
                    },
                    None => (),
                };
            }
            Some((
                all_locations.clone(),
                schedule_manager
                    .get_location_by_id(
                        &all_locations
                        .iter()
                        .filter(|x| x.1 == namespace.namespace)
                        .next()
                        .unwrap()
                        .0,
                        &namespace.namespace
                    )
                    .await
                    .unwrap()
                    .unwrap()
                    .timezone
                    .clone(),
            ))
        }
        false => {
            let mut location_ids = HashSet::new();
            location_ids.insert(location_id.to_string());
            let mut all_locations: HashSet<(String, String)> = location_ids.into_iter().map(
                |location| (location.clone(), namespace.namespace.clone())
            ).collect();
            match schedule_manager.location_associations_by_id.get(location_id) {
                Some(location_association) => {
                    for associated_location in &location_association.associated_locations {
                        match schedule_manager
                            .get_schedule_by_id(&associated_location.namespace).await.ok()? {
                            Some(_) => (),
                            // If the namespace for this location isn't loaded, that's fine, we
                            // just continue, it's probably just disabled.
                            None => continue,
                        };
                        match &associated_location.id {
                            Some(id) => {
                                all_locations.insert(
                                    (id.to_string(), associated_location.namespace.clone())
                                );
                            },
                            None => (),
                        };
                        match &associated_location.public_id {
                            Some(public_id) => {
                                all_locations = all_locations.union(
                                    &schedule_manager.get_locations_by_public_id(
                                        public_id, &associated_location.namespace
                                    ).await.ok()?.into_iter().map(
                                        |x| (x.id.clone(), associated_location.namespace.clone())
                                    ).collect()
                                ).cloned().collect();
                            },
                            None => (),
                        };
                    }
                },
                None => (),
            }
            let location = match
                schedule_manager.get_location_by_id(location_id, &namespace.namespace).await.ok()? {
                Some(x) => x,
                None => return None,
            };
            match &location.public_id {
                Some(public_id) => match schedule_manager.location_associations_by_public_id.get(
                    public_id
                ) {
                    Some(location_association) => {
                        for associated_location in &location_association.associated_locations {
                            match schedule_manager
                                .get_schedule_by_id(&associated_location.namespace).await.ok()? {
                                Some(_) => (),
                                // If the namespace for this location isn't loaded, that's fine, we
                                // just continue, it's probably just disabled.
                                None => continue,
                            };
                            match &associated_location.id {
                                Some(id) => {
                                    all_locations.insert(
                                        (id.to_string(), associated_location.namespace.clone())
                                    );
                                },
                                None => (),
                            };
                            match &associated_location.public_id {
                                Some(public_id) => {
                                    all_locations = all_locations.union(
                                        &schedule_manager.get_locations_by_public_id(
                                            public_id, &associated_location.namespace
                                        ).await.ok()?.into_iter().map(
                                            |x| (
                                                x.id.clone(),
                                                associated_location.namespace.clone()
                                            )
                                        ).collect()
                                    ).cloned().collect();
                                },
                                None => (),
                            };
                        }
                    },
                    None => (),
                },
                None => (),
            }
            Some((all_locations, location.timezone))
        }
    }
}

#[get("/location/<namespace>/<location_id>")]
async fn location(
    namespace: Namespace,
    location_id: &str,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (location_ids, timezone) =
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone()).await?;

    let timezone = match Tz::from_str(&timezone) {
        Ok(x) => x,
        Err(_) => return None,
    };
    let now = timezone
        .from_utc_datetime(&Utc::now().naive_utc())
        .naive_local();

    location_line_up(
        &namespace.namespace,
        &location_ids,
        now - Duration::minutes(30),
        now + Duration::minutes(120),
        None,
        None,
        (*schedule_manager).clone(),
    ).await
}

#[get("/location/<namespace>/<location_id>/from/<from_id>", rank = 0)]
async fn location_from(
    namespace: Namespace,
    location_id: &str,
    from_id: &str,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (location_ids, timezone) =
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone()).await?;

    let timezone = match Tz::from_str(&timezone) {
        Ok(x) => x,
        Err(_) => return None,
    };
    let now = timezone
        .from_utc_datetime(&Utc::now().naive_utc())
        .naive_local();

    let (from_ids, _timezone) =
        get_location_ids_and_first_tz(from_id, &namespace, (*schedule_manager).clone()).await?;

    location_line_up(
        &namespace.namespace,
        &location_ids,
        now - Duration::minutes(30),
        now + Duration::minutes(120),
        Some(from_ids),
        None,
        (*schedule_manager).clone(),
    ).await
}

#[get("/location/<namespace>/<location_id>/to/<to_id>", rank = 0)]
async fn location_to(
    namespace: Namespace,
    location_id: &str,
    to_id: &str,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (location_ids, timezone) =
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone()).await?;

    let timezone = match Tz::from_str(&timezone) {
        Ok(x) => x,
        Err(_) => return None,
    };
    let now = timezone
        .from_utc_datetime(&Utc::now().naive_utc())
        .naive_local();

    let (to_ids, _timezone) =
        get_location_ids_and_first_tz(to_id, &namespace, (*schedule_manager).clone()).await?;

    location_line_up(
        &namespace.namespace,
        &location_ids,
        now - Duration::minutes(30),
        now + Duration::minutes(120),
        None,
        Some(to_ids),
        (*schedule_manager).clone(),
    ).await
}

#[get(
    "/location/<namespace>/<location_id>/from/<from_id>/to/<to_id>",
    rank = 0
)]
async fn location_from_to(
    namespace: Namespace,
    location_id: &str,
    from_id: &str,
    to_id: &str,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (location_ids, timezone) =
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone()).await?;

    let timezone = match Tz::from_str(&timezone) {
        Ok(x) => x,
        Err(_) => return None,
    };
    let now = timezone
        .from_utc_datetime(&Utc::now().naive_utc())
        .naive_local();

    let (from_ids, _timezone) =
        get_location_ids_and_first_tz(from_id, &namespace, (*schedule_manager).clone()).await?;
    let (to_ids, _timezone) =
        get_location_ids_and_first_tz(to_id, &namespace, (*schedule_manager).clone()).await?;

    location_line_up(
        &namespace.namespace,
        &location_ids,
        now - Duration::minutes(30),
        now + Duration::minutes(120),
        Some(from_ids),
        Some(to_ids),
        (*schedule_manager).clone(),
    ).await
}

#[get("/location/<namespace>/<location_id>/<date>/<time>", rank = 1)]
async fn location_time(
    namespace: Namespace,
    location_id: &str,
    date: NaiveDateRocket,
    time: NaiveTimeRocket,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (location_ids, _timezone) =
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone()).await?;

    location_line_up(
        &namespace.namespace,
        &location_ids,
        date.0.and_time(time.0) - Duration::minutes(30),
        date.0.and_time(time.0) + Duration::minutes(120),
        None,
        None,
        (*schedule_manager).clone(),
    ).await
}

#[get(
    "/location/<namespace>/<location_id>/from/<from_id>/<date>/<time>",
    rank = 1
)]
async fn location_from_time(
    namespace: Namespace,
    location_id: &str,
    from_id: &str,
    date: NaiveDateRocket,
    time: NaiveTimeRocket,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (location_ids, _timezone) =
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone()).await?;

    let (from_ids, _timezone) =
        get_location_ids_and_first_tz(from_id, &namespace, (*schedule_manager).clone()).await?;

    location_line_up(
        &namespace.namespace,
        &location_ids,
        date.0.and_time(time.0) - Duration::minutes(30),
        date.0.and_time(time.0) + Duration::minutes(120),
        Some(from_ids),
        None,
        (*schedule_manager).clone(),
    ).await
}

#[get(
    "/location/<namespace>/<location_id>/to/<to_id>/<date>/<time>",
    rank = 1
)]
async fn location_to_time(
    namespace: Namespace,
    location_id: &str,
    to_id: &str,
    date: NaiveDateRocket,
    time: NaiveTimeRocket,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (location_ids, _timezone) =
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone()).await?;

    let (to_ids, _timezone) =
        get_location_ids_and_first_tz(to_id, &namespace, (*schedule_manager).clone()).await?;

    location_line_up(
        &namespace.namespace,
        &location_ids,
        date.0.and_time(time.0) - Duration::minutes(30),
        date.0.and_time(time.0) + Duration::minutes(120),
        None,
        Some(to_ids),
        (*schedule_manager).clone(),
    ).await
}

#[get(
    "/location/<namespace>/<location_id>/from/<from_id>/to/<to_id>/<date>/<time>",
    rank = 1
)]
async fn location_from_to_time(
    namespace: Namespace,
    location_id: &str,
    from_id: &str,
    to_id: &str,
    date: NaiveDateRocket,
    time: NaiveTimeRocket,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (location_ids, _timezone) =
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone()).await?;

    let (from_ids, _timezone) =
        get_location_ids_and_first_tz(from_id, &namespace, (*schedule_manager).clone()).await?;
    let (to_ids, _timezone) =
        get_location_ids_and_first_tz(to_id, &namespace, (*schedule_manager).clone()).await?;

    location_line_up(
        &namespace.namespace,
        &location_ids,
        date.0.and_time(time.0) - Duration::minutes(30),
        date.0.and_time(time.0) + Duration::minutes(120),
        Some(from_ids),
        Some(to_ids),
        (*schedule_manager).clone(),
    ).await
}

#[get(
    "/location/<namespace>/<location_id>/<date>/<from_time>/to/<to_time>",
    rank = 2
)]
async fn location_time_to(
    namespace: Namespace,
    location_id: &str,
    date: NaiveDateRocket,
    from_time: NaiveTimeRocket,
    to_time: NaiveTimeRocket,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (location_ids, _timezone) =
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone()).await?;

    let to_date = if to_time.0 < from_time.0 {
        date.0 + Days::new(1)
    } else {
        date.0
    };

    location_line_up(
        &namespace.namespace,
        &location_ids,
        date.0.and_time(from_time.0),
        to_date.and_time(to_time.0),
        None,
        None,
        (*schedule_manager).clone(),
    ).await
}

#[get(
    "/location/<namespace>/<location_id>/from/<from_id>/<date>/<from_time>/to/<to_time>",
    rank = 2
)]
async fn location_from_time_to(
    namespace: Namespace,
    location_id: &str,
    from_id: &str,
    date: NaiveDateRocket,
    from_time: NaiveTimeRocket,
    to_time: NaiveTimeRocket,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (location_ids, _timezone) =
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone()).await?;

    let to_date = if to_time.0 < from_time.0 {
        date.0 + Days::new(1)
    } else {
        date.0
    };

    let (from_ids, _timezone) =
        get_location_ids_and_first_tz(from_id, &namespace, (*schedule_manager).clone()).await?;

    location_line_up(
        &namespace.namespace,
        &location_ids,
        date.0.and_time(from_time.0),
        to_date.and_time(to_time.0),
        Some(from_ids),
        None,
        (*schedule_manager).clone(),
    ).await
}

#[get(
    "/location/<namespace>/<location_id>/to/<to_id>/<date>/<from_time>/to/<to_time>",
    rank = 2
)]
async fn location_to_time_to(
    namespace: Namespace,
    location_id: &str,
    to_id: &str,
    date: NaiveDateRocket,
    from_time: NaiveTimeRocket,
    to_time: NaiveTimeRocket,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (location_ids, _timezone) =
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone()).await?;

    let to_date = if to_time.0 < from_time.0 {
        date.0 + Days::new(1)
    } else {
        date.0
    };

    let (to_ids, _timezone) =
        get_location_ids_and_first_tz(to_id, &namespace, (*schedule_manager).clone()).await?;

    location_line_up(
        &namespace.namespace,
        &location_ids,
        date.0.and_time(from_time.0),
        to_date.and_time(to_time.0),
        None,
        Some(to_ids),
        (*schedule_manager).clone(),
    ).await
}

#[get(
    "/location/<namespace>/<location_id>/from/<from_id>/to/<to_id>/<date>/<from_time>/to/<to_time>",
    rank = 2
)]
async fn location_from_to_time_to(
    namespace: Namespace,
    location_id: &str,
    from_id: &str,
    to_id: &str,
    date: NaiveDateRocket,
    from_time: NaiveTimeRocket,
    to_time: NaiveTimeRocket,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (location_ids, _timezone) =
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone()).await?;

    let to_date = if to_time.0 < from_time.0 {
        date.0 + Days::new(1)
    } else {
        date.0
    };

    let (from_ids, _timezone) =
        get_location_ids_and_first_tz(from_id, &namespace, (*schedule_manager).clone()).await?;
    let (to_ids, _timezone) =
        get_location_ids_and_first_tz(to_id, &namespace, (*schedule_manager).clone()).await?;

    location_line_up(
        &namespace.namespace,
        &location_ids,
        date.0.and_time(from_time.0),
        to_date.and_time(to_time.0),
        Some(from_ids),
        Some(to_ids),
        (*schedule_manager).clone(),
    ).await
}

pub async fn rocket(schedule_manager: Arc<ScheduleManager>) -> Result<(), Error> {
    rocket::build()
        .mount(
            "/",
            routes![
                index,
                train_on_date,
                location,
                location_from,
                location_to,
                location_from_to,
                location_time,
                location_from_time,
                location_to_time,
                location_from_to_time,
                location_time_to,
                location_from_time_to,
                location_to_time_to,
                location_from_to_time_to
            ],
        )
        .attach(Template::custom(|tera| {
            tera.register_filter("hex_colour", hex_colour);
        }))
        .manage(schedule_manager)
        .launch()
        .await?;

    Err(Error::WebUiError(WebUiError {
        what: "Shutdown requested".to_string(),
    }))
}
