use chrono::naive::Days;
use chrono::offset::LocalResult;
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, ParseError, TimeZone, Utc};
use chrono_tz::Tz;

use crate::error::Error;
use crate::schedule::{AssociationNode, Train, TrainLocation, TrainOperator, TrainSource};
use crate::schedule_manager::ScheduleManager;

use rocket::request::FromParam;
use rocket::{get, routes, State};
use rocket_dyn_templates::{context, Template};

use serde::Serialize;

use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops::{Add, Sub};
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

#[get("/")]
fn index(schedule_manager: &State<Arc<ScheduleManager>>) -> Template {
    let namespaces = {
        let schedule_manager = schedule_manager.read();
        let mut map = HashMap::new();
        for (namespace, schedule) in &*schedule_manager {
            map.insert(namespace.clone(), schedule.description.clone());
        }
        map
    };

    let context = context! {
        namespaces,
    };

    Template::render("index", &context)
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
    time_tz: &Option<Tz>,
    target_tz: &Tz,
) -> Result<Option<NaiveTime>, Error> {
    let time_tz = match time_tz {
        None => return Ok(time.clone()),
        Some(x) => x,
    };
    let (time, day_diff) = match time {
        None => return Ok(None),
        Some(x) => (x, day_diff.unwrap()),
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

    let output_time_tz = date_time_with_tz.with_timezone(target_tz);

    Ok(Some(output_time_tz.time()))
}

fn get_train_instance(trains: &Vec<Train>, date: NaiveDate) -> (Option<Train>, bool, bool) {
    // let's make life easy and find the right train
    let mut final_train = None;
    let mut cancelled = false;
    let mut modified = false;
    for train in trains {
        for validity in &train.validity {
            if validity.valid_begin.date_naive() <= date
                && validity.valid_end.date_naive() >= date
                && validity.days_of_week.get_by_weekday(date.weekday())
            {
                cancelled = false;
                modified = false;
                'replacement: for replacement in &train.replacements {
                    for validity in &replacement.validity {
                        if validity.valid_begin.date_naive() <= date
                            && validity.valid_end.date_naive() >= date
                            && validity.days_of_week.get_by_weekday(date.weekday())
                        {
                            final_train = Some(replacement.clone());
                            modified = true;
                            break 'replacement;
                        }
                    }
                }
                if final_train.is_none() {
                    final_train = Some(train.clone());
                }
                for (cancellation, _source) in &train.cancellations {
                    if cancellation.valid_begin.date_naive() <= date
                        && cancellation.valid_end.date_naive() >= date
                        && cancellation.days_of_week.get_by_weekday(date.weekday())
                    {
                        cancelled = true;
                    }
                }
            }
        }
    }

    return (final_train, cancelled, modified);
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
enum AssociationCategory {
    Join,
    Divide,
    Next,
    IsJoinedToBy,
    DividesFrom,
    FormsFrom,
}

#[derive(Clone, Debug, Serialize)]
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

fn get_association(assoc: &AssociationNode, date: NaiveDate) -> Option<AssociationNode> {
    let mut final_assoc = None;
    let mut cancelled = false;
    for validity in &assoc.validity {
        if validity.valid_begin.date_naive() <= date
            && validity.valid_end.date_naive() >= date
            && validity.days_of_week.get_by_weekday(date.weekday())
        {
            cancelled = false;
            'replacement: for replacement in &assoc.replacements {
                for validity in &replacement.validity {
                    if validity.valid_begin.date_naive() <= date
                        && validity.valid_end.date_naive() >= date
                        && validity.days_of_week.get_by_weekday(date.weekday())
                    {
                        final_assoc = Some(replacement.clone());
                        break 'replacement;
                    }
                }
            }
            if final_assoc.is_none() {
                final_assoc = Some(assoc.clone());
            }
            for (cancellation, _source) in &assoc.cancellations {
                if cancellation.valid_begin.date_naive() <= date
                    && cancellation.valid_end.date_naive() >= date
                    && cancellation.days_of_week.get_by_weekday(date.weekday())
                {
                    cancelled = true;
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
    assoc: &AssociationNode,
    date: NaiveDate,
    location: &String,
    location_suffix: &Option<String>,
    category: AssociationCategory,
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
        category,
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
    assoc_vec: &Vec<AssociationNode>,
    date: NaiveDate,
    location: &String,
    location_suffix: &Option<String>,
    category: AssociationCategory,
) -> () {
    for assoc in assoc_vec {
        add_associated_train(
            associations,
            &assoc,
            date,
            location,
            location_suffix,
            category,
        );
    }
}

#[get("/train/<namespace>/<train_id>/<date>")]
fn train(
    namespace: &str,
    train_id: &str,
    date: NaiveDateRocket,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (trains, locations, schedule_desc) = {
        let schedule_manager = schedule_manager.read();
        let schedule = &schedule_manager.get(namespace)?;
        let train = schedule.trains.get(train_id)?;
        (
            train.clone(),
            schedule.locations.clone(),
            schedule.description.clone(),
        )
    };

    let date = date.0;

    let (final_train, cancelled, modified) = get_train_instance(&trains, date);

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
        add_associated_trains(
            &mut associations,
            &location.divides_to_form,
            date,
            &location.id,
            &location.id_suffix,
            AssociationCategory::Divide,
        );
        add_associated_trains(
            &mut associations,
            &location.joins_to,
            date,
            &location.id,
            &location.id_suffix,
            AssociationCategory::Join,
        );
        add_associated_trains(
            &mut associations,
            &location.divides_from,
            date,
            &location.id,
            &location.id_suffix,
            AssociationCategory::DividesFrom,
        );
        add_associated_trains(
            &mut associations,
            &location.is_joined_to_by,
            date,
            &location.id,
            &location.id_suffix,
            AssociationCategory::IsJoinedToBy,
        );
        match &location.becomes {
            Some(x) => add_associated_train(
                &mut associations,
                &x,
                date,
                &location.id,
                &location.id_suffix,
                AssociationCategory::Next,
            ),
            None => (),
        }
        match &location.forms_from {
            Some(x) => add_associated_train(
                &mut associations,
                &x,
                date,
                &location.id,
                &location.id_suffix,
                AssociationCategory::FormsFrom,
            ),
            None => (),
        }
    }

    let mut assoc_train_details: HashMap<String, Vec<BasicAssocTrainDetails>> = HashMap::new();
    for (train_id, day_diff, is_public, location_id, location_suffix, category) in &associations {
        let trains = {
            let schedule_manager = schedule_manager.read();
            schedule_manager
                .get(namespace)
                .unwrap()
                .trains
                .get(train_id)?
                .clone()
        };
        let other_date = if *day_diff >= 0 {
            date.add(Days::new(u64::try_from(*day_diff).unwrap()))
        } else {
            date.sub(Days::new(u64::try_from(-*day_diff).unwrap()))
        };
        let train = get_train_instance(&trains, other_date).0?;
        assoc_train_details
            .entry(location_id.clone() + "|" + &location_suffix.as_ref().unwrap_or(&"".to_string()))
            .or_insert(vec![])
            .push(BasicAssocTrainDetails {
                id: train.id.clone(),
                public_id: train.variable_train.public_id.clone(),
                origin_id: train.route.first().unwrap().id.clone(),
                destination_id: train.route.last().unwrap().id.clone(),
                date: other_date.clone(),
                namespace: namespace.to_string(),
                is_public: *is_public,
                category: *category,
                name: train.variable_train.name.clone(),
                dep_time: if train.route[0].public_dep.is_none() {
                    convert_tz(
                        &other_date,
                        &Some(0),
                        &train.route[0].working_dep,
                        &train.route[0].timing_tz,
                        &locations.get(location_id).unwrap().timezone,
                    )
                    .ok()?
                    .unwrap()
                } else {
                    convert_tz(
                        &other_date,
                        &Some(0),
                        &train.route[0].public_dep,
                        &train.route[0].timing_tz,
                        &locations.get(location_id).unwrap().timezone,
                    )
                    .ok()?
                    .unwrap()
                },
            });
    }

    let mut dates = vec![];
    for extra_days in 0..(max(
        train.route.last().unwrap().working_arr_day,
        train.route.last().unwrap().public_arr_day,
    )
    .unwrap()
        + 1)
    {
        dates.push(date.add(Days::new(extra_days.into())));
    }

    // now convert all the timezones of all the stops
    for location in train.route.iter_mut() {
        location.working_arr = convert_tz(
            &date,
            &location.working_arr_day,
            &location.working_arr,
            &location.timing_tz,
            &locations.get(&location.id).unwrap().timezone,
        )
        .ok()?;
        location.working_dep = convert_tz(
            &date,
            &location.working_dep_day,
            &location.working_dep,
            &location.timing_tz,
            &locations.get(&location.id).unwrap().timezone,
        )
        .ok()?;
        location.working_pass = convert_tz(
            &date,
            &location.working_pass_day,
            &location.working_pass,
            &location.timing_tz,
            &locations.get(&location.id).unwrap().timezone,
        )
        .ok()?;
        location.public_arr = convert_tz(
            &date,
            &location.public_arr_day,
            &location.public_arr,
            &location.timing_tz,
            &locations.get(&location.id).unwrap().timezone,
        )
        .ok()?;
        location.public_dep = convert_tz(
            &date,
            &location.public_dep_day,
            &location.public_dep,
            &location.timing_tz,
            &locations.get(&location.id).unwrap().timezone,
        )
        .ok()?;
    }

    let context = context! {
        train,
        locations,
        cancelled,
        modified,
        namespace: namespace.to_string(),
        dates,
        schedule_desc,
        assoc_train_details,
    };

    Some(Template::render("train", &context))
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
    operator: Option<TrainOperator>,
    name: Option<String>,
    namespace: String,
    date: NaiveDate,
    is_first: bool,
    is_last: bool,
    cur_found_tos: usize,
}

fn get_origins(
    i: usize,
    location: &TrainLocation,
    schedule_manager: Arc<ScheduleManager>,
    date: NaiveDate,
    namespace: &str,
) -> Vec<String> {
    let mut origins = vec![];

    if i == 0 {
        let mut found_origin = false;
        for assoc in &location.divides_from {
            let final_assoc = match get_association(assoc, date) {
                Some(x) => x,
                None => continue,
            };

            let trains = {
                let schedule_manager = schedule_manager.read();
                match schedule_manager
                    .get(namespace)
                    .unwrap()
                    .trains
                    .get(&final_assoc.other_train_id)
                {
                    Some(x) => x.clone(),
                    None => continue,
                }
            };

            let other_date = if final_assoc.day_diff >= 0 {
                date.add(Days::new(u64::try_from(final_assoc.day_diff).unwrap()))
            } else {
                date.sub(Days::new(u64::try_from(-final_assoc.day_diff).unwrap()))
            };
            let (train, cancelled, _modified) = get_train_instance(&trains, other_date);

            if cancelled || train.is_none() {
                continue;
            }

            found_origin = true;

            for (i, other_location) in train.as_ref().unwrap().route.iter().enumerate() {
                if location.id == other_location.id
                    && assoc.other_train_location_id_suffix == other_location.id_suffix
                {
                    break;
                }

                origins.append(&mut get_origins(
                    i,
                    other_location,
                    schedule_manager.clone(),
                    other_date,
                    namespace,
                ));
            }
        }
        if !found_origin {
            origins.push(location.id.clone());
        }
    }

    for assoc in &location.joins_to {
        let final_assoc = match get_association(&assoc, date) {
            Some(x) => x,
            None => continue,
        };

        let trains = {
            let schedule_manager = schedule_manager.read();
            match schedule_manager
                .get(namespace)
                .unwrap()
                .trains
                .get(&final_assoc.other_train_id)
            {
                Some(x) => x.clone(),
                None => continue,
            }
        };

        let other_date = if final_assoc.day_diff >= 0 {
            date.add(Days::new(u64::try_from(final_assoc.day_diff).unwrap()))
        } else {
            date.sub(Days::new(u64::try_from(-final_assoc.day_diff).unwrap()))
        };
        let (train, cancelled, _modified) = get_train_instance(&trains, other_date);

        if cancelled || train.is_none() {
            continue;
        }

        for (i, other_location) in train.as_ref().unwrap().route.iter().enumerate() {
            if location.id == other_location.id
                && assoc.other_train_location_id_suffix == other_location.id_suffix
            {
                break;
            }

            origins.append(&mut get_origins(
                i,
                other_location,
                schedule_manager.clone(),
                other_date,
                namespace,
            ));
        }
    }

    origins
}

fn get_destinations(
    i: usize,
    length: usize,
    location: &TrainLocation,
    schedule_manager: Arc<ScheduleManager>,
    date: NaiveDate,
    namespace: &str,
) -> Vec<String> {
    let mut destinations = vec![];

    if i == length - 1 {
        let mut found_destination = false;
        for assoc in &location.is_joined_to_by {
            let final_assoc = match get_association(assoc, date) {
                Some(x) => x,
                None => continue,
            };

            let trains = {
                let schedule_manager = schedule_manager.read();
                match schedule_manager
                    .get(namespace)
                    .unwrap()
                    .trains
                    .get(&final_assoc.other_train_id)
                {
                    Some(x) => x.clone(),
                    None => continue,
                }
            };

            let other_date = if final_assoc.day_diff >= 0 {
                date.add(Days::new(u64::try_from(final_assoc.day_diff).unwrap()))
            } else {
                date.sub(Days::new(u64::try_from(-final_assoc.day_diff).unwrap()))
            };
            let (train, cancelled, _modified) = get_train_instance(&trains, other_date);

            if cancelled || train.is_none() {
                continue;
            }

            found_destination = true;

            let mut found = false;

            for (i, other_location) in train.as_ref().unwrap().route.iter().enumerate() {
                if location.id == other_location.id
                    && assoc.other_train_location_id_suffix == other_location.id_suffix
                {
                    found = true;
                    continue;
                }

                if !found {
                    continue;
                }

                destinations.append(&mut get_destinations(
                    i,
                    train.as_ref().unwrap().route.len(),
                    other_location,
                    schedule_manager.clone(),
                    other_date,
                    namespace,
                ));
            }
        }
        if !found_destination {
            destinations.push(location.id.clone());
        }
    }

    for assoc in &location.divides_to_form {
        let final_assoc = match get_association(assoc, date) {
            Some(x) => x,
            None => continue,
        };

        let trains = {
            let schedule_manager = schedule_manager.read();
            match schedule_manager
                .get(namespace)
                .unwrap()
                .trains
                .get(&final_assoc.other_train_id)
            {
                Some(x) => x.clone(),
                None => continue,
            }
        };

        let other_date = if final_assoc.day_diff >= 0 {
            date.add(Days::new(u64::try_from(final_assoc.day_diff).unwrap()))
        } else {
            date.sub(Days::new(u64::try_from(-final_assoc.day_diff).unwrap()))
        };
        let (train, cancelled, _modified) = get_train_instance(&trains, other_date);

        if cancelled || train.is_none() {
            continue;
        }

        let mut found = false;
        for (i, other_location) in train.as_ref().unwrap().route.iter().enumerate() {
            if location.id == other_location.id
                && assoc.other_train_location_id_suffix == other_location.id_suffix
            {
                found = true;
                continue;
            }

            if !found {
                continue;
            }

            destinations.append(&mut get_destinations(
                i,
                train.as_ref().unwrap().route.len(),
                other_location,
                schedule_manager.clone(),
                other_date,
                namespace,
            ));
        }
    }

    destinations
}

fn location_line_up(
    namespace: &str,
    location_ids: &HashSet<String>,
    start_datetime: NaiveDateTime,
    end_datetime: NaiveDateTime,
    from_station: Option<HashSet<String>>,
    to_station: Option<HashSet<String>>,
    schedule_manager: Arc<ScheduleManager>,
) -> Option<Template> {
    let (trains, locations) = {
        let schedule_manager = schedule_manager.read();
        let schedule = &schedule_manager.get(namespace)?;
        let mut trains = vec![];
        for location_id in location_ids {
            if !schedule.locations.contains_key(location_id) {
                return None;
            }
            for train_id in schedule
                .trains_indexed_by_location
                .get(location_id)
                .unwrap_or(&HashSet::new())
            {
                let train = schedule.trains.get(train_id)?;
                trains.push(train.clone());
            }
        }
        (trains, schedule.locations.clone())
    };

    let mut actual_trains = vec![];
    for train in trains {
        // OK, this is somewhat hacky but I haven't yet thought of a better way.
        if train.len() == 0 {
            // deleted trains remain in map
            continue;
        }
        let last_location = &train[0].route.last().unwrap();
        let max_day_offset = if last_location.working_arr_day.is_none() {
            last_location.public_arr_day.unwrap()
        } else {
            last_location.working_arr_day.unwrap()
        } + 1;

        let first_date = start_datetime.date().sub(Days::new(max_day_offset.into()));
        let end_date = end_datetime.date().add(Days::new(1)); // one past the end
        let mut cur_date = first_date;

        while cur_date != end_date {
            let (train, cancelled, modified) = match get_train_instance(&train, cur_date) {
                (Some(x), y, z) => (x, y, z),
                _ => {
                    cur_date = cur_date.add(Days::new(1));
                    continue;
                }
            };

            let mut additions_for_this_train: Vec<BasicTrainForLocation> = vec![];
            let mut origins_so_far = vec![];
            let mut variable_train = &train.variable_train;
            let mut found_from = match from_station {
                Some(_) => false,
                None => true,
            };
            let mut just_found_from = false;
            let mut cur_found_tos = 0;
            for (i, location) in train.route.iter().enumerate() {
                if just_found_from {
                    found_from = true;
                    just_found_from = false;
                }

                if location.change_en_route.is_some() {
                    variable_train = &location.change_en_route.as_ref().unwrap();
                }

                if !found_from {
                    just_found_from = from_station.as_ref().unwrap().contains(&location.id);
                }
                if to_station.is_some() {
                    if to_station.as_ref().unwrap().contains(&location.id) {
                        cur_found_tos += 1;
                    }
                }

                origins_so_far.append(&mut get_origins(
                    i,
                    &location,
                    schedule_manager.clone(),
                    cur_date,
                    namespace,
                ));

                let destinations = get_destinations(
                    i,
                    train.route.len(),
                    &location,
                    schedule_manager.clone(),
                    cur_date,
                    namespace,
                );

                for addition in &mut additions_for_this_train {
                    addition.destinations.append(&mut destinations.clone());
                }

                if !location_ids.contains(&location.id) {
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
                let time_from_cur_date = cur_date
                    .add(Days::new(best_offset.into()))
                    .and_time(best_time);
                if time_from_cur_date < start_datetime || time_from_cur_date > end_datetime {
                    continue;
                }

                // special case: add this station as destination if we are in the last iteration
                let starting_destinations = if i == train.route.len() - 1 {
                    let mut dests = vec![];
                    dests.push(location.id.clone());
                    dests
                } else {
                    vec![]
                };

                additions_for_this_train.push(BasicTrainForLocation {
                    id: train.id.clone(),
                    public_id: variable_train.public_id.clone(),
                    origins: origins_so_far.clone(),
                    destinations: starting_destinations,
                    working_arr: match location.working_arr {
                        None => None,
                        Some(x) => Some(
                            cur_date
                                .add(Days::new(location.working_arr_day.unwrap().into()))
                                .and_time(x),
                        ),
                    },
                    working_dep: match location.working_dep {
                        None => None,
                        Some(x) => Some(
                            cur_date
                                .add(Days::new(location.working_dep_day.unwrap().into()))
                                .and_time(x),
                        ),
                    },
                    working_pass: match location.working_pass {
                        None => None,
                        Some(x) => Some(
                            cur_date
                                .add(Days::new(location.working_pass_day.unwrap().into()))
                                .and_time(x),
                        ),
                    },
                    public_arr: match location.public_arr {
                        None => None,
                        Some(x) => Some(
                            cur_date
                                .add(Days::new(location.public_arr_day.unwrap().into()))
                                .and_time(x),
                        ),
                    },
                    public_dep: match location.public_dep {
                        None => None,
                        Some(x) => Some(
                            cur_date
                                .add(Days::new(location.public_dep_day.unwrap().into()))
                                .and_time(x),
                        ),
                    },
                    platform: location.platform.clone(),
                    platform_zone: location.platform_zone.clone(),
                    modified,
                    cancelled,
                    source: train.source,
                    runs_as_required: train.runs_as_required,
                    operator: variable_train.operator.clone(),
                    name: variable_train.name.clone(),
                    namespace: namespace.to_string(),
                    date: cur_date,
                    is_first: i == 0,
                    is_last: i == train.route.len() - 1,
                    cur_found_tos,
                });
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
        if train.working_dep.is_some() {
            train.working_dep
        } else if train.public_dep.is_some() {
            train.public_dep
        } else if train.working_pass.is_some() {
            train.working_pass
        } else if train.working_arr.is_some() {
            train.working_arr
        } else if train.public_arr.is_some() {
            train.public_arr
        } else {
            return None;
        }
    });

    let context = context! {
        actual_trains,
        locations,
        location_id: location_ids.iter().next().unwrap(),
        namespace: namespace.to_string(),
    };

    Some(Template::render("location", &context))
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

fn get_location_ids_and_first_tz(
    location_id: &str,
    namespace: &Namespace,
    schedule_manager: Arc<ScheduleManager>,
) -> Option<(HashSet<String>, Tz)> {
    let schedule_manager = schedule_manager.read();
    let schedule = &schedule_manager.get(&namespace.namespace)?;
    match namespace.is_public_id {
        true => {
            let locations = schedule.locations_indexed_by_public_id.get(location_id)?;
            if locations.len() == 0 {
                return None;
            }
            Some((
                locations.clone(),
                schedule
                    .locations
                    .get(locations.iter().next().unwrap())
                    .unwrap()
                    .timezone
                    .clone(),
            ))
        }
        false => {
            let mut location_ids = HashSet::new();
            location_ids.insert(location_id.to_string());
            let location = match schedule.locations.get(location_id) {
                Some(x) => x,
                None => {
                    return None
                },
            };
            Some((location_ids, location.timezone))
        }
    }
}

#[get("/location/<namespace>/<location_id>")]
fn location(
    namespace: Namespace,
    location_id: &str,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (location_ids, timezone) =
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone())?;

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
    )
}

#[get("/location/<namespace>/<location_id>/from/<from_id>", rank = 0)]
fn location_from(
    namespace: Namespace,
    location_id: &str,
    from_id: &str,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (location_ids, timezone) =
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone())?;

    let now = timezone
        .from_utc_datetime(&Utc::now().naive_utc())
        .naive_local();

    let (from_ids, _timezone) =
        get_location_ids_and_first_tz(from_id, &namespace, (*schedule_manager).clone())?;

    location_line_up(
        &namespace.namespace,
        &location_ids,
        now - Duration::minutes(30),
        now + Duration::minutes(120),
        Some(from_ids),
        None,
        (*schedule_manager).clone(),
    )
}

#[get("/location/<namespace>/<location_id>/to/<to_id>", rank = 0)]
fn location_to(
    namespace: Namespace,
    location_id: &str,
    to_id: &str,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (location_ids, timezone) =
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone())?;

    let now = timezone
        .from_utc_datetime(&Utc::now().naive_utc())
        .naive_local();

    let (to_ids, _timezone) =
        get_location_ids_and_first_tz(to_id, &namespace, (*schedule_manager).clone())?;

    location_line_up(
        &namespace.namespace,
        &location_ids,
        now - Duration::minutes(30),
        now + Duration::minutes(120),
        None,
        Some(to_ids),
        (*schedule_manager).clone(),
    )
}

#[get(
    "/location/<namespace>/<location_id>/from/<from_id>/to/<to_id>",
    rank = 0
)]
fn location_from_to(
    namespace: Namespace,
    location_id: &str,
    from_id: &str,
    to_id: &str,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (location_ids, timezone) =
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone())?;

    let now = timezone
        .from_utc_datetime(&Utc::now().naive_utc())
        .naive_local();

    let (from_ids, _timezone) =
        get_location_ids_and_first_tz(from_id, &namespace, (*schedule_manager).clone())?;
    let (to_ids, _timezone) =
        get_location_ids_and_first_tz(to_id, &namespace, (*schedule_manager).clone())?;

    location_line_up(
        &namespace.namespace,
        &location_ids,
        now - Duration::minutes(30),
        now + Duration::minutes(120),
        Some(from_ids),
        Some(to_ids),
        (*schedule_manager).clone(),
    )
}

#[get("/location/<namespace>/<location_id>/<date>/<time>", rank = 1)]
fn location_time(
    namespace: Namespace,
    location_id: &str,
    date: NaiveDateRocket,
    time: NaiveTimeRocket,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (location_ids, _timezone) =
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone())?;

    location_line_up(
        &namespace.namespace,
        &location_ids,
        date.0.and_time(time.0) - Duration::minutes(30),
        date.0.and_time(time.0) + Duration::minutes(120),
        None,
        None,
        (*schedule_manager).clone(),
    )
}

#[get(
    "/location/<namespace>/<location_id>/from/<from_id>/<date>/<time>",
    rank = 1
)]
fn location_from_time(
    namespace: Namespace,
    location_id: &str,
    from_id: &str,
    date: NaiveDateRocket,
    time: NaiveTimeRocket,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (location_ids, _timezone) =
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone())?;

    let (from_ids, _timezone) =
        get_location_ids_and_first_tz(from_id, &namespace, (*schedule_manager).clone())?;

    location_line_up(
        &namespace.namespace,
        &location_ids,
        date.0.and_time(time.0) - Duration::minutes(30),
        date.0.and_time(time.0) + Duration::minutes(120),
        Some(from_ids),
        None,
        (*schedule_manager).clone(),
    )
}

#[get(
    "/location/<namespace>/<location_id>/to/<to_id>/<date>/<time>",
    rank = 1
)]
fn location_to_time(
    namespace: Namespace,
    location_id: &str,
    to_id: &str,
    date: NaiveDateRocket,
    time: NaiveTimeRocket,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (location_ids, _timezone) =
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone())?;

    let (to_ids, _timezone) =
        get_location_ids_and_first_tz(to_id, &namespace, (*schedule_manager).clone())?;

    location_line_up(
        &namespace.namespace,
        &location_ids,
        date.0.and_time(time.0) - Duration::minutes(30),
        date.0.and_time(time.0) + Duration::minutes(120),
        None,
        Some(to_ids),
        (*schedule_manager).clone(),
    )
}

#[get(
    "/location/<namespace>/<location_id>/from/<from_id>/to/<to_id>/<date>/<time>",
    rank = 1
)]
fn location_from_to_time(
    namespace: Namespace,
    location_id: &str,
    from_id: &str,
    to_id: &str,
    date: NaiveDateRocket,
    time: NaiveTimeRocket,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (location_ids, _timezone) =
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone())?;

    let (from_ids, _timezone) =
        get_location_ids_and_first_tz(from_id, &namespace, (*schedule_manager).clone())?;
    let (to_ids, _timezone) =
        get_location_ids_and_first_tz(to_id, &namespace, (*schedule_manager).clone())?;

    location_line_up(
        &namespace.namespace,
        &location_ids,
        date.0.and_time(time.0) - Duration::minutes(30),
        date.0.and_time(time.0) + Duration::minutes(120),
        Some(from_ids),
        Some(to_ids),
        (*schedule_manager).clone(),
    )
}

#[get(
    "/location/<namespace>/<location_id>/<date>/<from_time>/to/<to_time>",
    rank = 2
)]
fn location_time_to(
    namespace: Namespace,
    location_id: &str,
    date: NaiveDateRocket,
    from_time: NaiveTimeRocket,
    to_time: NaiveTimeRocket,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (location_ids, _timezone) =
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone())?;

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
    )
}

#[get(
    "/location/<namespace>/<location_id>/from/<from_id>/<date>/<from_time>/to/<to_time>",
    rank = 2
)]
fn location_from_time_to(
    namespace: Namespace,
    location_id: &str,
    from_id: &str,
    date: NaiveDateRocket,
    from_time: NaiveTimeRocket,
    to_time: NaiveTimeRocket,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (location_ids, _timezone) =
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone())?;

    let to_date = if to_time.0 < from_time.0 {
        date.0 + Days::new(1)
    } else {
        date.0
    };

    let (from_ids, _timezone) =
        get_location_ids_and_first_tz(from_id, &namespace, (*schedule_manager).clone())?;

    location_line_up(
        &namespace.namespace,
        &location_ids,
        date.0.and_time(from_time.0),
        to_date.and_time(to_time.0),
        Some(from_ids),
        None,
        (*schedule_manager).clone(),
    )
}

#[get(
    "/location/<namespace>/<location_id>/to/<to_id>/<date>/<from_time>/to/<to_time>",
    rank = 2
)]
fn location_to_time_to(
    namespace: Namespace,
    location_id: &str,
    to_id: &str,
    date: NaiveDateRocket,
    from_time: NaiveTimeRocket,
    to_time: NaiveTimeRocket,
    schedule_manager: &State<Arc<ScheduleManager>>,
) -> Option<Template> {
    let (location_ids, _timezone) =
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone())?;

    let to_date = if to_time.0 < from_time.0 {
        date.0 + Days::new(1)
    } else {
        date.0
    };

    let (to_ids, _timezone) =
        get_location_ids_and_first_tz(to_id, &namespace, (*schedule_manager).clone())?;

    location_line_up(
        &namespace.namespace,
        &location_ids,
        date.0.and_time(from_time.0),
        to_date.and_time(to_time.0),
        None,
        Some(to_ids),
        (*schedule_manager).clone(),
    )
}

#[get(
    "/location/<namespace>/<location_id>/from/<from_id>/to/<to_id>/<date>/<from_time>/to/<to_time>",
    rank = 2
)]
fn location_from_to_time_to(
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
        get_location_ids_and_first_tz(location_id, &namespace, (*schedule_manager).clone())?;

    let to_date = if to_time.0 < from_time.0 {
        date.0 + Days::new(1)
    } else {
        date.0
    };

    let (from_ids, _timezone) =
        get_location_ids_and_first_tz(from_id, &namespace, (*schedule_manager).clone())?;
    let (to_ids, _timezone) =
        get_location_ids_and_first_tz(to_id, &namespace, (*schedule_manager).clone())?;

    location_line_up(
        &namespace.namespace,
        &location_ids,
        date.0.and_time(from_time.0),
        to_date.and_time(to_time.0),
        Some(from_ids),
        Some(to_ids),
        (*schedule_manager).clone(),
    )
}

pub async fn rocket(schedule_manager: Arc<ScheduleManager>) -> Result<(), Error> {
    rocket::build()
        .mount(
            "/",
            routes![
                index,
                train,
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
        .attach(Template::fairing())
        .manage(schedule_manager)
        .launch()
        .await?;

    Err(Error::WebUiError(WebUiError {
        what: "Shutdown requested".to_string(),
    }))
}
