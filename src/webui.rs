use chrono::naive::Days;
use chrono::{Datelike, NaiveDate, NaiveTime, ParseError};

use crate::error::Error;
use crate::schedule::{AssociationNode, Train};
use crate::schedule_manager::ScheduleManager;

use rocket::request::FromParam;
use rocket::{get, routes, State};
use rocket_dyn_templates::{context, Template};

use serde::Serialize;

use std::cmp::max;
use std::collections::HashMap;
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

fn get_train_instance(trains: &Vec<Train>, date: NaiveDate) -> (Option<Train>, bool, bool) {
    // let's make life easy and find the right train
    let mut final_train = None;
    let mut cancelled = false;
    let mut modified = false;
    for train in trains {
        for validity in &train.validity {
            if validity.valid_begin.date_naive() <= date
                && validity.valid_end.date_naive() >= date
                && train.days_of_week.get_by_weekday(date.weekday())
            {
                cancelled = false;
                modified = false;
                'replacement: for replacement in &train.replacements {
                    for validity in &replacement.validity {
                        if validity.valid_begin.date_naive() <= date
                            && validity.valid_end.date_naive() >= date
                            && train.days_of_week.get_by_weekday(date.weekday())
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
                for (cancellation, weekdays) in &train.cancellations {
                    if cancellation.valid_begin.date_naive() <= date
                        && cancellation.valid_end.date_naive() >= date
                        && weekdays.get_by_weekday(date.weekday())
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
    let mut final_assoc = None;
    let mut cancelled = false;
    for validity in &assoc.validity {
        if validity.valid_begin.date_naive() <= date
            && validity.valid_end.date_naive() >= date
            && assoc.days.get_by_weekday(date.weekday())
        {
            cancelled = false;
            'replacement: for replacement in &assoc.replacements {
                for validity in &replacement.validity {
                    if validity.valid_begin.date_naive() <= date
                        && validity.valid_end.date_naive() >= date
                        && assoc.days.get_by_weekday(date.weekday())
                    {
                        final_assoc = Some(replacement.clone());
                        break 'replacement;
                    }
                }
            }
            if final_assoc.is_none() {
                final_assoc = Some(assoc.clone());
            }
            for (cancellation, weekdays) in &assoc.cancellations {
                if cancellation.valid_begin.date_naive() <= date
                    && cancellation.valid_end.date_naive() >= date
                    && weekdays.get_by_weekday(date.weekday())
                {
                    cancelled = true;
                }
            }
        }
    }

    if final_assoc.is_none() || cancelled {
        return;
    }

    let final_assoc = final_assoc.unwrap();

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
        match &schedule_manager.get(namespace) {
            None => return None,
            Some(schedule) => match schedule.trains.get(train_id) {
                None => return None,
                Some(train) => (
                    train.clone(),
                    schedule.locations.clone(),
                    schedule.description.clone(),
                ),
            },
        }
    };

    let date = date.0;

    let (final_train, cancelled, modified) = get_train_instance(&trains, date);

    match final_train {
        Some(train) => {
            println!("{:#?}", train);
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
            println!("{:#?}", associations);

            let mut assoc_train_details: HashMap<String, Vec<BasicAssocTrainDetails>> =
                HashMap::new();
            for (train_id, day_diff, is_public, location_id, location_suffix, category) in
                &associations
            {
                let schedule_manager = schedule_manager.read();
                match schedule_manager
                    .get(namespace)
                    .unwrap()
                    .trains
                    .get(train_id)
                {
                    None => return None,
                    Some(trains) => {
                        let other_date = if *day_diff >= 0 {
                            date.add(Days::new(u64::try_from(*day_diff).unwrap()))
                        } else {
                            date.sub(Days::new(u64::try_from(-*day_diff).unwrap()))
                        };
                        match get_train_instance(&trains, other_date).0 {
                            None => return None,
                            Some(train) => {
                                assoc_train_details
                                    .entry(
                                        location_id.clone()
                                            + "|"
                                            + &location_suffix.as_ref().unwrap_or(&"".to_string()),
                                    )
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
                                            train.route[0].working_dep.unwrap()
                                        } else {
                                            train.route[0].public_dep.unwrap()
                                        },
                                    });
                            }
                        }
                    }
                }
            }
            println!("{:#?}", assoc_train_details);

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
        None => None,
    }
}

pub async fn rocket(schedule_manager: Arc<ScheduleManager>) -> Result<(), Error> {
    rocket::build()
        .mount("/", routes![index, train])
        .attach(Template::fairing())
        .manage(schedule_manager)
        .launch()
        .await?;

    Err(Error::WebUiError(WebUiError {
        what: "Shutdown requested".to_string(),
    }))
}
