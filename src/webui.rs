use chrono::naive::Days;
use chrono::{Datelike, NaiveDate, ParseError};

use crate::error::Error;
use crate::schedule_manager::ScheduleManager;

use rocket::request::FromParam;
use rocket::{get, routes, State};
use rocket_dyn_templates::{context, Template};

use std::cmp::max;
use std::collections::HashMap;
use std::fmt;
use std::ops::Add;
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

    // let's make life easy and find the right train
    let mut final_train = None;
    let mut cancelled = false;
    let mut modified = false;
    let date = date.0;
    for train in &trains {
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

    match final_train {
        Some(train) => {
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
