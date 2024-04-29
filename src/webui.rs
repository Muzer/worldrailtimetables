use crate::error::Error;
use crate::schedule_manager::ScheduleManager;

use rocket::{get, routes, State};
use rocket_dyn_templates::{context, Template};

use std::collections::HashMap;
use std::fmt;
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

pub async fn rocket(schedule_manager: Arc<ScheduleManager>) -> Result<(), Error> {
    rocket::build()
        .mount("/", routes![index])
        .attach(Template::fairing())
        .manage(schedule_manager)
        .launch()
        .await?;

    Err(Error::WebUiError(WebUiError { what: "Shutdown requested".to_string() }))
}
