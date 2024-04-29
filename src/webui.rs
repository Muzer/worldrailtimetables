use crate::error::Error;
use crate::schedule_manager::ScheduleManager;

use rocket::{get, routes, State};
use rocket_dyn_templates::{context, Template};

use std::sync::Arc;

#[get("/")]
fn index(schedule_manager: &State<Arc<ScheduleManager>>) -> Template {
    let namespaces = {
        let schedule_manager = schedule_manager.read();
        schedule_manager.keys().cloned().collect::<Vec<String>>()
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

    Ok(())
}
