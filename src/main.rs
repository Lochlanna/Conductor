use log::LevelFilter;
use simple_logger::SimpleLogger;
mod db;
mod nice_log;
mod register;

#[macro_use]
extern crate rocket;

#[launch]
fn rocket() -> _ {
    SimpleLogger::new()
        .with_level(LevelFilter::Debug)
        .init()
        .unwrap();
    rocket::build()
        .mount("/", routes![register::register_producer_json, register::register_producer_pack])
        .attach(db::QuestDbConn::fairing())
}
