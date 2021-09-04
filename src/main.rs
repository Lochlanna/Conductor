use log::LevelFilter;
use simple_logger::SimpleLogger;
mod db;
mod producer;

#[macro_use]
extern crate rocket;

#[launch]
fn rocket() -> _ {
    SimpleLogger::new()
        .with_level(LevelFilter::Debug)
        .init()
        .unwrap();
    rocket::build()
        .mount(
            "/",
            routes![producer::register_json, producer::register_pack],
        )
        .attach(db::QuestDbConn::fairing())
}
