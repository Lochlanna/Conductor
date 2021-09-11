use log::LevelFilter;
use rocket::fairing::AdHoc;
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
            routes![
                producer::register_json,
                producer::register_pack,
                producer::emit_json,
                producer::emit_pack
            ],
        )
        .attach(db::QuestDbConn::fairing())
        .attach(AdHoc::on_ignite(
            "Creat application tables",
            db::create_app_schema,
        ))
}
