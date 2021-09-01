use log::LevelFilter;
use simple_logger::SimpleLogger;
mod nice_log;
use postgres::Client;

#[macro_use]
extern crate rocket;
use rocket_sync_db_pools::{database, postgres};

#[database("quest_db")]
struct QuestDbConn(postgres::Client);

#[get("/")]
async fn index(conn: QuestDbConn) -> String {
    let _ = conn.run(|c| Client::batch_execute(c, format!("CREATE TABLE IF NOT EXISTS trades_{} (ts TIMESTAMP, date DATE, name STRING, value INT) timestamp(ts);", 15).as_str())).await;
    format!("The config value is: {}", 3)
}

#[launch]
fn rocket() -> _ {
    SimpleLogger::new()
        .with_level(LevelFilter::Debug)
        .init()
        .unwrap();
    rocket::build()
        .mount("/", routes![index])
        .attach(QuestDbConn::fairing())
}
