use std::fmt;

use log::LevelFilter;
use simple_logger::SimpleLogger;
mod nice_log;
use postgres::Client;

#[macro_use]
extern crate rocket;
use rocket::serde::{msgpack::MsgPack, Deserialize, Serialize};
use rocket_sync_db_pools::{database, postgres};

#[derive(Debug, Clone)]
struct ConductorError {}
impl fmt::Display for ConductorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "invalid first item to double")
    }
}
#[database("quest_db")]
struct QuestDbConn(postgres::Client);

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ProducerMetaData {
    name: String,
    data_type: String,
    unit: String,
    update_rate: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ProducerRegistration {
    name: String,
    data_type: String,
    unit: String,
    update_rate: f64,
}

#[post("/register/producer", format = "msgpack", data = "<data>")]
async fn register_producer(
    _conn: QuestDbConn,
    data: MsgPack<ProducerMetaData>,
) -> MsgPack<ProducerRegistration> {
    MsgPack(ProducerRegistration {
        name: data.name.clone(),
        data_type: data.data_type.clone(),
        unit: data.unit.clone(),
        update_rate: data.update_rate,
    })
}

#[launch]
fn rocket() -> _ {
    SimpleLogger::new()
        .with_level(LevelFilter::Debug)
        .init()
        .unwrap();
    rocket::build()
        .mount("/", routes![register_producer])
        .attach(QuestDbConn::fairing())
}
