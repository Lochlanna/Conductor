use rocket::serde::{json::Json, msgpack::MsgPack, Deserialize, Serialize};

use crate::db;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProducerMetaData {
    name: String,
    data_type: String,
    unit: String,
    update_rate: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProducerRegistration {
    name: String,
    data_type: String,
    unit: String,
    update_rate: f64,
}
fn register_producer(_conn: &db::QuestDbConn, data: &ProducerMetaData) -> ProducerRegistration {
    ProducerRegistration {
        name: data.name.clone(),
        data_type: data.data_type.clone(),
        unit: data.unit.clone(),
        update_rate: data.update_rate,
    }
}

#[post("/register/producer", format = "msgpack", data = "<data>")]
pub async fn register_producer_pack(
    conn: db::QuestDbConn,
    data: MsgPack<ProducerMetaData>,
) -> MsgPack<ProducerRegistration> {
    MsgPack(register_producer(&conn, &data))
}

#[post("/register/producer", format = "json", data = "<data>")]
pub async fn register_producer_json(
    conn: db::QuestDbConn,
    data: Json<ProducerMetaData>,
) -> Json<ProducerRegistration> {
    Json(register_producer(&conn, &data))
}
