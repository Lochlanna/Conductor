use std::collections::HashMap;

use rocket::serde::{json::Json, msgpack::MsgPack, Deserialize, Serialize};

use uuid::Uuid;

use crate::db;

#[derive(Debug, Clone, Deserialize, Serialize)]
enum DataTypes {
    Int,
    Float,
    Binary,
    Time,
    String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
enum ErrorCode {
    NoError = 0,
    TimestampDefined = 1,
    NoMembers = 2,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Registration {
    name: String,
    schema: HashMap<String, DataTypes>,
}

impl Registration {
    fn schema_valid(&self) -> ErrorCode {
        if self.schema.contains_key("timestamp") {
            return ErrorCode::TimestampDefined;
        }
        if self.schema.is_empty() {
            return ErrorCode::NoMembers;
        }
        ErrorCode::NoError
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RegistrationResult {
    error: u8,
    uuid: Option<Uuid>,
}
fn register(_conn: &db::QuestDbConn, data: &Registration) -> RegistrationResult {
    let error = data.schema_valid();
    match error {
        ErrorCode::NoError => {
            let uuid = Some(Uuid::new_v4());
            RegistrationResult {
                error: error as u8,
                uuid,
            }
        }
        _ => RegistrationResult {
            error: error as u8,
            uuid: None,
        },
    }
}

#[post("/producer/register", format = "msgpack", data = "<data>")]
pub async fn register_pack(
    conn: db::QuestDbConn,
    data: MsgPack<Registration>,
) -> MsgPack<RegistrationResult> {
    MsgPack(register(&conn, &data))
}

#[post("/producer/register", format = "json", data = "<data>")]
pub async fn register_json(
    conn: db::QuestDbConn,
    data: Json<Registration>,
) -> Json<RegistrationResult> {
    Json(register(&conn, &data))
}
