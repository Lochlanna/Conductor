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
    Bool,
    Char,
    Long,
    Double,
}

impl DataTypes {
    fn to_quest_type(&self) -> &str {
        match self {
            DataTypes::Int => "int",
            DataTypes::Float => "float",
            DataTypes::Binary => "binary",
            DataTypes::Time => "timestamp",
            DataTypes::String => "string",
            DataTypes::Bool => "boolean",
            DataTypes::Char => "char",
            DataTypes::Long => "long",
            DataTypes::Double => "double",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
enum ErrorCode {
    NoError = 0,
    TimestampDefined = 1,
    NoMembers = 2,
    InvalidColumnNames = 3,
    TooManyColumns = 4, // who is doing this???
    InternalError = 5,
    InvalidCustomId = 6,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Registration {
    name: String,
    schema: HashMap<String, DataTypes>,
    use_custom_id: Option<String>, // this is to support devices without persistant storage such as an arduino. They can have a custom id
}

macro_rules! logErrorWithJson {
    ($self:ident, $($args:tt)+) => {{
        match serde_json::to_string($self) {
            Ok(json) => log::error!("{} JSON = \n{}", format_args!($($args)*), json),
            Err(error) => log::error!("{} JSON couldn't be produced: {}", format_args!($($args)*), error),
        }
    }};
}

impl Registration {
    fn schema_valid(&self) -> ErrorCode {
        if self.schema.contains_key("ts") {
            logErrorWithJson!(
                self,
                "Producer registration failed. column with name ts. This is a resereved name."
            );
            return ErrorCode::TimestampDefined;
        }
        if self.schema.is_empty() {
            logErrorWithJson!(self, "Producer registration failed. No columns in schema.");
            return ErrorCode::NoMembers;
        }
        for col in self.schema.keys() {
            if col.contains('.') || col.contains('\"') {
                logErrorWithJson!(self, "Producer registration failed. Column with name {} is invalid as it contains a '.' or a '\"'.", col);
                return ErrorCode::InvalidColumnNames;
            }
        }
        if self.schema.len() > 2147483647 {
            //I mean this is invalid. But seriously how did we get here
            logErrorWithJson!(self, "Producer schema registration had {} columns which is more than the maximum quest can support of 2,147,483,647.", self.schema.len());
            return ErrorCode::TooManyColumns;
        }
        //TODO validate that there is no sql inside the column names!

        ErrorCode::NoError
    }

    fn generate_table_sql(&self, table_name: &str) -> String {
        //     CREATE TABLE my_table(symb SYMBOL, price DOUBLE, ts TIMESTAMP, s STRING) timestamp(ts);
        let mut sql = format! {"CREATE TABLE IF NOT EXISTS \"{}\" (ts TIMESTAMP", table_name};
        for (col_name, col_type) in &self.schema {
            sql = sql + ", \"" + col_name + "\" " + col_type.to_quest_type();
        }
        sql += ") timestamp(ts);";
        sql
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RegistrationResult {
    error: u8,
    uuid: Option<String>,
}

async fn register(db: &db::QuestDbConn, data: &Registration) -> RegistrationResult {
    match data.schema_valid() {
        ErrorCode::NoError => {
            let uuid;
            if data.use_custom_id.is_some() {
                uuid = data
                    .use_custom_id
                    .as_ref()
                    .expect("Couldn't unwrap value that has already been checked as not none...")
                    .clone();
                if uuid.is_empty() {
                    return RegistrationResult {
                        error: ErrorCode::InvalidCustomId as u8,
                        uuid: None,
                    };
                }
            } else {
                uuid = Uuid::new_v4().to_string();
            }
            let sql = data.generate_table_sql(&(uuid.to_string()));
            let name = data.name.clone();
            let schema = serde_json::to_string(&data.schema).unwrap();
            let uuid_copy = uuid.clone();
            let result: Result<u64, _> = db
                .run(move |conn: &mut postgres::Client| {
                    log::info!("creating table with sql {}", sql);
                    let result = conn.execute(sql.as_str(), &[]);
                    if result.is_err() {
                        return result;
                    }
                    conn.execute(
                        "INSERT INTO producers VALUES($1, $2, $3);",
                        &[&name, &uuid_copy, &schema],
                    )
                })
                .await;
            match result {
                Ok(_) => RegistrationResult {
                    error: data.schema_valid() as u8,
                    uuid: Some(uuid),
                },
                Err(error) => {
                    log::error!("There was an issue creating producer data store: {}", error);
                    RegistrationResult {
                        error: ErrorCode::InternalError as u8,
                        uuid: None,
                    }
                }
            }
        }
        _ => RegistrationResult {
            error: data.schema_valid() as u8,
            uuid: None,
        },
    }
}

#[post("/producer/register", format = "msgpack", data = "<data>")]
pub async fn register_pack(
    conn: db::QuestDbConn,
    data: MsgPack<Registration>,
) -> MsgPack<RegistrationResult> {
    MsgPack(register(&conn, &data).await)
}

#[post("/producer/register", format = "json", data = "<data>")]
pub async fn register_json(
    conn: db::QuestDbConn,
    data: Json<Registration>,
) -> Json<RegistrationResult> {
    Json(register(&conn, &data).await)
}
