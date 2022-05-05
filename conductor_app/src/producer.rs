use std::collections::HashMap;

use postgres::{types::ToSql, Row};
use rocket::http::Status;
use rocket::serde::{json::Json, msgpack::MsgPack, Deserialize, Serialize};
use uuid::Uuid;
use crate::db;
use conductor_common;
use conductor_common::producer as producer_com;
use conductor_common::schema as schema_com;
use conductor_common::error as error_com;

macro_rules! log_error_with_json {
    ($self:ident, $($args:tt)+) => {{
        match serde_json::to_string($self) {
            Ok(json) => log::error!("{} JSON = \n{}", format_args!($($args)*), json),
            Err(error) => log::error!("{} JSON couldn't be produced: {}", format_args!($($args)*), error),
        }
    }};
}

macro_rules! log_error_and_get_emit_result {
    ($errorCode:expr) => {{
        log::error!("{}", $errorCode);
        Err($errorCode)
    }};
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Producer {
    pub name: String,
    pub uuid: String,
    pub schema: String,
}

///
/// Converts json into a proper rust type. It does this using the registered schema to understand
/// the expected type of each field.
///
/// TODO Use proper errors here.
pub fn to_solid_type_from_json(
    val: &serde_json::Value,
    data_type: schema_com::DataTypes,
) -> Result<Box<dyn postgres::types::ToSql + Sync + Send>, String> {
    match data_type {
        schema_com::DataTypes::Int => match val.as_i64() {
            Some(v) => Ok(Box::new(v)),
            None => Err(format!(
                "Not possible to convert json value to i64. Value: {:?}",
                val
            )),
        },
        schema_com::DataTypes::Float => {
            match val.as_f64() {
                Some(v) => {
                    /*check that this will actually fit within an f32 bounds so the cast should? be safe.
                    use epsilon to make extra sure that this is an okay thing to do.
                    There could be a time when a valid f32 value is rejected due to the epsilon difference but if your data
                    is that close use a double type...*/
                    if v > f64::from(f32::MAX) - f64::from(f32::EPSILON) || v < f64::from(f32::MIN) + f64::from(f32::EPSILON) {
                        return Err(format!("Not possible to convert json value to f32 (too big to fit). Value: {:?}", val));
                    }
                    // It should be safe to cast this to an f32. It fits
                    #[allow(clippy::cast_possible_truncation)]
                        Ok(Box::new(v as f32))
                }
                None => Err(format!("Not possible to convert json value to f32 (Couldn't get f64 first). Value: {:?}", val)),
            }
        }
        schema_com::DataTypes::Time => match serde_json::from_value::<chrono::NaiveDateTime>(val.clone()) {
            Ok(v) => Ok(Box::new(v)),
            Err(_) => Err(format!(
                "Not possible to convert json value to naive date time. Value: {:?}",
                val
            )),
        },
        schema_com::DataTypes::String => match val.as_str() {
            Some(v) => Ok(Box::new(v.to_string())),
            None => Err(format!(
                "Not possible to convert json value to string. Value: {:?}",
                val
            )),
        },
        schema_com::DataTypes::Bool => match val.as_bool() {
            Some(v) => Ok(Box::new(v)),
            None => Err(format!(
                "Not possible to convert json value to bool. Value: {:?}",
                val
            )),
        },
        schema_com::DataTypes::Double => match val.as_f64() {
            Some(v) => Ok(Box::new(v)),
            None => Err(format!(
                "Not possible to convert json value to double. Value: {:?}",
                val
            )),
        },
        schema_com::DataTypes::Binary => match serde_json::from_value::<Vec<u8>>(val.clone()) {
            Ok(v) => Ok(Box::new(v)),
            Err(_) => Err(format!(
                "Not possible to convert json value to binary. Value: {:?}",
                val
            )),
        },
    }
}

///
/// Retrieves the registration row for a producer from the database based on it's uuid.
///
/// # Errors
/// * `ConductorError::InvalidUuid` : The uuid is empty
/// * `ConductorError::Unregistered` : The uuid doesn't exist in the database
/// * `ConductorError::InternalError` : There were multiple entries in the database for the given
/// uuid
/// * `ConductorError::InternalError` : The row couldn't be deserialized.
///
async fn get_producer_row(
    db: &db::QuestDbConn,
    #[allow(clippy::ptr_arg)]
    uuid: &str,
) -> Result<Producer, error_com::ConductorError> {
    if uuid.is_empty() {
        return log_error_and_get_emit_result!(
            error_com::ConductorError::InvalidUuid("Incoming request had an empty uuid".to_string())
        );
    }
    //check if the uuid is in the db
    let uuid_copy = uuid.to_string();
    let get_producer_row = move |conn: &mut postgres::Client| {
        conn.query("SELECT * FROM producers WHERE uuid = $1;", &[&uuid_copy])
    };
    let rows: Vec<Row> = match db.run(get_producer_row).await {
        Ok(rows) => rows,
        Err(error) => {
            return log_error_and_get_emit_result!(
                error_com::ConductorError::Unregistered(format!("Error getting producer from database {}",
                error))

            );
        }
    };
    if rows.is_empty() {
        return log_error_and_get_emit_result!(
            error_com::ConductorError::Unregistered(format!("Error getting producer. No rows returned for uuid: {}",
            &uuid))

        );
    }
    if rows.len() > 1 {
        //this shouldn't happen...
        return log_error_and_get_emit_result!(
            error_com::ConductorError::InternalError(format!("There were multiple entries for uuid: {}",
            &uuid))

        );
    }
    if let Some(row) = rows.get(0) {
        let producer = Producer {
            name: row.try_get("name").unwrap_or_default(),
            uuid: row.try_get("uuid").unwrap_or_default(),
            schema: row.try_get("schema").unwrap_or_default(),
        };
        let default_string = String::default();
        if producer.name == default_string
            || producer.uuid == default_string
            || producer.schema == default_string
        {
            return log_error_and_get_emit_result!(
                error_com::ConductorError::InternalError(format!("Couldn't deserialize row into struct for uuid: {}",
                &uuid))
            );
        }
        Ok(producer)
    } else {
        //this should be impossible as we have checked that it's not empty
        log_error_and_get_emit_result!(
            error_com::ConductorError::InternalError(format!("Couldn't get the row from the row list for uuid: {}",
            &uuid))

        )
    }
}

///
/// Validates that the producer schema given matches the one that is registered in the database
///
fn validate_emit_schema(data: &conductor_common::Emit<'_, HashMap<String,serde_json::Value>>, producer: &Producer) -> bool {
    if let Ok(schema) = serde_json::from_str::<HashMap<String, serde_json::Value>>(&producer.schema)
    {
        if schema == *data.get_data() {
            return true;
        }
    }
    false
}

///
/// Record a new registration in the database.
///
async fn register(db: &db::QuestDbConn, registration: &producer_com::Registration) -> conductor_common::RegistrationResult {
    //TODO this should use an option
    let error_code = validate_registration(registration);
    if error_code != error_com::ConductorError::NoError {
        return conductor_common::RegistrationResult {
            error: error_code,
            uuid: None,
        };
    }

    match persist_registration(registration, db).await {
        Ok(uuid) => conductor_common::RegistrationResult {
            error: error_code,
            uuid: Some(uuid),
        },
        Err(err) => conductor_common::RegistrationResult {
            error: err,
            uuid: None,
        },
    }
}

async fn emit(db: &db::QuestDbConn, data: &conductor_common::Emit<'_,HashMap<String,serde_json::Value>>) -> conductor_common::EmitResult {
    let producer = match get_producer_row(db, data.get_uuid()).await {
        Ok(producer) => producer,
        Err(error_code) => {
            return conductor_common::EmitResult {
                error: error_code,
            };
        }
    };
    if !validate_emit_schema(data, &producer) {
        return conductor_common::EmitResult {
            error: error_com::ConductorError::InvalidSchema("Emitted schema didn't match registered schema".to_string()),
        };
    }
    // we know the schema is good, the uuid is good. The emit is good. Lets do this thing
    match persist_emit(data, db).await {
        Ok(_) => conductor_common::EmitResult {
            error: error_com::ConductorError::NoError,
        },
        Err(err) => conductor_common::EmitResult { error: err},
    }
}

fn validate_registration(registration: &producer_com::Registration) -> error_com::ConductorError {
    if registration.get_name().is_empty() {
        log_error_with_json!(
            registration,
            "Producer registration failed. Producer name is empty."
        );
        return error_com::ConductorError::NameInvalid("Producer registration failed. Producer name is empty.".to_string());
    }
    if let Some(custom_id) = &registration.get_custom_id() {
        if custom_id.is_empty() || custom_id.contains('.') || custom_id.contains('\"') {
            log_error_with_json!(
                registration,
                "Producer registration failed. Custom ID has illegal chars or is empty."
            );
            return error_com::ConductorError::InvalidUuid("Producer registration failed. Custom ID has illegal chars or is empty.".to_string());
        }
    }
    if registration.contains_column("ts") {
        log_error_with_json!(
            registration,
            "Producer registration failed. column with name ts. This is a reserved name."
        );
        return error_com::ConductorError::TimestampDefined("Producer registration failed. column with name ts. This is a reserved name.".to_string());
    }
    if registration.get_schema().is_empty() {
        log_error_with_json!(registration, "Producer registration failed. No columns in schema.");
        return error_com::ConductorError::NoMembers("Producer registration failed. No columns in schema.".to_string());
    }
    for col in registration.get_schema().keys() {
        if col.contains('.') || col.contains('\"') {
            log_error_with_json!(registration, "Producer registration failed. Column with name {} is invalid as it contains a '.' or a '\"'.", col);
            return error_com::ConductorError::InvalidColumnNames(format!("Producer registration failed. Column with name {} is invalid as it contains a '.' or a '\"'.", col));
        }
    }
    if registration.schema_len() > 2_147_483_647 {
        //I mean this is invalid. But seriously how did we get here
        log_error_with_json!(registration, "Producer schema registration had {} columns which is more than the maximum quest can support of 2,147,483,647.", registration.schema_len());
        return error_com::ConductorError::TooManyColumns(format!("Producer schema registration had {} columns which is more than the maximum quest can support of 2,147,483,647.", registration.schema_len()));
    }

    error_com::ConductorError::NoError
}

fn generate_create_table_sql(registration: &producer_com::Registration, table_name: &str) -> String {
    //     CREATE TABLE my_table(symb SYMBOL, price DOUBLE, ts TIMESTAMP, s STRING) timestamp(ts);
    let mut sql = format!("CREATE TABLE IF NOT EXISTS \"{}\" (ts TIMESTAMP", table_name);
    for (col_name, col_type) in registration.get_schema() {
        sql = sql + ", \"" + col_name + "\" " + col_type.to_quest_type_str();
    }
    sql += ") timestamp(ts);";
    sql
}

#[inline]
fn get_or_create_uuid_for_registration(registration: &producer_com::Registration) -> String {
    match &registration.get_custom_id() {
        Some(custom_id) => (*custom_id).to_string(),
        None => Uuid::new_v4().to_string(),
    }
}


#[inline]
fn generate_data_for_creation(registration: &producer_com::Registration, uuid: &str) -> (String, String, String, String) {
    (
        generate_create_table_sql(registration, uuid),
        registration.get_name().to_string(),
        serde_json::to_string_pretty(registration.get_schema()).unwrap_or_default(),
        uuid.to_string(),
    )
}

async fn persist_registration(registration: &producer_com::Registration, db: &db::QuestDbConn) -> Result<String, error_com::ConductorError> {
    let uuid = get_or_create_uuid_for_registration(registration);
    let (create_table_sql, producer_name, schema_json, uuid_copy) = generate_data_for_creation(registration, &uuid);

    let result: Result<u64, _> = db
        .run(move |conn: &mut postgres::Client| {
            //we will do both these in one go so that we don't add it to the producers table unless we were able to create its data table
            log::info!("creating table with sql {}", create_table_sql);
            let result = conn.execute(create_table_sql.as_str(), &[]);
            if result.is_err() {
                return result;
            }
            conn.execute(
                "INSERT INTO producers VALUES($1, $2, $3);",
                &[&producer_name, &uuid_copy, &schema_json],
            )
        })
        .await;
    match result {
        Ok(_) => Ok(uuid),
        Err(err) => {
            log::error!(
                "There was an error persisting the producer to the db: {}",
                err
            );
            Err(error_com::ConductorError::InternalError(format!("There was an error persisting the producer to the db: {}", err)))
        }
    }
}

fn get_insert_sql(emit: &conductor_common::Emit<'_, HashMap<String,serde_json::Value>>, column_names: &[&String]) -> Result<String, String> {
    if column_names.is_empty() {
        return Err("Insert Sql must have at least one colum but there were none".to_string());
    }
    let mut column_iter = column_names.iter();
    let mut columns = format!("\"{}\"", column_iter.next().unwrap());
    for column_name in column_iter {
        columns = columns + ", " + &format!("\"{}\"", column_name);
    }

    let mut values_str = String::from("$1");
    for i in 1..column_names.len() {
        values_str.push_str(format!(",${}", i).as_str());
    }
    Ok(format!(
        "INSERT INTO \"{}\" ({}) VALUES ({});",
        emit.get_uuid(), columns, values_str
    ))
}


async fn persist_emit(emit: &conductor_common::Emit<'_, HashMap<String,serde_json::Value>>, db: &db::QuestDbConn) -> Result<(), error_com::ConductorError> {
    let schema_json = get_producer_row(db, emit.get_uuid()).await?.schema;
    if schema_json.is_empty() {
        return log_error_and_get_emit_result!(
            error_com::ConductorError::NoMembers(format!("Error persisting producer emit to db. Empty registered schema for uuid: {}",
            emit.get_uuid()))
        );
    }
    let schema: schema_com::Schema;
    match serde_json::from_str(schema_json.as_str()) {
        Ok(s) => schema = s,
        Err(err) => return log_error_and_get_emit_result!(error_com::ConductorError::NoMembers(format!("Error persisting producer emit to db. Empty registered schema for uuid: {} with error: {}", emit.get_uuid(), err))),
    };

    //pull out keys and values to guarantee order!
    let mut columns = Vec::new();
    let mut params_store: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    for (key, val) in emit.get_data() {
        columns.push(key);
        let data_type;
        if let Some(dt) = schema.get(key) {
            data_type = dt;
        } else {
            return log_error_and_get_emit_result!(
                error_com::ConductorError::InvalidColumnNames(format!("Error persisting producer emit to db. Schema doesn't contain key {}",
                key))
            );
        }

        match to_solid_type_from_json(val, *data_type) {
            Ok(param) => params_store.push(param),
            Err(err) => {
                return log_error_and_get_emit_result!(
                    error_com::ConductorError::InvalidData(format!("Error persisting producer emit to db. Couldn't parse data packet. {}",
                    err))

                );
            }
        }
    }
    let sql = get_insert_sql(emit, &columns).unwrap();

    let write_result = db
        .run(move |conn: &mut postgres::Client| {
            //we will do both these in one go so that we don't add it to the producers table unless we were able to create its data table

            let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
            for p in &params_store {
                params.push(p.as_ref());
            }
            conn.execute(sql.as_str(), params.as_slice())
        })
        .await;
    match write_result {
        Ok(_) => Ok(()),
        Err(err) => {
            log_error_and_get_emit_result!(
                error_com::ConductorError::InternalError(format!("Error persisting producer emit to db. Couldn't parse data packet. {}",
                err))

            )
        }
    }
}


#[post("/v1/producer/register", format = "msgpack", data = "<data>")]
pub async fn register_pack(
    conn: db::QuestDbConn,
    data: MsgPack<producer_com::Registration>,
) -> MsgPack<conductor_common::RegistrationResult> {
    MsgPack(register(&conn, &data).await)
}

#[post("/v1/producer/register", format = "json", data = "<data>")]
pub async fn register_json(
    conn: db::QuestDbConn,
    data: Json<producer_com::Registration>,
) -> Json<conductor_common::RegistrationResult> {
    Json(register(&conn, &data).await)
}

#[post("/v1/producer/emit", format = "msgpack", data = "<data>")]
pub async fn emit_pack(conn: db::QuestDbConn, data: MsgPack<conductor_common::Emit<'_, HashMap<String,serde_json::Value>>>) -> MsgPack<conductor_common::EmitResult> {
    MsgPack(emit(&conn, &data).await)
}

#[post("/v1/producer/emit", format = "json", data = "<data>")]
pub async fn emit_json(conn: db::QuestDbConn, data: Json<conductor_common::Emit<'_, HashMap<String,serde_json::Value>>>) -> Json<conductor_common::EmitResult> {
    Json(emit(&conn, &data).await)
}

#[get("/v1/producer/check?<uuid>", format = "json")]
pub async fn check(conn: db::QuestDbConn, uuid: &str) -> Status {
    match get_producer_row(&conn, &uuid.to_string()).await {
        Ok(_) => Status::Ok,
        Err(_) => Status::NotFound,
    }
}
