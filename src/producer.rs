use std::collections::HashMap;

use postgres::{types::ToSql, Row};
use rocket::http::Status;
use rocket::serde::{json::Json, msgpack::MsgPack};
use uuid::Uuid;

use crate::db;
use conductor::producer::*;

macro_rules! logErrorWithJson {
    ($self:ident, $($args:tt)+) => {{
        match serde_json::to_string($self) {
            Ok(json) => log::error!("{} JSON = \n{}", format_args!($($args)*), json),
            Err(error) => log::error!("{} JSON couldn't be produced: {}", format_args!($($args)*), error),
        }
    }};
}

macro_rules! LogErrorAndGetEmitResult {
    ($errorCode:expr, $($args:tt)+) => {{
        log::error!($($args)*);
        Err($errorCode)
    }};
}



async fn get_producer_row(
    db: &db::QuestDbConn,
    uuid: &String,
) -> Result<Producer, ProducerErrorCode> {
    if uuid.is_empty() {
        return LogErrorAndGetEmitResult!(
            ProducerErrorCode::InvalidUuid,
            "Incoming request had an empty uuid"
        );
    }
    if uuid.is_empty() {
        return LogErrorAndGetEmitResult!(
            ProducerErrorCode::NoMembers,
            "Incoming request had no data with uuid {}",
            &uuid
        );
    }
    //check if the uuid is in the db
    let uuid_copy = uuid.clone();
    let get_producer_row = move |conn: &mut postgres::Client| {
        conn.query("SELECT * FROM producers WHERE uuid = $1;", &[&uuid_copy])
    };
    let rows: Vec<Row> = match db.run(get_producer_row).await {
        Ok(rows) => rows,
        Err(error) => {
            return LogErrorAndGetEmitResult!(
                ProducerErrorCode::Unregistered,
                "Error getting producer from database {}",
                error
            );
        }
    };
    if rows.is_empty() {
        return LogErrorAndGetEmitResult!(
            ProducerErrorCode::Unregistered,
            "Error getting producer. No rows returned for uuid: {}",
            &uuid
        );
    }
    if rows.len() > 1 {
        //this shouldn't happen...
        return LogErrorAndGetEmitResult!(
            ProducerErrorCode::InternalError,
            "There were multiple entries for uuid: {}",
            &uuid
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
            return LogErrorAndGetEmitResult!(
                ProducerErrorCode::InternalError,
                "Couldn't deserialize row into struct for uuid: {}",
                &uuid
            );
        }
        Ok(producer)
    } else {
        //this should be impossible as we have checked that it's not empty
        LogErrorAndGetEmitResult!(
            ProducerErrorCode::InternalError,
            "Couldn't get the row from the row list for uuid: {}",
            &uuid
        )
    }
}

fn validate_emit_schema(data: &Emit, producer: &Producer) -> bool {
    if let Ok(schema) = serde_json::from_str::<HashMap<String, serde_json::Value>>(&producer.schema)
    {
        if schema == data.data {
            return true;
        }
    }
    false
}

async fn register(db: &db::QuestDbConn, registration: &Registration) -> RegistrationResult {
    let error_code = validate_registration(registration);
    if error_code != ProducerErrorCode::NoError {
        return RegistrationResult {
            error: error_code as u8,
            uuid: None,
        };
    }

    match persist_registration(registration, db).await {
        Ok(uuid) => RegistrationResult {
            error: error_code as u8,
            uuid: Some(uuid),
        },
        Err(err) => RegistrationResult {
            error: err as u8,
            uuid: None,
        },
    }
}

async fn emit(db: &db::QuestDbConn, data: &Emit) -> EmitResult {
    let producer = match get_producer_row(db, &data.uuid).await {
        Ok(producer) => producer,
        Err(error_code) => {
            return EmitResult {
                error: error_code as u8,
            }
        }
    };
    if validate_emit_schema(data, &producer) {
        return EmitResult {
            error: ProducerErrorCode::InvalidColumnNames as u8,
        };
    }
    // we know the schema is good, the uuid is good. The emit is good. Lets do this thing
    match persist_emit(data, db).await {
        Ok(_) => EmitResult {
            error: ProducerErrorCode::NoError as u8,
        },
        Err(err) => EmitResult { error: err as u8 },
    }
}

fn validate_registration(registration: &Registration) -> ProducerErrorCode {
    if registration.name.is_empty() {
        logErrorWithJson!(
            registration,
            "Producer registration failed. Producer name is empty."
        );
        return ProducerErrorCode::NameInvalid;
    }
    if let Some(custom_id) = &registration.use_custom_id {
        if custom_id.is_empty() || custom_id.contains('.') || custom_id.contains('\"') {
            logErrorWithJson!(
                registration,
                "Producer registration failed. Custom ID has illegal chars or is empty."
            );
            return ProducerErrorCode::InvalidUuid;
        }
    }
    if registration.schema.contains_key("ts") {
        logErrorWithJson!(
            registration,
            "Producer registration failed. column with name ts. This is a resereved name."
        );
        return ProducerErrorCode::TimestampDefined;
    }
    if registration.schema.is_empty() {
        logErrorWithJson!(registration, "Producer registration failed. No columns in schema.");
        return ProducerErrorCode::NoMembers;
    }
    for col in registration.schema.keys() {
        if col.contains('.') || col.contains('\"') {
            logErrorWithJson!(registration, "Producer registration failed. Column with name {} is invalid as it contains a '.' or a '\"'.", col);
            return ProducerErrorCode::InvalidColumnNames;
        }
    }
    if registration.schema.len() > 2147483647 {
        //I mean this is invalid. But seriously how did we get here
        logErrorWithJson!(registration, "Producer schema registration had {} columns which is more than the maximum quest can support of 2,147,483,647.", registration.schema.len());
        return ProducerErrorCode::TooManyColumns;
    }

    ProducerErrorCode::NoError
}

fn generate_table_sql(registration: &Registration, table_name: &str) -> String {
    //     CREATE TABLE my_table(symb SYMBOL, price DOUBLE, ts TIMESTAMP, s STRING) timestamp(ts);
    let mut sql = format! {"CREATE TABLE IF NOT EXISTS \"{}\" (ts TIMESTAMP", table_name};
    for (col_name, col_type) in &registration.schema {
        sql = sql + ", \"" + col_name + "\" " + col_type.to_quest_type_str();
    }
    sql += ") timestamp(ts);";
    sql
}

#[inline]
fn get_or_create_uuid_for_registration(registration: &Registration) -> String {
    match &registration.use_custom_id {
        Some(custom_id) => custom_id.clone(),
        None => Uuid::new_v4().to_string(),
    }
}



#[inline]
fn generate_data_for_creation(registration: &Registration, uuid: &str) -> (String, String, String, String) {
    (
        generate_table_sql(registration, uuid),
        registration.name.clone(),
        get_schema_as_json_str(&registration.schema),
        uuid.to_string(),
    )
}

async fn persist_registration(registration: &Registration, db: &db::QuestDbConn) -> Result<String, ProducerErrorCode> {
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
            Err(ProducerErrorCode::InternalError)
        }
    }
}

fn get_insert_sql(emit: &Emit, column_names: &[&String]) -> Result<String, String> {
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
        &emit.uuid, columns, values_str
    ))
}


async fn persist_emit(emit: &Emit, db: &db::QuestDbConn) -> Result<(), ProducerErrorCode> {
    let schema_json = match get_producer_row(db, &emit.uuid).await {
        Ok(p) => p.schema,
        Err(ec) => {
            return LogErrorAndGetEmitResult!(
                ec,
                "Error persisting producer emit to db. Couldn't get producer  for uuid: {}",
                &emit.uuid
            )
        }
    };
    if schema_json.is_empty() {
        return LogErrorAndGetEmitResult!(
            ProducerErrorCode::NoMembers,
            "Error persisting producer emit to db. Empty registered schema for uuid: {}",
            &emit.uuid
        );
    }
    let schema: HashMap<String, DataTypes>;
    match serde_json::from_str(schema_json.as_str()) {
        Ok(s) => schema = s,
        Err(err) => return LogErrorAndGetEmitResult!(ProducerErrorCode::NoMembers, "Error persisting producer emit to db. Empty registered schema for uuid: {} with error: {}", &emit.uuid, err),
    };

    //pull out keys and values to garantee order!
    let mut columns = Vec::new();
    let mut params_store: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    for (key, val) in &emit.data {
        columns.push(key);
        let data_type;
        if let Some(dt) = schema.get(key) {
            data_type = dt;
        } else {
            return LogErrorAndGetEmitResult!(
                ProducerErrorCode::InvalidColumnNames,
                "Error persisting producer emit to db. Schema doesn't contain key {}",
                key
            );
        }

        match to_solid_type_from_json(val, data_type) {
            Ok(param) => params_store.push(param),
            Err(err) => {
                return LogErrorAndGetEmitResult!(
                    ProducerErrorCode::InvalidData,
                    "Error persisting producer emit to db. Couldn't parse data packet. {}",
                    err
                );
            }
        }
    }
    let sql = get_insert_sql(emit, &columns).unwrap();

    let _ = db
        .run(move |conn: &mut postgres::Client| {
            //we will do both these in one go so that we don't add it to the producers table unless we were able to create its data table

            let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
            for p in &params_store {
                params.push(p.as_ref());
            }
            conn.execute(sql.as_str(), params.as_slice())
        })
        .await;
    Ok(())
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

#[post("/producer/emit", format = "msgpack", data = "<data>")]
pub async fn emit_pack(conn: db::QuestDbConn, data: MsgPack<Emit>) -> MsgPack<EmitResult> {
    MsgPack(emit(&conn, &data).await)
}

#[post("/producer/emit", format = "json", data = "<data>")]
pub async fn emit_json(conn: db::QuestDbConn, data: Json<Emit>) -> Json<EmitResult> {
    Json(emit(&conn, &data).await)
}

#[get("/producer/check?<uuid>", format = "json")]
pub async fn check(conn: db::QuestDbConn, uuid: &str) -> Status {
    match get_producer_row(&conn, &uuid.to_string()).await {
        Ok(_) => Status::Ok,
        Err(_) => Status::NotFound,
    }
}
