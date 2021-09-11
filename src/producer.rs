use std::collections::HashMap;

use postgres::{types::ToSql, Row};
use rocket::serde::{json::Json, msgpack::MsgPack, Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::db;

macro_rules! logErrorWithJson {
    ($self:ident, $($args:tt)+) => {{
        match serde_json::to_string($self) {
            Ok(json) => log::error!("{} JSON = \n{}", format_args!($($args)*), json),
            Err(error) => log::error!("{} JSON couldn't be produced: {}", format_args!($($args)*), error),
        }
    }};
}

#[derive(Debug, Clone, Deserialize, Serialize)]
enum DataTypes {
    Int,
    Float,
    Time,
    String,
    Binary,
    Bool,
    Double,
}

impl DataTypes {
    fn to_quest_type(&self) -> &str {
        match self {
            DataTypes::Int => "long",
            DataTypes::Float => "float",
            DataTypes::Time => "timestamp",
            DataTypes::Binary => "binary",
            DataTypes::String => "string",
            DataTypes::Bool => "boolean",
            DataTypes::Double => "double",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
enum ProducerErrorCode {
    NoError = 0,
    TimestampDefined = 1,
    NoMembers = 2,
    InvalidColumnNames = 3,
    TooManyColumns = 4, // who is doing this???
    InternalError = 5,
    InvalidUuid = 6,
    NameInvalid = 7,
    Unregistered = 8,
    InvalidData = 9,
}

macro_rules! LogErrorAndGetEmitResult {
    ($errorCode:expr, $($args:tt)+) => {{
        log::error!($($args)*);
        Err($errorCode)
    }};
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Registration {
    name: String,
    schema: HashMap<String, DataTypes>,
    use_custom_id: Option<String>, // this is to support devices without persistant storage such as an arduino. They can have a custom id
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RegistrationResult {
    error: u8,
    uuid: Option<String>,
}

impl Registration {
    fn valid(&self) -> ProducerErrorCode {
        if self.name.is_empty() {
            logErrorWithJson!(
                self,
                "Producer registration failed. Producer name is empty."
            );
            return ProducerErrorCode::NameInvalid;
        }
        if let Some(custom_id) = &self.use_custom_id {
            if custom_id.is_empty() || custom_id.contains('.') || custom_id.contains('\"') {
                logErrorWithJson!(
                    self,
                    "Producer registration failed. Custom ID has illegal chars or is empty."
                );
                return ProducerErrorCode::InvalidUuid;
            }
        }
        if self.schema.contains_key("ts") {
            logErrorWithJson!(
                self,
                "Producer registration failed. column with name ts. This is a resereved name."
            );
            return ProducerErrorCode::TimestampDefined;
        }
        if self.schema.is_empty() {
            logErrorWithJson!(self, "Producer registration failed. No columns in schema.");
            return ProducerErrorCode::NoMembers;
        }
        for col in self.schema.keys() {
            if col.contains('.') || col.contains('\"') {
                logErrorWithJson!(self, "Producer registration failed. Column with name {} is invalid as it contains a '.' or a '\"'.", col);
                return ProducerErrorCode::InvalidColumnNames;
            }
        }
        if self.schema.len() > 2147483647 {
            //I mean this is invalid. But seriously how did we get here
            logErrorWithJson!(self, "Producer schema registration had {} columns which is more than the maximum quest can support of 2,147,483,647.", self.schema.len());
            return ProducerErrorCode::TooManyColumns;
        }

        ProducerErrorCode::NoError
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

    #[inline]
    fn get_uuid(&self) -> String {
        match &self.use_custom_id {
            Some(custom_id) => custom_id.clone(),
            None => Uuid::new_v4().to_string(),
        }
    }

    #[inline]
    fn get_schema_as_json_str(&self) -> String {
        match serde_json::to_string(&self.schema) {
            Ok(v) => v,
            Err(err) => {
                log::error!("Couldn't serialize producer schema into json: {}", err);
                String::new()
            }
        }
    }

    #[inline]
    fn generate_data_for_creation(&self, uuid: &str) -> (String, String, String, String) {
        (
            self.generate_table_sql(uuid),
            self.name.clone(),
            self.get_schema_as_json_str(),
            uuid.to_string(),
        )
    }

    async fn persist(&self, db: &db::QuestDbConn) -> Result<String, ProducerErrorCode> {
        let uuid = self.get_uuid();
        let (create_table_sql, producer_name, schema_json, uuid_copy) =
            self.generate_data_for_creation(&uuid);

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
}

async fn register(db: &db::QuestDbConn, data: &Registration) -> RegistrationResult {
    let error_code = data.valid();
    if error_code != ProducerErrorCode::NoError {
        return RegistrationResult {
            error: error_code as u8,
            uuid: None,
        };
    }

    match data.persist(db).await {
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Emit {
    uuid: String,
    timestamp: Option<u64>,
    data: HashMap<String, serde_json::Value>,
}

impl Emit {
    fn get_insert_sql(&self, column_names: &[&String]) -> Result<String, String> {
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
            &self.uuid, columns, values_str
        ))
    }

    #[inline(always)]
    fn json_value_to_type(
        val: &Value,
        data_type: &DataTypes,
    ) -> Result<Box<dyn ToSql + Sync + Send>, String> {
        match data_type {
            DataTypes::Int => match val.as_i64() {
                Some(v) => Ok(Box::new(v)),
                None => Err(format!(
                    "Not possible to convert json value to i64. Value: {:?}",
                    val
                )),
            },
            DataTypes::Float => {
                match val.as_f64() {
                    Some(v) => {
                        /*check that this will actually fit within an f32 bounds so the cast should? be safe.
                        use epsilon to make extra sure that this is an okay thing to do. 
                        There could be a time when a valid f32 value is rejected due to the epsilon difference but if your data
                        is that close use a double type...*/
                        if v > (f32::MAX as f64) - (f32::EPSILON as f64) || v < (f32::MIN as f64) + (f32::EPSILON as f64) {
                            return Err(format!("Not possible to convert json value to f32 (too big to fit). Value: {:?}", val));
                        }
                        // It should be safe to cast this to an f32. It fits
                        Ok(Box::new(v as f32))
                    },
                    None => Err(format!("Not possible to convert json value to f32 (Couldn't get f64 first). Value: {:?}", val)),
                }
            }
            DataTypes::Time => match serde_json::from_value::<chrono::NaiveDateTime>(val.clone()) {
                Ok(v) => Ok(Box::new(v)),
                Err(_) => Err(format!(
                    "Not possible to convert json value to naive date time. Value: {:?}",
                    val
                )),
            },
            DataTypes::String => match val.as_str() {
                Some(v) => Ok(Box::new(v.to_string())),
                None => Err(format!(
                    "Not possible to convert json value to string. Value: {:?}",
                    val
                )),
            },
            DataTypes::Bool => match val.as_bool() {
                Some(v) => Ok(Box::new(v)),
                None => Err(format!(
                    "Not possible to convert json value to bool. Value: {:?}",
                    val
                )),
            },
            DataTypes::Double => match val.as_f64() {
                Some(v) => Ok(Box::new(v)),
                None => Err(format!(
                    "Not possible to convert json value to double. Value: {:?}",
                    val
                )),
            },
            DataTypes::Binary => match serde_json::from_value::<Vec<u8>>(val.clone()) {
                Ok(v) => Ok(Box::new(v)),
                Err(_) => Err(format!(
                    "Not possible to convert json value to binary. Value: {:?}",
                    val
                )),
            },
        }
    }

    async fn persist(&self, db: &db::QuestDbConn) -> Result<(), ProducerErrorCode> {
        let schema_json = match get_producer_row(db, &self.uuid).await {
            Ok(p) => p.schema,
            Err(ec) => {
                return LogErrorAndGetEmitResult!(
                    ec,
                    "Error persisting producer emit to db. Couldn't get producer  for uuid: {}",
                    &self.uuid
                )
            }
        };
        if schema_json.is_empty() {
            return LogErrorAndGetEmitResult!(
                ProducerErrorCode::NoMembers,
                "Error persisting producer emit to db. Empty registered schema for uuid: {}",
                &self.uuid
            );
        }
        let schema: HashMap<String, DataTypes>;
        match serde_json::from_str(schema_json.as_str()) {
            Ok(s) => schema = s,
            Err(err) => return LogErrorAndGetEmitResult!(ProducerErrorCode::NoMembers, "Error persisting producer emit to db. Empty registered schema for uuid: {} with error: {}", &self.uuid, err),
        };

        //pull out keys and values to garantee order!
        let mut columns = Vec::new();
        let mut params_store: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
        for (key, val) in &self.data {
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

            match Self::json_value_to_type(val, data_type) {
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
        let sql = self.get_insert_sql(&columns).unwrap();

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
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EmitResult {
    error: u8,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Producer {
    name: String,
    uuid: String,
    schema: String,
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

    data.persist(db).await;

    EmitResult {
        error: ProducerErrorCode::NoError as u8,
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

#[post("/producer/emit", format = "msgpack", data = "<data>")]
pub async fn emit_pack(conn: db::QuestDbConn, data: MsgPack<Emit>) -> MsgPack<EmitResult> {
    MsgPack(emit(&conn, &data).await)
}

#[post("/producer/emit", format = "json", data = "<data>")]
pub async fn emit_json(conn: db::QuestDbConn, data: Json<Emit>) -> Json<EmitResult> {
    Json(emit(&conn, &data).await)
}
