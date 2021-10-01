
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use url::Url;
use duplicate::duplicate;
use chrono::{DateTime, Utc, NaiveDate, NaiveDateTime};
#[cfg(feature = "async")]
use async_trait::async_trait;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
pub enum DataTypes {
    Int,
    Float,
    Time,
    String,
    Binary,
    Bool,
    Double,
}

impl DataTypes {
    #[must_use] pub const fn to_quest_type_str(&self) -> &str {
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


#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
pub enum ErrorCode {
    NoError = 0,
    TimestampDefined = 1,
    NoMembers = 2,
    InvalidColumnNames = 3,
    TooManyColumns = 4,
    // who is doing this???
    InternalError = 5,
    InvalidUuid = 6,
    NameInvalid = 7,
    Unregistered = 8,
    InvalidData = 9,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RegistrationResult {
    pub error: u8,
    pub uuid: Option<String>,
}

pub type Schema = HashMap<String, DataTypes>;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Registration {
    name: String,
    schema: Schema,
    use_custom_id: Option<String>, // this is to support devices without persistent storage such as an arduino. They can have a custom id
}

impl Registration {
    #[must_use] pub const fn new(name: String, schema: Schema, custom_id: Option<String>) -> Self {
        Self {
            name,
            schema,
            use_custom_id: custom_id,
        }
    }

    #[must_use] pub fn new_empty(name: String) -> Self {
        Self {
            name,
            schema: std::collections::HashMap::default(),
            use_custom_id: None,
        }
    }

    #[must_use] pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn set_name(&mut self, name: &str) {
        self.name = String::from(name);
    }

    pub fn set_custom_id(&mut self, id: String) {
        self.use_custom_id = Some(id);
    }

    #[must_use] pub const fn has_custom_id(&self) -> bool {
        self.use_custom_id.is_some()
    }
    #[must_use] pub fn get_custom_id(&self) -> Option<&str> {
        if let Some(c_id) = &self.use_custom_id {
            return Some(c_id.as_str());
        }
        None
    }

    pub fn add_column(&mut self, column_name: String, data_type: DataTypes) -> bool {
        self.schema.insert(column_name, data_type).is_some()
    }

    pub fn remove_column(&mut self, column_name: &str) -> bool {
        self.schema.remove(column_name).is_some()
    }

    #[must_use] pub fn contains_column(&self, column_name: &str) -> bool {
        self.schema.contains_key(column_name)
    }

    #[must_use] pub fn schema_len(&self) -> usize {
        self.schema.len()
    }

    #[must_use] pub const fn get_schema(&self) -> &Schema {
        &self.schema
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Emit {
    uuid: String,
    timestamp: Option<u64>,
    data: HashMap<String, serde_json::Value>,
}

impl Emit {
    #[must_use] pub const fn new(uuid: String, timestamp: Option<u64>, data: HashMap<String, serde_json::Value>) -> Self {
        Self {
            uuid,
            timestamp,
            data,
        }
    }
    #[must_use] pub fn new_empty(uuid: String, timestamp: Option<u64>) -> Self {
        Self {
            uuid,
            timestamp,
            data: std::collections::HashMap::default(),
        }
    }

    #[must_use] pub const fn get_uuid(&self) -> &String {
        &self.uuid
    }

    #[must_use] pub const fn get_timestamp(&self) -> Option<u64> {
        self.timestamp
    }

    #[must_use] pub const fn get_data(&self) -> &HashMap<String, serde_json::Value> {
        &self.data
    }

    #[must_use] pub fn column_in_data(&self, column_name: &str) -> bool {
        self.data.contains_key(column_name)
    }

    #[must_use] pub fn get_value_for_column(&self, column_name: &str) -> Option<&serde_json::Value> {
        self.data.get(column_name)
    }

    pub fn insert_or_overwrite_column(&mut self, column_name: String, value: serde_json::Value) -> bool {
        self.data.insert(column_name, value).is_some()
    }

    pub fn remove_column(&mut self, column_name: &str) -> Option<serde_json::Value> {
        self.data.remove(column_name)
    }

    #[must_use] pub fn get_column_list(&self) -> Vec<&String> {
        self.data.keys().collect()
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct EmitResult {
    pub error: u8,
}


pub struct SchemaBuilder {
    schema: Schema,
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaBuilder {
    #[must_use] pub fn new() -> Self {
        Self {
            schema: std::collections::HashMap::default()
        }
    }

    //noinspection RsSelfConvention
    #[must_use] pub fn with_capacity(n: usize) -> Self {
        Self {
            schema: HashMap::with_capacity(n)
        }
    }

    #[must_use] pub fn add_column(mut self, name: String, col_type: DataTypes) -> Self {
        self.schema.insert(name, col_type);
        self
    }

    #[must_use] pub fn add_int(mut self, name: String) -> Self {
        self.schema.insert(name, DataTypes::Int);
        self
    }
    #[must_use] pub fn add_float(mut self, name: String) -> Self {
        self.schema.insert(name, DataTypes::Float);
        self
    }
    #[must_use] pub fn add_time(mut self, name: String) -> Self {
        self.schema.insert(name, DataTypes::Time);
        self
    }
    #[must_use] pub fn add_binary(mut self, name: String) -> Self {
        self.schema.insert(name, DataTypes::Binary);
        self
    }
    #[must_use] pub fn add_string(mut self, name: String) -> Self {
        self.schema.insert(name, DataTypes::String);
        self
    }
    #[must_use] pub fn add_bool(mut self, name: String) -> Self {
        self.schema.insert(name, DataTypes::Bool);
        self
    }
    #[must_use] pub fn add_double(mut self, name: String) -> Self {
        self.schema.insert(name, DataTypes::Double);
        self
    }

    #[allow(clippy::missing_const_for_fn)]
    #[must_use] pub fn build(self) -> Schema {
        self.schema
    }
}


#[cfg(feature = "async")]
#[async_trait]
#[allow(clippy::module_name_repetitions)]
pub trait AsyncProducer {
    async fn emit_raw(uuid: &str, conductor_url: Url, data: &[u8]) -> Result<(), &'static str>
    {
        Err("")
    }
    //Generate the schema for this struct and register it with conductor
    async fn register(name: &str, uuid: Option<String>, conductor_url: Url) -> Result<String, &'static str>
    {
        Err("")
    }
    async fn is_registered(uuid: &str, conductor_url: Url) -> Result<bool, &'static str>
    {
        Err("")
    }
    fn generate_schema() -> HashMap<String, DataTypes>;
}


pub trait Producer {
    fn emit_raw(_uuid: &str, _conductor_url: Url, _data: &[u8]) -> Result<(), &'static str>
    {
        Err("")
    }
    ///
    /// TODO
    /// # Arguments
    ///
    /// * `name`:
    /// * `uuid`:
    /// * `conductor_url`:
    ///
    /// returns: Result<String, &str>
    /// # Errors
    /// TODO
    /// # Examples
    ///
    /// ```
    ///
    /// ```
    //Generate the schema for this struct and register it with conductor
    fn register(name: &str, uuid: Option<String>, conductor_url: Url) -> Result<String, &'static str>
    {
        //TODO handle errors correctly
        let reg = Registration {
            name: name.to_string(),
            schema: Self::generate_schema(),
            use_custom_id: uuid
        };
        let client = reqwest::blocking::Client::new();
        let request = {
            let msg_pack = match rmp_serde::to_vec_named(&reg){
                Ok(m) => m,
                Err(_) => return Err("there was an error serializing the registration struct")
            };
            client.post(conductor_url)
                .body(msg_pack)
                .header(reqwest::header::CONTENT_TYPE,reqwest::header::HeaderValue::from_static("application/msgpack")).send()
        };
        let response = match request {
            Ok(r) => r,
            Err(_) => return Err("There was an error sending the registration")
        };
        let result:RegistrationResult = match rmp_serde::from_read_ref(response.bytes().unwrap().as_ref()) {
            Ok(r) => r,
            Err(_) => return Err("Couldn't deserialize the registration response")
        };
        Ok(result.uuid.unwrap())
    }

    fn is_registered(_uuid: &str, _conductor_url: Url) -> Result<bool, &'static str>
    {
        Err("")
    }
    fn generate_schema() -> HashMap<String, DataTypes>;
}

pub trait ToProducerData {
    fn conductor_data_type() -> DataTypes;
}

#[duplicate(
int_type;
[ u8 ]; [ u16 ]; [ u32 ];
[ i8 ]; [ i16 ]; [ i32 ]; [ i64 ];
)]
impl ToProducerData for int_type {
    fn conductor_data_type() -> DataTypes {
        DataTypes::Int
    }
}

#[duplicate(
string_type;
[ String ]; [ str ];
)]
impl ToProducerData for string_type {
    fn conductor_data_type() -> DataTypes {
        DataTypes::String
    }
}

#[duplicate(
float_type;
[ f32 ]; [ f64 ];
)]
impl ToProducerData for float_type {
    fn conductor_data_type() -> DataTypes {
        DataTypes::Double
    }
}

impl ToProducerData for [u8] {
    fn conductor_data_type() -> DataTypes {
        DataTypes::Binary
    }
}

impl ToProducerData for bool {
    fn conductor_data_type() -> DataTypes {
        DataTypes::Bool
    }
}

#[duplicate(
time_type;
[ NaiveDate ]; [ NaiveDateTime ];
[ DateTime < Utc > ];
)]
impl ToProducerData for time_type {
    fn conductor_data_type() -> DataTypes {
        DataTypes::Time
    }
}


#[cfg(test)]
mod tests {
    use super::SchemaBuilder;
    use crate::producer;


    #[test]
    fn schema_builder() {
        let schema = SchemaBuilder::new().add_binary(String::from("hello")).add_bool(String::from("hello world")).build();
        let value = schema.get("hello").expect("expected value wasn't in the schema");
        assert!(matches!(value, producer::DataTypes::Binary));
        assert!(schema.contains_key("hello"));
    }
}