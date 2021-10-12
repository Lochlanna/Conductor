use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use url::{Url};
use duplicate::duplicate;
use chrono::{DateTime, Utc, NaiveDate, NaiveDateTime};
#[cfg(feature = "async")]
use async_trait::async_trait;
use std::fmt;
use std::fmt::Formatter;

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
    #[must_use]
    pub const fn to_quest_type_str(&self) -> &str {
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
pub enum ConductorError {
    NoError,
    TimestampDefined (String),
    NoMembers(String),
    InvalidColumnNames(String),
    TooManyColumns(String),
    InternalError(String),
    InvalidUuid(String),
    NameInvalid(String),
    Unregistered(String),
    InvalidData(String),
    InvalidSchema(String),
}

impl std::error::Error for ConductorError {}

impl fmt::Display for ConductorError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ConductorError::NoError => write!(f, "NoError"),
            ConductorError::TimestampDefined(message) => write!(f, "NoError: {}", message),
            ConductorError::NoMembers(message) => write!(f, "NoMembers: {}", message),
            ConductorError::InvalidColumnNames(message) => write!(f, "InvalidColumnNames: {}", message),
            ConductorError::TooManyColumns(message) => write!(f, "TooManyColumns: {}", message),
            ConductorError::InternalError(message) => write!(f, "InternalError: {}", message),
            ConductorError::InvalidUuid(message) => write!(f, "InvalidUuid: {}", message),
            ConductorError::NameInvalid(message) => write!(f, "NameInvalid: {}", message),
            ConductorError::Unregistered(message) => write!(f, "Unregistered: {}", message),
            ConductorError::InvalidData(message) => write!(f, "InvalidData: {}", message),
            ConductorError::InvalidSchema(message) => write!(f, "InvalidSchema: {}", message),
        }
    }
}


#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RegistrationResult {
    pub error: ConductorError,
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
    #[must_use]
    pub const fn new(name: String, schema: Schema, custom_id: Option<String>) -> Self {
        Self {
            name,
            schema,
            use_custom_id: custom_id,
        }
    }

    #[must_use]
    pub fn new_empty(name: String) -> Self {
        Self {
            name,
            schema: std::collections::HashMap::default(),
            use_custom_id: None,
        }
    }

    #[must_use]
    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn set_name(&mut self, name: &str) {
        self.name = String::from(name);
    }

    pub fn set_custom_id(&mut self, id: String) {
        self.use_custom_id = Some(id);
    }

    #[must_use]
    pub const fn has_custom_id(&self) -> bool {
        self.use_custom_id.is_some()
    }
    #[must_use]
    pub fn get_custom_id(&self) -> Option<&str> {
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

    #[must_use]
    pub fn contains_column(&self, column_name: &str) -> bool {
        self.schema.contains_key(column_name)
    }

    #[must_use]
    pub fn schema_len(&self) -> usize {
        self.schema.len()
    }

    #[must_use]
    pub const fn get_schema(&self) -> &Schema {
        &self.schema
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Emit<'a, T> {
    uuid: &'a str,
    timestamp: Option<u64>,
    data: T,
}

impl<'a, T> Emit<'a, T> {
    #[must_use]
    pub const fn new(uuid: &'a str, timestamp: Option<u64>, data: T) -> Self {
        Self {
            uuid,
            timestamp,
            data,
        }
    }

    #[must_use]
    pub const fn get_uuid(&self) -> &str {
        self.uuid
    }

    #[must_use]
    pub const fn get_timestamp(&self) -> Option<u64> {
        self.timestamp
    }

    #[must_use]
    pub const fn get_data(&self) -> &T {
        &self.data
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EmitResult {
    pub error: ConductorError,
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
    #[must_use]
    pub fn new() -> Self {
        Self {
            schema: std::collections::HashMap::default()
        }
    }

    //noinspection RsSelfConvention
    #[must_use]
    pub fn with_capacity(n: usize) -> Self {
        Self {
            schema: HashMap::with_capacity(n)
        }
    }

    #[must_use]
    pub fn add_column(mut self, name: String, col_type: DataTypes) -> Self {
        self.schema.insert(name, col_type);
        self
    }

    #[must_use]
    pub fn add_int(mut self, name: String) -> Self {
        self.schema.insert(name, DataTypes::Int);
        self
    }
    #[must_use]
    pub fn add_float(mut self, name: String) -> Self {
        self.schema.insert(name, DataTypes::Float);
        self
    }
    #[must_use]
    pub fn add_time(mut self, name: String) -> Self {
        self.schema.insert(name, DataTypes::Time);
        self
    }
    #[must_use]
    pub fn add_binary(mut self, name: String) -> Self {
        self.schema.insert(name, DataTypes::Binary);
        self
    }
    #[must_use]
    pub fn add_string(mut self, name: String) -> Self {
        self.schema.insert(name, DataTypes::String);
        self
    }
    #[must_use]
    pub fn add_bool(mut self, name: String) -> Self {
        self.schema.insert(name, DataTypes::Bool);
        self
    }
    #[must_use]
    pub fn add_double(mut self, name: String) -> Self {
        self.schema.insert(name, DataTypes::Double);
        self
    }

    #[allow(clippy::missing_const_for_fn)]
    #[must_use]
    pub fn build(self) -> Schema {
        self.schema
    }
}

#[derive(Debug)]
pub enum Error {
    InvalidConductorDomain(String),
    MsgPackSerialisationFailure(rmp_serde::encode::Error),
    JsonSerialisationFailure(serde_json::Error),
    GenericSerialisationFailure(Box<dyn std::error::Error>),
    ConductorError(ConductorError),
    NetworkError(reqwest::Error),
    MsgPackDeserializationFailure(rmp_serde::decode::Error),
    JsonDeserializationFailure(serde_json::Error),
    GenericDeserializationFailure(Box<dyn std::error::Error>),

}


impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Error::InvalidConductorDomain(message) => write!(f, "InvalidConductorDomain: {}", message),
            Error::MsgPackSerialisationFailure(encode_error) => write!(f, "MsgPackSerialisationFailure: {}", encode_error),
            Error::ConductorError(ce) => write!(f, "ConductorError: {}", ce),
            Error::NetworkError(re) => write!(f, "NetworkError: {}", re),
            Error::MsgPackDeserializationFailure(decode_error) => write!(f, "MsgPackDeserializationFailure: {}", decode_error),
            Error::JsonSerialisationFailure(encode_error) => write!(f, "JsonSerialisationFailure: {}", encode_error),
            Error::GenericSerialisationFailure(encode_error) => write!(f, "GenericSerialisationFailure: {}", encode_error),
            Error::JsonDeserializationFailure(decode_error) => write!(f, "JsonDeserializationFailure: {}", decode_error),
            Error::GenericDeserializationFailure(decode_error) => write!(f, "GenericDeserializationFailure: {}", decode_error),
        }
    }
}

pub trait Base: Serialize + Clone {
    fn generate_schema() -> HashMap<String, DataTypes>;

    ///
    /// Prepares a payload for emitting data. This function doesn't send the payload.
    ///
    /// # Arguments
    ///
    /// * `uuid`: The unique ID of this producer.
    /// * `conductor_domain`: The url to the conductor instance.
    ///
    /// # Errors
    ///
    /// * `InvalidConductorDomain`: Produced when the conductor domain is an invalid url.
    /// * `SerialisationFailure`: Produced when the emit payload cannot be serialised to the message pack format. This is most likely
    /// due to a difficulty serialising Self using serde.
    /// # Example
    ///
    /// ```
    /// use std::collections::HashMap;
    /// use conductor_shared::producer::{Base, DataTypes};
    /// #[derive(Clone, Serialize)]
    /// struct Measurement {
    ///     data_point:u8
    /// }
    /// impl Base for Measurement {
    ///     fn generate_schema() -> HashMap<String, DataTypes> {
    ///         unimplemented!("Not needed for example/test");
    ///     }
    /// }
    /// let m = Measurement {
    ///     data_point: 10
    /// };
    /// let expected:Vec<u8> = vec![3,4,5];
    /// assert_eq!(m, expected);
    /// ```
    fn generate_emit_data(&self, uuid: &str, conductor_domain: Url) -> Result<(Vec<u8>, Url), Error> {
        let url = match conductor_domain.join("/v1/producer/emit") {
            Ok(u) => u,
            Err(err) => return Err(Error::InvalidConductorDomain(format!("The conductor domain was invalid. {}", err)))
        };
        let emit: Emit<Self> = Emit {
            uuid,
            timestamp: None,
            data: self.clone(),
        };
        let payload = match rmp_serde::to_vec_named(&emit) {
            Ok(p) => p,
            Err(err) => {
                return Err(Error::MsgPackSerialisationFailure(err));
            }
        };
        Ok((payload, url))
    }

    ///
    /// Prepares the payload used for registration. Registration is not done by this function.
    ///
    /// # Arguments
    ///
    /// * `name`: The name of the producer.
    /// This doesn't need to be unique in a Conductor network although it may be helpful to you if it is.
    /// * `uuid`: The unique ID string to identify this producer. If it's none one will be generated by the
    /// Conductor server and returned to us. Most of the time you'll want to leave this as None.
    /// * `conductor_domain`: The url to the conductor instance.
    ///
    ///# Errors
    ///
    /// * `InvalidConductorDomain`: Produced when the conductor domain is an invalid url.
    /// * `MsgPackSerialisationFailure`: Produced when the emit payload cannot be serialised to the message pack format.
    ///
    /// # Example
    ///
    /// ```
    /// use std::collections::HashMap;
    /// use conductor_shared::producer::{Base, DataTypes};
    /// #[derive(Clone, Serialize)]
    /// struct Measurement {
    ///     data_point:u8
    /// }
    /// impl Base for Measurement {
    ///     fn generate_schema() -> HashMap<String, DataTypes> {
    ///         unimplemented!("Not needed for example/test");
    ///     }
    /// }
    /// let m = Measurement {
    ///     data_point: 10
    /// };
    /// let expected:Vec<u8> = vec![3,4,5];
    /// assert_eq!(m, expected);
    /// ```
    fn prepare_registration_data(name: &str, uuid: Option<String>, conductor_domain: Url) -> Result<(Vec<u8>, Url), Error> {
        let url = match conductor_domain.join("/v1/producer/register") {
            Ok(u) => u,
            Err(err) => return Err(Error::InvalidConductorDomain(format!("The conductor domain was invalid. {}", err)))
        };

        let reg = Registration {
            name: name.to_string(),
            schema: Self::generate_schema(),
            use_custom_id: uuid,
        };
        let payload = match rmp_serde::to_vec_named(&reg) {
            Ok(m) => m,
            Err(err) => {
                return Err(Error::MsgPackSerialisationFailure(err));
            }
        };
        Ok((payload, url))
    }
}

#[cfg(feature = "async")]
#[async_trait]
#[allow(clippy::module_name_repetitions)]
pub trait AsyncProducer: Base {
    async fn emit(&self, uuid: &str, conductor_domain: Url) -> Result<(), Error>
    {
        let (payload, url) = self.generate_emit_data(uuid, conductor_domain)?;

        //start blocking specific
        let client = reqwest::Client::new();
        let request_resp = client.post(url)
            .body(payload)
            .header(reqwest::header::CONTENT_TYPE, reqwest::header::HeaderValue::from_static("application/msgpack"))
            .send().await;

        let response = match request_resp {
            Ok(r) => r,
            Err(err) => return Err(Error::NetworkError(err))
        };
        let result: EmitResult = match rmp_serde::from_read_ref(response.bytes().await.unwrap().as_ref()) {
            Ok(r) => r,
            Err(err) => return Err(MsgPackDeserializationFailure(err))
        };
        //end blocking specific code
        if result.error == ConductorError::NoError {
            return Ok(());
        }
        Err(Error::ConductorError(result.error))
    }
    //Generate the schema for this struct and register it with conductor
    async fn register(name: &str, uuid: Option<String>, conductor_domain: Url) -> Result<String, &'static str>
    {
        //TODO handle errors correctly
        let (payload, url) = match Self::prepare_registration_data(name, uuid, conductor_domain) {
            Ok(pu) => pu,
            Err(_) => todo!()
        };
        let client = reqwest::Client::new();
        let request = client.post(url)
            .body(payload)
            .header(reqwest::header::CONTENT_TYPE, reqwest::header::HeaderValue::from_static("application/msgpack"))
            .send().await;
        let response = match request {
            Ok(r) => r,
            Err(_) => todo!()
        };
        let result: RegistrationResult = match rmp_serde::from_read_ref(response.bytes().await.unwrap().as_ref()) {
            Ok(r) => r,
            Err(_) => todo!()
        };
        Ok(result.uuid.unwrap())
    }
    async fn is_registered(uuid: &str, conductor_domain: Url) -> Result<bool, &'static str>
    {
        let url = match conductor_domain.join("/v1/producer/check") {
            Ok(u) => u,
            Err(_) => todo!()
        };
        let params = [("uuid", uuid)];
        let client = reqwest::Client::new();
        match client.get(url).query(&params).send().await {
            Ok(response) => {
                Ok(response.status().is_success())
            }
            Err(_) => todo!()
        }
    }
}


pub trait Producer: Base {
    ///
    ///
    /// # Arguments
    ///
    /// * `uuid`:
    /// * `conductor_url`:
    /// # Errors
    /// returns: Result<(), &str>
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// ```
    fn emit(&self, uuid: &str, conductor_domain: Url) -> Result<(), Error>
    {
        let (payload, url) = self.generate_emit_data(uuid, conductor_domain)?;

        //start blocking specific
        let client = reqwest::blocking::Client::new();
        let request_resp = client.post(url)
            .body(payload)
            .header(reqwest::header::CONTENT_TYPE, reqwest::header::HeaderValue::from_static("application/msgpack"))
            .send();
        let response = match request_resp {
            Ok(r) => r,
            Err(err) => todo!()
        };
        let result: EmitResult = match rmp_serde::from_read_ref(response.bytes().unwrap().as_ref()) {
            Ok(r) => r,
            Err(err) => todo!()
        };
        //end blocking specific code
        match &result.error {
            ConductorError::NoError => Ok(()),
            _ => Err(Error::ConductorError(result.error))
        }
    }
    ///
    ///
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
    fn register(name: &str, uuid: Option<String>, conductor_domain: Url) -> Result<String, &'static str>
    {
        //TODO handle errors correctly
        let (payload, url) = match Self::prepare_registration_data(name, uuid, conductor_domain) {
            Ok(pu) => pu,
            Err(_) => todo!()
        };
        let client = reqwest::blocking::Client::new();
        let request = client.post(url)
            .body(payload)
            .header(reqwest::header::CONTENT_TYPE, reqwest::header::HeaderValue::from_static("application/msgpack"))
            .send();
        let response = match request {
            Ok(r) => r,
            Err(_) => todo!()
        };
        let result: RegistrationResult = match rmp_serde::from_read_ref(response.bytes().unwrap().as_ref()) {
            Ok(r) => r,
            Err(_) => todo!()
        };
        Ok(result.uuid.unwrap())
    }

    ///
    ///
    /// # Arguments
    ///
    /// * `uuid`:
    /// * `conductor_base_url`:
    ///
    /// returns: Result<bool, &str>
    /// # Errors
    /// # Examples
    ///
    /// ```
    ///
    /// ```
    fn is_registered(uuid: &str, conductor_domain: Url) -> Result<bool, &'static str>
    {
        let url = match conductor_domain.join("/v1/producer/check") {
            Ok(u) => u,
            Err(_) => todo!()
        };
        let params = [("uuid", uuid)];
        let client = reqwest::blocking::Client::new();
        match client.get(url).query(&params).send() {
            Ok(response) => {
                Ok(response.status().is_success())
            }
            Err(_) => todo!()
        }
    }
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