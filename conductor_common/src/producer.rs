use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use url::{Url};
use duplicate::duplicate;

#[cfg(feature = "async")]
use async_trait::async_trait;
use std::fmt;
use std::fmt::Formatter;
use crate::schema;
use crate::error;


/// Contains the information required to register a producer with a Conductor server.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Registration {
    name: String,
    schema: schema::Schema,
    use_custom_id: Option<String>, // this is to support devices without persistent storage such as an arduino. They can have a custom id
}

impl Registration {
    #[must_use]
    pub const fn new(name: String, schema: schema::Schema, custom_id: Option<String>) -> Self {
        Self {
            name,
            schema,
            use_custom_id: custom_id,
        }
    }

    /// Create a new instance of Registration with an empty schema.
    #[must_use]
    pub fn new_empty(name: String, custom_id: Option<String>) -> Self {
        Self {
            name,
            schema: std::collections::HashMap::default(),
            use_custom_id: custom_id,
        }
    }

    /// Get the name of the producer
    #[must_use]
    pub fn get_name(&self) -> &str {
        &self.name
    }

    /// returns true if a uuid has been set.
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

    #[must_use]
    pub fn contains_column(&self, column_name: &str) -> bool {
        self.schema.contains_key(column_name)
    }

    #[must_use]
    pub fn schema_len(&self) -> usize {
        self.schema.len()
    }

    #[must_use]
    pub const fn get_schema(&self) -> &schema::Schema {
        &self.schema
    }
}

///The response from the Conductor instance after a registration attempt
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RegistrationResult {
    pub error: error::ConductorError,
    pub uuid: Option<String>,
}

/// A new data packet to be sent to the Conductor instance
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
    pub error: error::ConductorError,
}

/// All the errors that can be produced by a producer
#[derive(Debug)]
pub enum Error {
    /// The domain given for the conductor instance is invalid in some way
    InvalidConductorDomain(String),
    /// Indicates a failure to serialize a struct to message pack. Contains rmp_serde encoding error
    MsgPackSerialisationFailure(rmp_serde::encode::Error),
    /// Indicates a failure to serialize a struct to json. Contains serde_json error type
    JsonSerialisationFailure(serde_json::Error),
    /// Indicates a failure to serialize a struct. Contains the error given by the serializer.
    GenericSerialisationFailure(Box<dyn std::error::Error>),
    /// Indicates an error which was emitted from the Conductor server (Internal Server Error)
    ConductorError(error::ConductorError),
    /// Indicates an issue with the network layer. Contains the reqwest error type
    NetworkError(reqwest::Error),
    /// Indicates a failure to deserialize a struct from message pack. Contains rmp_serde decoding error
    MsgPackDeserializationFailure(rmp_serde::decode::Error),
    /// Indicates a failure to deserialize a struct from json. Contains serde_json error type
    JsonDeserializationFailure(serde_json::Error),
    /// Indicates a failure to deserialize a struct. Contains the error given by the serializer.
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

///
/// Provides functionality that is shared between both the async and blocking versions of the Producer trait.
/// Prepares and processes conductor requests and responses.
///
pub trait Base: Serialize + Clone + crate::schema::ConductorSchema {
    ///
    /// Prepares a payload for emitting data. This function doesn't send the payload.
    ///
    /// # Arguments
    ///
    /// * `uuid`: The unique ID of this producer.
    /// * `conductor_domain`: The url of the conductor instance.
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
    /// use conductor_common::producer::Base;
    /// use conductor_common::schema;
    /// #[derive(Clone, Serialize)]
    /// struct Measurement {
    ///     data_point:u8
    /// }
    /// impl schema::ConductorSchema for Measurement {
    ///     fn generate_schema() -> HashMap<String, schema::DataTypes> {
    ///         unimplemented!("Not needed for example/test");
    ///     }
    /// }
    /// impl Base for Measurement {}
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
    /// * `conductor_domain`: The url of the conductor instance.
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
    /// use conductor_common::producer::Base;
    /// use conductor_common::schema;
    /// #[derive(Clone, Serialize)]
    /// struct Measurement {
    ///     data_point:u8
    /// }
    /// impl schema::ConductorSchema for Measurement {
    ///     fn generate_schema() -> HashMap<String, schema::DataTypes> {
    ///         unimplemented!("Not needed for example/test");
    ///     }
    /// }
    /// impl Base for Measurement {}
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

///
/// Provides functions to add Conductor interactions to a type. Turns the implementing type into
/// a Conductor Producer. This version of the trait provides a Asynchronous version of the functions.
/// Refer to `conductor::producer::Producer` for the blocking version.
///
/// This should not be implemented directly in most cases.
/// Instead use the `#[derive(conductor::Producer)]` macro to generate everything for you.
///
#[cfg(feature = "async")]
#[async_trait]
#[allow(clippy::module_name_repetitions)]
pub trait AsyncProducer: Base {
    /// Async send a new data packet to the conductor server.
    /// Messagepack is used as the format over the wire.
    ///
    /// # Arguments
    ///
    /// * `uuid`: The unique id of this producer which was registered with conductor.
    /// * `conductor_domain`: The url of the conductor instance.
    ///
    /// # Errors
    /// * `InvalidConductorDomain`: Produced when the conductor domain is an invalid url.
    /// * `MsgPackSerialisationFailure`: Produced when the emit payload cannot be serialised to the message pack format. This is most likely
    /// due to a difficulty serialising Self using serde.
    /// * `NetworkError`: Produced when the http post fails for any reason. Holds the Reqwest Error Struct.
    /// * `MsgPackDeserializationFailure`: Produced when the emit response couldn't be deserialized from message pack. Holds the
    /// rmp_serde Error struct.
    /// * `ConductorError`: Produced when there was an error on the server.
    ///
    async fn emit(&self, uuid: &str, conductor_domain: Url) -> Result<(), Error>
    {
        let (payload, url) = self.generate_emit_data(uuid, conductor_domain)?;

        //start async specific
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
            Err(err) => return Err(Error::MsgPackDeserializationFailure(err))
        };
        //end async specific code
        if result.error == error::ConductorError::NoError {
            return Ok(());
        }
        Err(Error::ConductorError(result.error))
    }


    /// Generates the schema for this struct and register it with conductor asynchronously.
    ///
    /// # Arguments
    ///
    /// * `name`: A human friendly name for this producer. This isn't important to conductor and doesn't have to be unique.
    /// It's stored in the DB and can be useful to identify the producer. And empty string is valid but not recommended.
    /// * `uuid`: An optional unique ID which will be used to identify this producer. If this is set to None one is generated automatically by
    /// Conductor. It's recommended to leave this as null and let the server generate the ID.
    /// * `conductor_domain`: The url of the conductor instance.
    ///
    /// # Errors
    /// * `InvalidConductorDomain`: Produced when the conductor domain is an invalid url.
    /// * `MsgPackSerialisationFailure`: Produced when the emit payload cannot be serialised to the message pack format. This is most likely
    /// due to a difficulty serialising Self using serde.
    /// * `NetworkError`: Produced when the http post fails for any reason. Holds the Reqwest Error Struct.
    /// * `MsgPackDeserializationFailure`: Produced when the emit response couldn't be deserialized from message pack. Holds the
    /// rmp_serde Error struct.
    /// * `ConductorError`: Produced when there was an error on the server.
    ///
    async fn register(name: &str, uuid: Option<String>, conductor_domain: Url) -> Result<String, Error>
    {
        //TODO handle errors correctly
        let (payload, url) = Self::prepare_registration_data(name, uuid, conductor_domain)?;

        let client = reqwest::Client::new();
        let request = client.post(url)
            .body(payload)
            .header(reqwest::header::CONTENT_TYPE, reqwest::header::HeaderValue::from_static("application/msgpack"))
            .send().await;
        let response = match request {
            Ok(r) => r,
            Err(err) => return Err(Error::NetworkError(err))
        };
        let result: RegistrationResult = match rmp_serde::from_read_ref(response.bytes().await.unwrap().as_ref()) {
            Ok(r) => r,
            Err(err) => return Err(Error::MsgPackDeserializationFailure(err))
        };
        if result.error != error::ConductorError::NoError {
            return Err(Error::ConductorError(result.error));
        }
        Ok(result.uuid.unwrap())
    }

    ///
    /// Asynchronously checks to see if the UUID has been registered with Conductor.
    /// This does not verify that the schema registered with the server is correct.
    ///
    /// # Arguments
    ///
    /// * `uuid`: The unique id of this producer which was registered with conductor.
    /// * `conductor_domain`: The url of the conductor instance.
    ///
    /// # Errors
    /// * `InvalidConductorDomain`: Produced when the conductor domain is an invalid url.
    /// * `NetworkError`: Produced when the http get fails for any reason. Holds the Reqwest Error Struct.
    ///
    async fn is_registered(uuid: &str, conductor_domain: Url) -> Result<bool, Error>
    {
        let url = match conductor_domain.join("/v1/producer/check") {
            Ok(u) => u,
            Err(err) => return Err(Error::InvalidConductorDomain(format!("The conductor domain was invalid. {}", err)))
        };
        let params = [("uuid", uuid)];
        let client = reqwest::Client::new();
        match client.get(url).query(&params).send().await {
            Ok(response) => {
                Ok(response.status().is_success())
            }
            Err(err) => Err(Error::NetworkError(err))
        }
    }
}

///
/// Provides functions to add Conductor interactions to a type. Turns the implementing type into
/// a Conductor Producer. This version of the trait provides a blocking version of the functions.
/// Refer to `conductor::producer::AsyncProducer` for the Asynchronous version.
///
/// This should not be implemented directly in most cases.
/// Instead use the `#[derive(conductor::Producer)]` macro to generate everything for you.
///
pub trait Producer: Base {
    /// Send a new data packet to the conductor server.
    /// Messagepack is used as the format over the wire.
    /// This function blocks.
    ///
    /// # Arguments
    ///
    /// * `uuid`: The unique id of this producer which was registered with conductor.
    /// * `conductor_domain`: The url of the conductor instance.
    ///
    /// # Errors
    /// * `InvalidConductorDomain`: Produced when the conductor domain is an invalid url.
    /// * `MsgPackSerialisationFailure`: Produced when the emit payload cannot be serialised to the message pack format. This is most likely
    /// due to a difficulty serialising Self using serde.
    /// * `NetworkError`: Produced when the http post fails for any reason. Holds the Reqwest Error Struct.
    /// * `MsgPackDeserializationFailure`: Produced when the emit response couldn't be deserialized from message pack. Holds the
    /// rmp_serde Error struct.
    /// * `ConductorError`: Produced when there was an error on the server.
    ///
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
            Err(err) => return Err(Error::NetworkError(err))
        };
        let result: EmitResult = match rmp_serde::from_read_ref(response.bytes().unwrap().as_ref()) {
            Ok(r) => r,
            Err(err) => return Err(Error::MsgPackDeserializationFailure(err))
        };
        //end blocking specific code
        match &result.error {
            error::ConductorError::NoError => Ok(()),
            _ => Err(Error::ConductorError(result.error))
        }
    }

    /// Generates the schema for this struct and register it with conductor.
    /// This function blocks.
    ///
    /// # Arguments
    ///
    /// * `name`: A human friendly name for this producer. This isn't important to conductor and doesn't have to be unique.
    /// It's stored in the DB and can be useful to identify the producer. And empty string is valid but not recommended.
    /// * `uuid`: An optional unique ID which will be used to identify this producer. If this is set to None one is generated automatically by
    /// Conductor. It's recommended to leave this as null and let the server generate the ID.
    /// * `conductor_domain`: The url of the conductor instance.
    ///
    /// # Errors
    /// * `InvalidConductorDomain`: Produced when the conductor domain is an invalid url.
    /// * `MsgPackSerialisationFailure`: Produced when the emit payload cannot be serialised to the message pack format. This is most likely
    /// due to a difficulty serialising Self using serde.
    /// * `NetworkError`: Produced when the http post fails for any reason. Holds the Reqwest Error Struct.
    /// * `MsgPackDeserializationFailure`: Produced when the emit response couldn't be deserialized from message pack. Holds the
    /// rmp_serde Error struct.
    /// * `ConductorError`: Produced when there was an error on the server.
    ///
    fn register(name: &str, uuid: Option<String>, conductor_domain: Url) -> Result<String, Error>
    {
        //TODO handle errors correctly
        let (payload, url) = Self::prepare_registration_data(name, uuid, conductor_domain)?;

        let client = reqwest::blocking::Client::new();
        let request = client.post(url)
            .body(payload)
            .header(reqwest::header::CONTENT_TYPE, reqwest::header::HeaderValue::from_static("application/msgpack"))
            .send();
        let response = match request {
            Ok(r) => r,
            Err(err) => return Err(Error::NetworkError(err))
        };
        let result: RegistrationResult = match rmp_serde::from_read_ref(response.bytes().unwrap().as_ref()) {
            Ok(r) => r,
            Err(err) => return Err(Error::MsgPackDeserializationFailure(err))
        };
        if result.error != error::ConductorError::NoError {
            return Err(Error::ConductorError(result.error));
        }
        Ok(result.uuid.unwrap())
    }

    ///
    /// Checks to see if the UUID has been registered with Conductor.
    /// This does not verify that the schema registered with the server is correct.
    /// This function blocks
    ///
    /// # Arguments
    ///
    /// * `uuid`: The unique id of this producer which was registered with conductor.
    /// * `conductor_domain`: The url of the conductor instance.
    ///
    /// # Errors
    /// * `InvalidConductorDomain`: Produced when the conductor domain is an invalid url.
    /// * `NetworkError`: Produced when the http get fails for any reason. Holds the Reqwest Error Struct.
    ///
    fn is_registered(uuid: &str, conductor_domain: Url) -> Result<bool, Error>
    {
        let url = match conductor_domain.join("/v1/producer/check") {
            Ok(u) => u,
            Err(err) => return Err(Error::InvalidConductorDomain(format!("The conductor domain was invalid. {}", err)))
        };
        let params = [("uuid", uuid)];
        let client = reqwest::blocking::Client::new();
        match client.get(url).query(&params).send() {
            Ok(response) => {
                Ok(response.status().is_success())
            }
            Err(err) => Err(Error::NetworkError(err))
        }
    }
}

