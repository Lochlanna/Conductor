use std::fmt;
use std::fmt::Formatter;
use serde::{Deserialize, Serialize};

/// Errors produced by the Conductor Instance
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub enum ConductorError {
    /// Indicates that there was no error. This exists to be more compatible with being sent over
    /// the wire to clients which may not have proper support for options.
    NoError,
    /// Indicates that a Producer schema contains a timestamp field which is not allowed as it's generated automatically by Conductor
    TimestampDefined(String),
    /// Indicates that an empty schema was sent
    NoMembers(String),
    /// Indicates that there was an issue with at least one of the columns in the schema using illegal characters or formatting
    InvalidColumnNames(String),
    /// Indicates the schema is too large (> 2_147_483_647)
    TooManyColumns(String),
    /// A generic Conductor error
    InternalError(String),
    /// The uuid provided was invalid. This could be an invalid custom id during registration or an ID which has not been registered during all other actions.
    InvalidUuid(String),
    /// The name provided is empty.
    NameInvalid(String),
    /// Attempted to emit data without having first registered the Producer.
    Unregistered(String),
    /// The data doesn't match the data type or cannot be converted to that data type
    InvalidData(String),
    /// The schema sent in an emit doesn't match the one which was registered.
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