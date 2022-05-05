pub mod reactor;
pub mod producer;
pub mod schema;
pub mod error;


use serde::{Deserialize, Serialize};
///The response from the Conductor instance after a registration attempt
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RegistrationResult {
    pub error: error::ConductorError,
    pub uuid: Option<String>,
}

impl RegistrationResult {
    pub fn new(error: error::ConductorError, uuid: Option<String>) -> Self {
        Self { error, uuid }
    }
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